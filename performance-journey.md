# 🚀 100 Million Row Challenge - Performance Journey

## Starting Point: Naive Implementation
- **Time:** ~50s+
- Single-threaded, using `parse_url()`, inefficient string handling

---

## Phase 1: Basic Optimizations

### String Parsing Optimizations
- Replaced `parse_url()` with `strpos()`/`substr()`
- Hardcoded known positions where possible
- **Result:** 46.68s (7% faster)

### Parallel Processing with `pcntl_fork()`
| Workers | Time | Improvement |
|---------|------|-------------|
| 2 | 42.20s | 16% faster |
| 4 | 24.18s | 52% faster |
| 8 | 16.32s | 67% faster |
| 16 | 10.92s | 78% faster |

### IPC Optimization with igbinary
| Metric | Text-based | igbinary | Improvement |
|--------|-----------|----------|-------------|
| IPC Size | ~330 MB | **36.5 MB** | **89% smaller** |
| Write time | ~40ms | **~20ms** | **50% faster** |
| Merge time | 1.37s | **0.53s** | **61% faster** |
| **TOTAL** | 10.92s | **9.73s** | **11% faster** |

---

## Phase 2: Hot Loop Optimizations

After profiling, we found the **parse phase was 92% of total time** (~9s out of 9.73s). We targeted micro-optimizations in the hot loop that processes 100M lines.

### Optimization #1: Eliminate `ftell()` calls
**Problem:** Calling `ftell($handle) < $end` on every iteration = 6.25M syscalls per worker!

**Solution:** Track bytes read manually:
```php
$bytesRead = 0;
$bytesToRead = $end - $start;
while ($bytesRead < $bytesToRead && ($line = fgets($handle)) !== false) {
    $bytesRead += strlen($line);
}
```

### Optimization #2: Calculate comma position instead of `strpos()`
**Problem:** `strpos($line, ',', 19)` searches through the string = 100M function calls

**Solution:** The line format is fixed! Date is always 25 chars + comma = 26 from end:
```php
$commaPos = strlen($line) - 26;
```

### Optimization #3: Use `++` increment with `isset` checks
**Solution:** Separate paths for new vs existing entries with pre-increment:
```php
if (!isset($result[$path])) {
    $result[$path] = [$date => 1];
} elseif (!isset($result[$path][$date])) {
    $result[$path][$date] = 1;
} else {
    ++$result[$path][$date];
}
```

### Results After Hot Loop Optimizations
| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Parse time (per worker) | ~9.0s | **~7.4s** | **18% faster** |
| **TOTAL** | 9.73s | **8.03s** | **17% faster** |

---

## Phase 3: C-Native Functions 🔥

The bottleneck was still I/O: **6.25M `fgets()` syscalls per worker**. We explored PHP's C-native functions.

### The Winner: `file_get_contents()` + `strtok()`

```php
// Read entire chunk - 1 syscall instead of 6.25M fgets() calls
$chunk = file_get_contents($inputPath, false, null, $start, $end - $start);

// strtok() tokenizes in place without creating an array
$line = strtok($chunk, "\n");
while ($line !== false) {
    // process line...
    $line = strtok("\n");
}
```

**Why this works:**
- `file_get_contents()` - 1 syscall to read ~469MB chunk vs 6.25M `fgets()` calls
- `strtok()` - C function that tokenizes in place, no array allocation

### Results After C-Native Optimization (Unconstrained Environment)
| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Parse time (per worker) | ~7.4s | **~4.8s** | **35% faster** |
| **TOTAL** | 8.03s | **5.44s** | **32% faster** |

---

## Phase 4: Constrained Environment Optimization 🐳

The benchmark server has only **2 vCPUs and 1.5GB RAM**. Our 16-worker bulk-loading approach failed spectacularly!

### The Problem
- 16 workers × 469MB chunks = **7.5GB memory** needed
- With 1.5GB RAM, workers get OOM-killed
- Batching workers (2 at a time) → 32s due to serialization overhead

### The Solution: Hybrid Sub-Chunking

**2 workers** (1 per CPU), each processing half the file in **300MB sub-chunks**:

```php
// Each worker processes 3.5GB, but only loads 300MB at a time
while ($currentPos < $end) {
    $subChunkEnd = min($currentPos + $subChunkSize, $end);
    
    // Read sub-chunk into memory
    $chunk = fread($handle, $subChunkEnd - $currentPos);
    
    // Process with strtok (fast C-native tokenization)
    $line = strtok($chunk, "\n");
    while ($line !== false) {
        // process...
        $line = strtok("\n");
    }
    
    // Free memory before next sub-chunk
    unset($chunk);
    $currentPos = $subChunkEnd;
}
```

### Additional Optimizations
- **`/dev/shm` for IPC** - RAM-based tmpfs instead of disk (Linux)
- **Auto-detection** - Detects constrained environment (256MB < memory < 2GB)
- **True parallelism** - 2 workers run simultaneously (no batching)

### Results in Constrained Environment (2 vCPU, 1.5GB RAM)

| Approach | Time | Notes |
|----------|------|-------|
| 16 workers batched (2 concurrent) | 32.5s | OOM issues, serialized batches |
| **2 workers + hybrid sub-chunking** | **~26s** | Stable, no OOM |

**Improvement: 20% faster in constrained environment!**

---

## 🏆 Final Summary: 100M Rows Journey

### Unconstrained Environment (Mac M4, 16+ GB RAM)
| Approach | Time | vs Baseline |
|----------|------|-------------|
| Naive single-threaded | ~50s+ | baseline |
| Optimized single-threaded | 46.68s | 7% faster |
| Parallel 16 workers (text) | 10.92s | 78% faster |
| Parallel 16 workers (igbinary) | 9.73s | 80% faster |
| + Hot loop optimizations | 8.03s | 84% faster |
| **+ C-native strtok()** | **5.44s** | **89% faster** |

### Constrained Environment (2 vCPU, 1.5GB RAM)
| Approach | Time |
|----------|------|
| 16 workers batched | 32.5s |
| **2 workers + hybrid** | **~26s** |

---

## Architecture Summary

```
┌─────────────────────────────────────────────────────────────┐
│                    ENVIRONMENT DETECTION                     │
├─────────────────────────────────────────────────────────────┤
│  Memory > 2GB?                                              │
│    YES → 16 workers, bulk loading (file_get_contents)       │
│    NO  → 2 workers, hybrid sub-chunking (300MB chunks)      │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                   UNCONSTRAINED PATH                         │
├─────────────────────────────────────────────────────────────┤
│  Worker 0──┬──file_get_contents(469MB)──strtok()──results   │
│  Worker 1──┤                                                 │
│  ...       ├──(all 16 run in parallel)                      │
│  Worker 15─┘                                                 │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                    CONSTRAINED PATH                          │
├─────────────────────────────────────────────────────────────┤
│  Worker 0: 3.5GB file half                                  │
│    ├─ Load 300MB sub-chunk → strtok() → free                │
│    ├─ Load 300MB sub-chunk → strtok() → free                │
│    └─ ... (12 iterations)                                   │
│                                                             │
│  Worker 1: 3.5GB file half (parallel)                       │
│    └─ Same sub-chunking approach                            │
│                                                             │
│  IPC: /dev/shm (RAM-based tmpfs)                            │
└─────────────────────────────────────────────────────────────┘
```

---

## What Made the Difference

1. **String parsing optimizations** - `strpos`/`substr` instead of `parse_url()`
2. **Parallel processing** - `pcntl_fork()` for multi-core utilization
3. **Smart IPC** - `igbinary` for compact, fast serialization
4. **C-native functions** - `file_get_contents()` + `strtok()`
5. **Hybrid sub-chunking** - Memory-efficient processing for constrained environments
6. **`/dev/shm`** - RAM-based IPC in Docker/Linux
7. **Environment auto-detection** - Optimal strategy per environment

## Failed Experiments
- **`preg_match_all()`** - Creates massive matches array (1.25GB for 6.25M matches)
- **`explode()`** - Creates array with 6.25M elements, huge memory overhead
- **Flat key `"$path|$date"`** - More memory than nested arrays
- **Large sub-chunks (400-500MB)** - Caused memory pressure, slower than 300MB

## Docker Setup

```dockerfile
FROM php:8.5-rc-cli
# Extensions: igbinary, pcntl, intl
# Memory limit: 1500M
```

```yaml
# docker-compose.yml
services:
  parser:
    cpus: 2
    mem_limit: 1.5g
    tmpfs:
      - /dev/shm:size=512m
```
