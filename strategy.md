# 100M Row Challenge — Strategy & Findings

## How to run

```sh
# Build the Docker image (matches benchmark server: 2 vCPUs, 1.5GB RAM)
docker compose build

# Validate correctness against test data
docker compose run --rm php php tempest data:validate

# Benchmark on current dataset (default 1M rows)
docker compose run --rm php php tempest data:parse

# Run bench.php (avoids Tempest overhead, multiple runs with min/avg/max)
docker compose run --rm php php bench.php /app/data/data.csv 5

# Generate larger datasets
echo "0" | docker compose run --rm php php tempest data:generate 10000000    # 10M rows
echo "0" | docker compose run --rm php php tempest data:generate 100000000   # 100M rows

# Profile time breakdown (read/parse/sort/json/write)
docker compose run --rm php php profile.php /app/data/data.csv
```

## Docker constraints (matching benchmark server)

- `cpus: 2` — 2 vCPUs
- `mem_limit: 1536m` — 1.5GB RAM
- `shm_size: 768m` — half of RAM for /dev/shm
- `memory_limit=1536M` — PHP memory limit matches available RAM

## Key data characteristics

- All URLs share prefix `https://stitcher.io` (19 chars)
- 280 possible URL paths (from Visit.php)
- Timestamp always `YYYY-MM-DDTHH:MM:SS+00:00` (25 chars)
- Comma position is fixed: 26 chars before `\n`, or 27 chars from end of line (including `\n`)
- Date is first 10 chars after the comma
- Flat key: `substr($chunk, $offset + 19, $lineEnd - $offset - 34)` extracts "/path,YYYY-MM-DD"

## Profile breakdown (10M rows, single-threaded)

| Phase              | Time   | % of total |
|--------------------|--------|------------|
| Read+Parse+Agg     | 4.38s  | 97%        |
| Sort               | 0.11s  | 2%         |
| JSON encode        | 0.02s  | <1%        |
| File write         | 0.01s  | <1%        |

Only the inner read+parse+aggregate loop matters.

## Benchmark results (10M rows, 2 vCPU constraint)

| Approach                                  | Median (s) | Notes                        |
|-------------------------------------------|------------|------------------------------|
| fgets + strrpos (naive baseline)          | 5.0        | Phase 1 starting point       |
| fgets + fixed comma offset                | 4.75       | Avoids strrpos               |
| fread 1MB chunks + strpos loop            | 4.4        | Best single-threaded         |
| 2 workers (fork) + fread + explode        | 3.24       |                              |
| 2 workers (fork) + fread + strpos loop    | 3.08       | Phase 2 best                 |
| 2 workers (fork) + fgets + stream buffer  | 3.03       | Close, simpler code          |
| 2 workers (fork) + fgets + byte tracking  | 3.20       | strlen per line hurts        |
| 3 workers (fork) + fread + explode        | 3.29       | Extra worker hurts on 2 vCPU |
| 4 workers (fork) + fread + explode        | 3.55       | Context switching overhead   |
| Parent-as-worker + 1 child                | 3.55       | Bench script overhead        |
| 2 workers + flat key aggregation          | 3.37       | String concat per line hurts |
| 2 workers + preg_match_all                | 4.29       | Regex overhead too high      |
| + /dev/shm IPC                            | 3.21       | Small IPC improvement        |
| + 16MB chunks                             | 2.87       | Less fread overhead          |
| + igbinary serialize                      | 2.67       | Faster IPC                   |
| + flat key (1 substr per line)            | 2.35       | Half the hash lookups        |
| + 32MB chunks                             | 2.28       |                              |
| + array_count_values (C-level counting)   | 2.02       | Big win: PHP→C counting      |
| + 256MB chunks + unset()                  | 1.70       | **Current best**             |

### Approaches tested but slower
| Approach                                  | Median (s) | Notes                        |
|-------------------------------------------|------------|------------------------------|
| preg_match_all + array_count_values       | 4.35-4.91  | Regex engine overhead        |
| explode("\n") + foreach                   | 2.77       | Array alloc for all lines    |
| fgets + flat key                          | 2.62       | Per-call overhead            |
| stream_set_read_buffer(0)                 | 2.57       | Higher variance, worse       |
| Flat IPC (no nested split in workers)     | 2.00       | igbinary can't dedup keys    |
| gc_disable()                              | worse      | unset() can't free memory    |
| 384MB chunks                              | 1.87       | $keys array too large        |

## Current best approach

2 workers via `pcntl_fork`, each processing half the file with `fread` in 256MB chunks.
Inner loop: `strpos` to find newlines, single `substr` per line to extract flat "path,date" key,
collect all keys in `$keys[]` array, then `array_count_values()` for C-level counting.
After all chunks, split flat keys into nested `path => date => count`.
IPC via `igbinary_serialize` (with fallback to `serialize`) to temp files in `/dev/shm`.

### Key insight: array_count_values
The biggest single optimization was replacing PHP-level `isset`/`++` per line with:
1. Build `$keys[]` array (just `$keys[] = substr(...)`)
2. `array_count_values($keys)` — C-level dedup + counting
3. Merge ~102K unique counts into `$data` (vs 5M lines)

This moves the counting from 5M PHP hash operations to a single C function call.

## 100M row scaling

| Rows | Median (s)| Per-row (ns)|
|------|-----------|-------------|
| 10M  | 1.70      | 170         |
| 100M | 15.6      | 156         |

Slightly sub-linear scaling — larger dataset amortizes setup costs.
