# 100 Million Row Challenge — Summary

## Environment
- Benchmark server: **Mac Mini M1, 12GB RAM**
- PHP 8.5, JIT disabled, no FFI
- `Zend OPcache` extension is listed as available, but OPcache is typically disabled for CLI by default — unconfirmed whether it's active on the benchmark server
- Our best on leaderboard: **26.5s** (old 2-worker approach)
- Top of leaderboard: **~3.4s**

## Current Implementation

### Architecture
- **16 parallel workers** via `pcntl_fork()`
- Each worker reads its chunk with `file_get_contents()` (1 syscall)
- Workers write results as packed binary (`pack('V*')`) to `/tmp`
- Parent merges with a `for` loop on 1-indexed `unpack('V*')` arrays
- Manual JSON building (no `json_encode`) with pre-escaped paths

### Key Design Decisions

**Path discovery:** scan first 4MB of file to build `$pathBases` map  
(`path => pathId * DATE_COUNT` — pre-multiplied to avoid hot-loop multiplication)

**Hot loop (strpos walk):**
```php
while (($nl = strpos($chunk, "\n", $pos)) !== false) {
    $base = $pathBases[substr($chunk, $pos + 19, $nl - $pos - 45)] ?? -1;
    if ($base >= 0) {
        ++$counts[$base + $dateIds[substr($chunk, $nl - 23, 8)]];
    }
    $pos = $nl + 1;
}
```
- `strpos` walk avoids `strtok`'s per-line string copy (~1.49x faster)
- All offsets derived from `$nl` directly — no `$lineLen` variable
- `$pathBases` lookup returns pre-multiplied base, removing one multiply per hit
- `-1` sentinel avoids `null` type check overhead

**IPC (binary flat array):**
- `pack('V*', ...$counts)` — 2.74MB per worker vs ~36MB igbinary
- `unpack('V*')` produces 1-indexed array; merge with `for ($i=1; $i<=$N; $i++)`
- No sorting needed — counts array is pre-indexed by date order

**JSON output (manual building):**
- Pre-escape paths once: `str_replace('/', '\/', $path)` to match `json_encode` behaviour
- Pre-build date prefix strings once: `'        "YYYY-MM-DD": '`
- Accumulate into one string buffer, single `file_put_contents` call
- ~1.34x faster than `json_encode` + intermediate PHP array

### Input Format
```
https://stitcher.io/PATH,YYYY-MM-DDTHH:MM:SS+00:00
```
- Fixed prefix: 19 chars (`https://stitcher.io`)
- Fixed suffix: 26 chars (`,` + 25-char ISO timestamp)
- Path length = `lineLen - 45`
- Date `YY-MM-DD` at `nl - 23` (8 chars), skipping `,20` prefix of year

### Date Encoding
- Pre-built `$dateIds["YY-MM-DD"]` hash table (2557 entries, 2020–2026)
- Date range is fixed; 2557 = exact day count for those 7 years

## Performance Journey (all on benchmark server)

| Approach | Time | Notes |
|----------|------|-------|
| 2 workers + `stream_get_line` + igbinary | **26.5s** | old committed code |
| **16 workers + strpos walk + binary IPC + manual JSON** | **TBD** | current |

## What We Benchmarked and Rejected

| Idea | Result |
|------|--------|
| `strtok` vs `strpos` walk | strpos **1.49x faster** — no per-line string copy |
| `ymFlat` arithmetic date decode (ord() math) | **0.74x slower** — hash beats arithmetic |
| `unpack('P')` 8-byte int date key | **0.32x slower** — unpack overhead |
| Binary search over sorted paths | **0.34x slower** — PHP function call overhead |
| `preg_match_all` on full chunk | OOM — too much memory |
| `shmop` shared memory IPC | macOS shmmax=4MB limit, not viable |
| `array_map` for merge | **0.30x slower** — closure overhead |
| `??-1` sentinel vs `??null` | `-1` + `>= 0` is marginally faster |
| Shorter path key (strip `/blog/`) | negligible — hash cost is not key-length-sensitive here |
| Pre-multiplied `pathBases` (avoid `* DATE_COUNT`) | **~1.13x** — one less multiply per matched line |
| `$nl`-based offsets (no `$lineLen` var) | **~1.14x** — one less subtraction per line |

## Files
- `app/Parser.php` — main implementation
- `app/Commands/DataParseCommand.php` — CLI entry point (`php tempest data:parse`)
- `bench_merge.php` — merge + JSON output strategy benchmarks
- `bench_hotloop.php` — hot loop variant benchmarks