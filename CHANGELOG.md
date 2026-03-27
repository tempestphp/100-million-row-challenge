# Changelog / Optimisation Journey

## Where it started

Kicked things off with a dead simple `fgets` loop - reading line by line, using fixed offsets to extract the path and date (as [@brendt](https://github.com/brendt) covered in his livestream - the timestamp suffix is always 26 chars, the URL prefix is always 19). Chucked everything into an associative array. Managed about **0.37s on 1M rows**, which felt alright until you scale that up to 100M and realise you're looking at ~31s. Not ideal.

## The big wins

### Parallel processing with `pcntl_fork` (~ -25%)

Split the file into chunks and forked child processes to parse them in parallel. Started with 2 workers and worked up to 6. Each worker gets a clean chunk boundary (seek to offset, skip to the next newline) so no lines get mangled.

### Integer-indexed arrays instead of string keys (massive)

Rather than using the full path and date strings as array keys in the hot loop, I map each unique path and date to an integer ID on first encounter. The counts matrix becomes `$counts[$pathId][$dateId]++` which is significantly faster than hashing long strings 100M times.

**Hat tip to [@xHeaven](https://github.com/xHeaven) (PR #3)** - had a proper look at their approach and this technique was the inspiration. Cheers for that.

### Custom binary packing over Unix socket pairs (~ -15% vs serialize)

Swapped out `serialize` / temp files for IPC and went with:
- `stream_socket_pair` for communication between parent and children (no disk I/O)
- `pack` / `unpack` for a custom binary format (path strings + 10-byte dates + uint32 counts matrix)

Benchmarked at roughly 3.5x faster serialisation and 4x faster deserialisation compared to `serialize`, with about a third of the data size. Probably overkill for ~2MB per worker, but every little helps.

### Global date sort instead of per-path `ksort` (~ -35% for sort phase)

The original approach called `ksort` on every path's date array individually (~281 calls). Replaced this with a single `sort()` on the full set of unique dates, then rebuilt each path's data in sorted order. About 35% faster for that phase - not huge in absolute terms since sorting is <1% of total runtime, but it's cleaner code too.

## The smaller wins

### Separate leftover handling (~ -6%)

When reading 8MB chunks, partial lines at chunk boundaries were being handled by concatenating the leftover onto the next full chunk (`$leftover . $chunk`). That's copying ~8MB every iteration for no good reason. Refactored to handle the partial line separately - just concat the small leftover with the start of the next chunk to complete the line, then process the rest of the chunk as normal.

### Unbuffered file reads (~ -1.5%)

Added `stream_set_read_buffer($handle, 0)` to bypass PHP's internal stream buffer. Since i'm already doing 8MB `fread` calls, the extra buffering layer is just overhead. Small but measurable.

## What I tried that didn't work

- **Sparse counts** (skip zero-filling the counts matrix): 22% slower. The `isset` check on every line costs more than the occasional zero-fill when a new date appears.
- **Manual JSON building**: `json_encode` is implemented in C and is about 20% faster than string concatenation.
- **Larger/smaller read buffers** (4MB to 32MB): Basically no difference. 8MB is the sweet spot.
- **`explode`/`strtok` instead of `strpos`**: Both slower for line splitting.
- **`strspn` instead of `substr`**: `substr` is C-optimised and wins.

## Acknowledgements

- **[@xHeaven](https://github.com/xHeaven)** - The leading solution (PR #3) was proper inspiration for the integer-indexed arrays and newline-relative offset parsing approach. I've built on top of those ideas with my own IPC mechanism and additional micro-optimisations, but credit where it's due.
