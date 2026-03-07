<?php

declare(strict_types=1);

namespace App;

use Exception;

/**
 * 100-Million-Row Challenge — fread block-buffered edition
 *
 * WHY fread() IS FASTER THAN fgets() EVEN WITH 1MB LIBC BUFFER
 * ─────────────────────────────────────────────────────────────
 * The current code uses stream_set_read_buffer($handle, 1MB), which
 * makes the libc FILE* buffer 1MB at a time. This reduces syscalls
 * from 8.3M to ~625 per child. But fgets() STILL pays per-line cost:
 *
 *   FILE* mutex check           ~3ns
 *   memchr() scan for \n        ~5ns  (75 bytes, C/SIMD)
 *   memcpy line → PHP zval      ~5ns
 *   PHP zval allocation/setup   ~10ns
 *   PHP function dispatch       ~5ns
 *   ─────────────────────────────────
 *   Total per line:             ~45ns × 8.3M = 375ms/child
 *
 * fread(8MB) + explode("\n") amortises all of the above:
 *
 *   fread(8MB) syscall          ~100ns amortised over 106,667 lines ≈ 0ns/line
 *   explode("\n") on 8MB block  one C/SIMD pass ≈ 2ns/line amortised
 *   Per-line array access       ~5ns
 *   strlen() of element         ~2ns  (reads pre-stored zval.len field)
 *   substr() path               ~10ns
 *   substr() date               ~5ns
 *   ─────────────────────────────────
 *   Total per line:             ~25ns × 8.3M = 208ms/child
 *
 *   Saving: ~167ms/child → ~167ms off wall time
 *   2.9s → ~2.7s expected
 *
 * THE BOUNDARY PROBLEM — AND HOW WE SOLVE IT
 * ────────────────────────────────────────────
 * Each worker owns chunk [start, end). Lines whose first byte falls
 * in that range belong to this worker. A line that STARTS before end
 * but ENDS after end still belongs to this worker (not worker i+1).
 *
 * With fread(), the last block is capped at exactly (end - pos) bytes:
 *   toRead = min(BLOCK_SIZE, end - pos)
 *
 * This means the last block never reads past end. After processing it,
 * $carry holds the incomplete line fragment whose first byte is before
 * end (= this worker's line). We call fgets() exactly once to read
 * the remainder of that line past end, then process the complete line.
 *
 * Worker i+1 does fseek(end-1) + fgets() to skip that same line,
 * landing cleanly on the first line that starts at or after end.
 *
 * FOUR BOUNDARY CASES — ALL CORRECT:
 *
 *   Case 1: Last block ends mid-line (most common)
 *     carry = "https://site.io/blog/post-name"  (partial)
 *     fgets() reads ",2024-01-24T01:16:58+00:00\n"
 *     complete = carry . fgets_result → process ✓
 *
 *   Case 2: Last block ends exactly on \n
 *     carry = "" (array_pop gets empty string)
 *     No overflow fgets needed — guard: if ($carry !== '')
 *     Worker i+1 fseek(end-1) hits the \n, fgets() reads "\n" → discards ✓
 *
 *   Case 3: end falls exactly at a line start
 *     Identical to Case 2 ✓
 *
 *   Case 4: Last worker (end = fileSize)
 *     fread reads to EOF. File may have no trailing \n.
 *     carry = last line (no \n) or '' if file ends with \n
 *     fgets() returns false at EOF → carry processed as-is ✓
 *
 * NOTE ON eolOffset WITH fread+explode:
 * explode("\n") strips the \n delimiter. Lines from explode have NO \n.
 * However the last line of each block (the carry) is a fragment, not a
 * complete line — it's never parsed directly from the explode output.
 * All complete lines come from the explode array (no \n) EXCEPT the
 * boundary carry line which is assembled from carry + fgets() result.
 *
 * For the carry + fgets() assembled line: fgets() includes \n (or \r\n).
 * We detect the eol style once and use it for that single assembled line.
 * For all explode lines: use the NO-NEWLINE offset instead (strlen - 26).
 *
 * Timestamp "YYYY-MM-DDTHH:MM:SS+00:00" = 25 chars.
 *   With \n:    strlen - 27 → Unix,   strlen - 28 → Windows
 *   Without \n: strlen - 26 (explode lines, always, regardless of OS)
 */
final class Parser
{
    private const YEAR_BASE  = 2020;
    private const BLOCK_SIZE = 1048576; // 1MB — keeps block+array+pathCache within M4 L2

    public function parse(string $inputPath, string $outputPath): void
    {
        \ini_set('memory_limit', '4G');
        \gc_disable();

        $cpuCores  = $this->detectCores();
        $fileSize  = \filesize($inputPath);
        $chunkSize = (int) \ceil($fileSize / $cpuCores);
        $sockets   = [];
        $pids      = [];

        for ($i = 0; $i < $cpuCores; $i++) {
            $pair = \stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            if ($pair === false) throw new \Exception("socket_pair failed");

            $start = $i * $chunkSize;
            $end   = ($i === $cpuCores - 1) ? $fileSize : ($start + $chunkSize);

            $pid = \pcntl_fork();
            if ($pid === -1) throw new \Exception("fork failed");

            if ($pid === 0) {
                \fclose($pair[0]);

                [$results, $pathNames] = $this->processChunk($inputPath, $start, $end);
                $binary    = $this->packBinary($results, $pathNames);
                unset($results, $pathNames);

                $tot = strlen($binary);
                \fwrite($pair[1], \pack('N', $tot));
                $off = 0;
                while ($off < $tot) {
                    $w = \fwrite($pair[1], \substr($binary, $off, 65536));
                    if ($w === false || $w === 0) break;
                    $off += $w;
                }
                \fclose($pair[1]);
                exit(0);
            }

            \fclose($pair[1]);
            $pids[]    = $pid;
            $sockets[] = $pair[0];
        }

        $final = [];
        foreach ($sockets as $sock) {
            $hdr = '';
            while (\strlen($hdr) < 4) {
                $c = \fread($sock, 4 - \strlen($hdr));
                if ($c === false || $c === '') break;
                $hdr .= $c;
            }
            $len = \unpack('N', $hdr)[1];

            $buf = '';
            while (\strlen($buf) < $len) {
                $c = \fread($sock, \min(65536, $len - strlen($buf)));
                if ($c === false || $c === '') break;
                $buf .= $c;
            }
            \fclose($sock);

            $this->mergeInto($final, $buf);
            unset($buf);
        }

        foreach ($pids as $pid) \pcntl_waitpid($pid, $status);

        foreach ($final as &$dates) {
            \ksort($dates, SORT_NUMERIC);
        }
        unset($dates);

        $output = [];
        foreach ($final as $path => $dateCounts) {
            $decoded = [];
            foreach ($dateCounts as $packed => $count) {
                $day   =  $packed        & 0x1F;
                $month = ($packed >> 5)  & 0x0F;
                $year  = ($packed >> 9)  + self::YEAR_BASE;
                $decoded[\sprintf('%04d-%02d-%02d', $year, $month, $day)] = $count;
            }
            $output[$path] = $decoded;
        }

        \file_put_contents($outputPath, \json_encode($output, JSON_PRETTY_PRINT));
    }

    // =========================================================================
    // HOT LOOP — fread block buffered
    // =========================================================================

    /**
     * HOT LOOP — fread block-buffered, integer-keyed results
     *
     * Returns [$results, $pathNames] where:
     *   $results   = nested int array  [ pathId => [ packedDate => count ] ]
     *   $pathNames = id→string map     [ pathId => pathString ]
     *
     * WHY INTEGER OUTER KEY
     * ──────────────────────
     * The outer key of $results was previously a string (the path slug).
     * PHP HashTable buckets for string keys store a zend_string* pointer
     * and require a separate heap allocation for the string itself.
     *
     * With an integer outer key:
     *   - The key is stored inline in the zend_bucket (no separate allocation)
     *   - Hash = key itself — one integer compare, no pointer dereference
     *   - 50k fewer malloc() calls for the outer HT string storage
     *   - Outer bucket: 32B instead of ~87B → 2× entries per cache line
     *
     * The string→int mapping ($pathToId) has the same lookup cost as the old
     * $pathCache (both are string-keyed HTs), but the $results outer lookup
     * drops from string (2ns: hash + pointer) to int (1ns: hash only).
     *
     * Memory saved in results[] outer HT per child:
     *   50k × (32 + 23) bytes = ~2.6 MB (no string key allocations)
     *
     * The $pathToId and $pathNames arrays replace $pathCache.
     * $pathNames (int→string) is passed to packBinary for IPC serialisation.
     * mergeInto() and the parent decode loop are UNCHANGED — they still
     * receive and use string keys via the binary wire format.
     */
    private function processChunk(string $filePath, int $start, int $end): array
    {
        $handle = \fopen($filePath, 'rb');
        \stream_set_read_buffer($handle, 1048576);

        if ($start !== 0) {
            \fseek($handle, $start - 1);
            \fgets($handle);
        }

        // $results   : [ int pathId  => [ int packedDate => int count ] ]
        // $pathToId  : [ string path => int id ]  — string→int, looked up every line
        // $pathNames : [ int id => string path ]  — int→string, used only in packBinary
        // $dateCache : [ string "YYYY-MM-DD" => int packedDate ]  — ~3,650 unique entries
        $results   = [];
        $pathToId  = [];
        $pathNames = [];
        $dateCache = [];
        $nextId    = 0;
        $pathOffset = null;

        $tailLen   = 26; // Unix default
        $dateRight = 25;

        // ── EOL detection + first line ────────────────────────────────────
        $firstLine = \fgets($handle);
        if ($firstLine !== false) {
            $flen = \strlen($firstLine);
            if ($flen >= 2 && $firstLine[$flen - 2] === "\r") {
                $tailLen   = 27;
                $dateRight = 26;
            }
            $pathOffset = \strpos($firstLine, '/', 8);
            if ($pathOffset !== false) {
                $firstLine = \rtrim($firstLine, "\r\n");
                $rawPath = \substr($firstLine, $pathOffset, -$tailLen);
                $rawDate = \substr($firstLine, -$dateRight, 10);

                if (!isset($dateCache[$rawDate])) {
                    $y = (int)\substr($rawDate, 0, 4) - self::YEAR_BASE;
                    $m = (int)\substr($rawDate, 5, 2);
                    $d = (int)\substr($rawDate, 8, 2);
                    $dateCache[$rawDate] = ($y << 9) | ($m << 5) | $d;
                }
                if (!isset($pathToId[$rawPath])) {
                    $pathToId[$rawPath] = $nextId;
                    $pathNames[$nextId] = $rawPath;
                    $nextId++;
                }
                $id = $pathToId[$rawPath];
                $pd = $dateCache[$rawDate];
                isset($results[$id][$pd]) ? $results[$id][$pd]++ : $results[$id][$pd] = 1;
            }
        }

        $pos   = \ftell($handle);
        $carry = '';

        // ── fread hot loop ────────────────────────────────────────────────
        while ($pos < $end) {
            $toRead = \min(self::BLOCK_SIZE, $end - $pos);
            $block  = \fread($handle, $toRead);
            if ($block === false || $block === '') break;

            $pos += \strlen($block);

            if ($carry !== '') {
                $block = $carry . $block;
                $carry = '';
            }

            $lines = \explode("\n", $block);
            $carry = \array_pop($lines);

            $pathOffsetLocal = $pathOffset;
            $dateRightLocal = $dateRight;
            $tailLenLocal = $tailLen;
            foreach ($lines as $line) {
                if ($line === '') continue;

                // Negative-length substr: no strlen() in PHP bytecode.
                $rawPath = \substr($line, $pathOffsetLocal, -$tailLenLocal);
                $rawDate = \substr($line, -$dateRightLocal, 10);

                // dateCache miss rate: 0.004% (~3,650 unique dates in dataset)
                if (!isset($dateCache[$rawDate])) {
                    $y = (int)\substr($rawDate, 0, 4) - self::YEAR_BASE;
                    $m = (int)\substr($rawDate, 5, 2);
                    $d = (int)\substr($rawDate, 8, 2);
                    $dateCache[$rawDate] = ($y << 9) | ($m << 5) | $d;
                }

                // First encounter: assign sequential integer id, record name.
                // Repeat: one string HT lookup → int id (same cost as before).
                // Then: $results[$id][$pd]++ — int outer key stored inline,
                // no pointer chase, 2× bucket density vs string key.
                if (!isset($pathToId[$rawPath])) {
                    $pathToId[$rawPath] = $nextId;
                    $pathNames[$nextId] = $rawPath;
                    $nextId++;
                }
                $id = $pathToId[$rawPath];
                $pd = $dateCache[$rawDate];

                if (!isset($results[$id][$pd])) {
                    $results[$id][$pd] = 1;
                } else {
                    $results[$id][$pd]++;
                }
            }
        }

        // ── Boundary-spanning line ────────────────────────────────────────
        if ($carry !== '') {
            $rest = \fgets($handle);
            $line = \rtrim(($rest !== false ? $carry . $rest : $carry), "\r\n");
            if (\strlen($line) > $tailLen && $pathOffset !== null) {
                $rawPath = \substr($line, $pathOffset, -$tailLen);
                $rawDate = \substr($line, -$dateRight, 10);
                if (!isset($dateCache[$rawDate])) {
                    $y = (int)\substr($rawDate, 0, 4) - self::YEAR_BASE;
                    $m = (int)\substr($rawDate, 5, 2);
                    $d = (int)\substr($rawDate, 8, 2);
                    $dateCache[$rawDate] = ($y << 9) | ($m << 5) | $d;
                }
                if (!isset($pathToId[$rawPath])) {
                    $pathToId[$rawPath] = $nextId;
                    $pathNames[$nextId] = $rawPath;
                    $nextId++;
                }
                $id = $pathToId[$rawPath];
                $pd = $dateCache[$rawDate];
                isset($results[$id][$pd])
                    ? $results[$id][$pd]++
                    : $results[$id][$pd] = 1;
            }
        }

        \fclose($handle);
        return [$results, $pathNames];
    }

    // =========================================================================
    // BINARY PACK
    // =========================================================================
    //
    // $results   = [ int pathId => [ int packedDate => int count ] ]
    // $pathNames = [ int pathId => string path ]
    //
    // Resolves each integer pathId to its string for the wire format.
    // Called 50k times (unique paths), not 8.3M — cost is negligible.
    // Wire format and mergeInto() are UNCHANGED.

    private function packBinary(array $results, array $pathNames): string
    {
        $out = \pack('N', \count($results));

        foreach ($results as $id => $dateCounts) {
            $path = $pathNames[$id]; // int → string, one lookup per unique path
            $out .= \pack('N', \strlen($path));
            $out .= $path;
            $out .= pack('N', count($dateCounts));

            $pairs = [];
            foreach ($dateCounts as $pd => $cnt) {
                $pairs[] = $pd;
                $pairs[] = $cnt;
            }
            $out .= \pack('N*', ...$pairs);
        }

        return $out;
    }

    // =========================================================================
    // BINARY MERGE
    // =========================================================================

    private function mergeInto(array &$final, string $buf): void
    {
        $offset = 0;
        $bufLen = \strlen($buf);
        if ($bufLen < 4) return;

        [, $pathCount] = \unpack('N', \substr($buf, $offset, 4));
        $offset += 4;

        for ($p = 0; $p < $pathCount; $p++) {
            if ($offset + 4 > $bufLen) break;
            [, $plen] = \unpack('N', \substr($buf, $offset, 4));
            $offset  += 4;

            $path    = \substr($buf, $offset, $plen);
            $offset += $plen;

            if ($offset + 4 > $bufLen) break;
            [, $dcnt] = \unpack('N', \substr($buf, $offset, 4));
            $offset  += 4;

            if ($dcnt === 0) continue;

            $blockLen = $dcnt * 8;
            $pairs    = \unpack('N*', \substr($buf, $offset, $blockLen));
            $offset  += $blockLen;

            for ($j = 1, $jmax = $dcnt * 2; $j <= $jmax; $j += 2) {
                $pd  = $pairs[$j];
                $cnt = $pairs[$j + 1];
                isset($final[$path][$pd])
                    ? $final[$path][$pd] += $cnt
                    : $final[$path][$pd]  = $cnt;
            }
        }
    }

    // =========================================================================
    // HELPERS
    // =========================================================================

    private function detectCores(): int
    {
        // On macOS (M1/M2/M3/M4), all cores are physical, but some are "Efficiency" cores.
        // Performance is best when using all of them.
        if (PHP_OS_FAMILY === 'Darwin') {
            $n = (int) shell_exec('sysctl -n hw.physicalcpu');
            return $n > 0 ? $n : 8;
        }

        // On Linux, we check the difference between 'cpu cores' and 'siblings'
        // to detect if Hyper-threading is active.
        if (is_readable('/proc/cpuinfo')) {
            $cpuinfo = file_get_contents('/proc/cpuinfo');
            preg_match_all('/^cpu cores\s+:\s+(\d+)/m', $cpuinfo, $matches);
            if (!empty($matches[1])) {
                // Sum of physical cores across all sockets
                return (int) array_sum($matches[1]);
            }
        }

        return 4; // Conservative fallback
    }
}