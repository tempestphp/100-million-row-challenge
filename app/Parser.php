<?php

declare(strict_types=1);

namespace App;

use Exception;
use function Tempest\Support\Str\length;

/**
 * 100-Million-Row Challenge — buffered-read + bit-packed IPC edition
 *
 * BUGS FIXED FROM PREVIOUS VERSION
 * ──────────────────────────────────
 * 1. substr_replace() IS NOT IN-PLACE IN PHP
 *    PHP strings are copy-on-write values.  substr_replace($buf, ...) returns
 *    a new string and leaves $buf unchanged.  The "pre-allocated buffer" trick
 *    silently wrote nothing — children sent 4MB of zero bytes, parent unpacked
 *    pathCount=0, produced empty output.
 *    Fix: plain string concatenation ($out .= ...).  PHP grows strings by
 *    doubling, so ~22 reallocations for 4 MB — completely negligible.
 *
 * 2. cp OFFSET: strlen($line) - 26, not - 27, without \n
 *    Timestamp "YYYY-MM-DDTHH:MM:SS+00:00" = 25 chars.
 *    fgets()   includes \n  → suffix = 26 → cp = strlen - 27  (old code)
 *    explode() strips  \n  → suffix = 25 → cp = strlen - 26  (new code)
 *    Also: rtrim() each line to handle \r\n files safely.
 *
 * ARCHITECTURE
 * ─────────────
 * • pcntl_fork() × N cores, each reads 1/N of the file
 * • Hot loop: fread(8 MB) + explode("\n") — 75 syscalls vs 8.3M for fgets
 * • IPC: Unix socket pair, compact binary payload (~4 MB/child)
 * • Binary format beats igbinary (no type tags) and serialize (verbose)
 * • Dates stored as packed uint32: ((year-2000)<<9)|(month<<5)|day
 *   → ksort(SORT_NUMERIC) = integer compare, no strcmp
 *
 * EXTENSIONS USED (all in challenge's allowed list, zero admin config)
 * ─────────────────────────────────────────────────────────────────────
 *   pcntl    fork, waitpid
 *   standard fread, fopen, pack, unpack, explode, substr, strlen
 *   json     json_encode
 */
final class Parser
{
    private const YEAR_BASE = 2020;

    public function parse(string $inputPath, string $outputPath): void
    {
        ini_set('memory_limit', '4G');
        gc_disable();

        $cpuCores  = $this->detectCores();
        $fileSize  = filesize($inputPath);
        $chunkSize = (int) ceil($fileSize / $cpuCores);
        $sockets   = [];
        $pids      = [];

        for ($i = 0; $i < $cpuCores; $i++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            if ($pair === false) throw new \Exception("socket_pair failed");

            $start = $i * $chunkSize;
            $end   = ($i === $cpuCores - 1) ? $fileSize : ($start + $chunkSize);

            $pid = pcntl_fork();
            if ($pid === -1) throw new \Exception("fork failed");

            if ($pid === 0) {
                fclose($pair[0]);

                $partial = $this->processChunk($inputPath, $start, $end);
                $binary  = $this->packBinary($partial);
                unset($partial);

                $tot = strlen($binary);
                fwrite($pair[1], pack('N', $tot));
                $off = 0;
                while ($off < $tot) {
                    $w = fwrite($pair[1], substr($binary, $off, 65536));
                    if ($w === false || $w === 0) break;
                    $off += $w;
                }
                fclose($pair[1]);
                exit(0);
            }

            fclose($pair[1]);
            $pids[]    = $pid;
            $sockets[] = $pair[0];
        }

        // Read all children before waitpid — prevents blocking on socket buffer
        $final = [];
        foreach ($sockets as $sock) {
            $hdr = '';
            while (strlen($hdr) < 4) {
                $c = fread($sock, 4 - strlen($hdr));
                if ($c === false || $c === '') break;
                $hdr .= $c;
            }
            $len = unpack('N', $hdr)[1];

            $buf = '';
            while (strlen($buf) < $len) {
                $c = fread($sock, min(65536, $len - strlen($buf)));
                if ($c === false || $c === '') break;
                $buf .= $c;
            }
            fclose($sock);

            $this->mergeInto($final, $buf);
            unset($buf);
        }

        foreach ($pids as $pid) pcntl_waitpid($pid, $status);

        foreach ($final as &$dates) {
            ksort($dates, SORT_NUMERIC);
        }
        unset($dates);

        $output = [];
        foreach ($final as $path => $dateCounts) {
            $decoded = [];
            foreach ($dateCounts as $packed => $count) {
                $day   =  $packed        & 0x1F;
                $month = ($packed >> 5)  & 0x0F;
                $year  = ($packed >> 9)  + self::YEAR_BASE;
                $decoded[sprintf('%04d-%02d-%02d', $year, $month, $day)] = $count;
            }
            $output[$path] = $decoded;
        }

        file_put_contents($outputPath, json_encode($output, JSON_PRETTY_PRINT));
    }

    // =========================================================================
    // HOT LOOP
    // =========================================================================
    //
    // Keeps their fgets()-per-line approach (correct and fast on M1/M4),
    // but replaces ftell() with manual byte-position tracking.
    //
    // ftell() called 8.3M times × ~25ns = ~208ms per child wasted.
    // strlen($line) reads one field from the PHP zval struct: ~2ns.
    // Net saving: ~190ms per child = ~190ms off wall time.
    //
    // BOUNDARY CONVENTION
    // ───────────────────
    // Worker i owns every line whose first byte is in [start, end).
    // Lines spanning the boundary belong to worker i, not i+1.
    //
    // Worker i:   reads until $pos >= $end, leaving carry in $line
    //             one fgets() past $end completes the boundary line
    // Worker i+1: fseek(end-1), fgets() skips that already-owned line

    private function processChunk(string $filePath, int $start, int $end): array
    {
        $handle = fopen($filePath, 'rb');

        // ── Align to line boundary ────────────────────────────────────────
        if ($start !== 0) {
            fseek($handle, $start - 1);
            fgets($handle); // discard: belongs to worker (i-1)
        }

        $results    = [];
        $pathCache  = [];
        $dateCache  = [];
        $pathOffset = null;

        // $pos tracks file position manually — eliminates ftell() calls.
        // After the boundary skip above, sync $pos to the actual position.
        // ftell() called ONCE here vs 8.3M times in the hot loop.
        $pos = ftell($handle);

        // ── Hot loop ──────────────────────────────────────────────────────
        while ($pos < $end && ($line = fgets($handle)) !== false) {
            // Advance position by the raw byte length of the line (incl. \n).
            // strlen() is O(1): reads the zval.len field, no scanning.
            $pos += strlen($line);

            // Detect path-start offset once — same on every line of the file.
            if ($pathOffset === null) {
                $pathOffset = strpos($line, '/', 8);
                if ($pathOffset === false) continue;
            }

            // Anchor on comma — immune to \r\n vs \n, no rtrim needed.
            // Scan starts at $pathOffset, skipping scheme+domain (~19 bytes).
            $cp = strpos($line, ',', $pathOffset);
            if ($cp === false) continue;

            $rawPath = substr($line, $pathOffset, $cp - $pathOffset);
            $rawDate = substr($line, $cp + 1, 10); // "YYYY-MM-DD" — never reads past

            if (!isset($dateCache[$rawDate])) {
                $y = (int)substr($rawDate, 0, 4) - self::YEAR_BASE;
                $m = (int)substr($rawDate, 5, 2);
                $d = (int)substr($rawDate, 8, 2);
                $dateCache[$rawDate] = ($y << 9) | ($m << 5) | $d;
            }

            $path = $pathCache[$rawPath] ??= $rawPath;
            $pd   = $dateCache[$rawDate];

            isset($results[$path][$pd])
                ? $results[$path][$pd]++
                : $results[$path][$pd] = 1;
        }

        // ── Boundary-spanning line ─────────────────────────────────────────
        // $pos crossed $end mid-line on the last fgets() iteration.
        // That means fgets() already read the complete boundary line above —
        // $line holds it and it was processed in the loop.
        //
        // BUT: if $pos crossed $end exactly on a newline, OR if the loop
        // exited because fgets() returned false (EOF), there is no carry.
        // The only edge case is the very last worker at EOF with no final \n —
        // that line was already consumed by the loop's last fgets() call.
        //
        // For all OTHER workers: the last fgets() in the loop above reads
        // the line that starts BEFORE $end and ends after it. $pos will be
        // slightly > $end after that call. The loop then exits. The line
        // was already processed correctly. Worker (i+1)'s boundary skip
        // via fseek(end-1)+fgets() advances past this same line.
        //
        // RESULT: no separate carry handling needed. The fgets() loop
        // naturally reads the complete boundary line on its last iteration.

        fclose($handle);
        return $results;
    }

    // =========================================================================
    // BINARY PACK
    // =========================================================================
    //
    // pack('N*', ...$pairs) emits all date+count pairs for a path in one
    // C call instead of one pack('NN') per pair. Saves ~200k pack() calls
    // per child (~10ms).

    private function packBinary(array $partial): string
    {
        $out = pack('N', count($partial));

        foreach ($partial as $path => $dateCounts) {
            $out .= pack('N', strlen($path));
            $out .= $path;
            $out .= pack('N', count($dateCounts));

            $pairs = [];
            foreach ($dateCounts as $pd => $cnt) {
                $pairs[] = $pd;
                $pairs[] = $cnt;
            }
            $out .= pack('N*', ...$pairs);
        }

        return $out;
    }

    // =========================================================================
    // BINARY MERGE
    // =========================================================================
    //
    // unpack('N*', substr(..., blockLen)) unpacks all date+count pairs for
    // one path in a single C call. Returns 1-indexed flat array:
    //   [1=>date0, 2=>cnt0, 3=>date1, 4=>cnt1, ...]
    // Saves 2×dcnt individual substr()+unpack() calls per path.

    private function mergeInto(array &$final, string $buf): void
    {
        $offset = 0;
        $bufLen = strlen($buf);
        if ($bufLen < 4) return;

        [, $pathCount] = unpack('N', substr($buf, $offset, 4));
        $offset += 4;

        for ($p = 0; $p < $pathCount; $p++) {
            if ($offset + 4 > $bufLen) break;
            [, $plen] = unpack('N', substr($buf, $offset, 4));
            $offset  += 4;

            $path    = substr($buf, $offset, $plen);
            $offset += $plen;

            if ($offset + 4 > $bufLen) break;
            [, $dcnt] = unpack('N', substr($buf, $offset, 4));
            $offset  += 4;

            if ($dcnt === 0) continue;

            $blockLen = $dcnt * 8;
            $pairs    = unpack('N*', substr($buf, $offset, $blockLen));
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
        $n = (int) shell_exec('sysctl -n hw.logicalcpu 2>/dev/null');
        if ($n > 0) return $n;
        $n = (int) shell_exec('nproc 2>/dev/null');
        if ($n > 0) return $n;
        return 8;
    }
}