<?php

declare(strict_types=1);

namespace App;

final class Parser
{
    // Empirically, 1-4 MB outperforms larger buffers due to L3 cache pressure
    // during explode() string scanning. Benchmark on target server to finalize.
    private const BUFFER_SIZE = 1 * 1024 * 1024;

    public function parse(string $inputPath, string $outputPath): void
    {
        // No circular references created; eliminate cyclic GC scan overhead
        gc_disable();

        $fileSize = filesize($inputPath);

        if (function_exists('pcntl_fork') && $fileSize > 64 * 1024 * 1024) {
            $results = $this->parallelParse($inputPath, $fileSize);
        } else {
            $results = $this->processSegment($inputPath, 0, $fileSize);
        }

        // Do NOT sort outer paths: expected output preserves Visit::all()
        // insertion order, not alphabetical. Byte-for-byte validator rejects
        // any reordering of outer keys.
        // Sort inner date arrays: YYYY-MM-DD lexicographic == chronological.
        foreach ($results as &$dates) {
            ksort($dates);
        }
        unset($dates);

        // JSON_PRETTY_PRINT without JSON_UNESCAPED_SLASHES: forward slashes
        // encode as \/ matching the reference expected output format.
        file_put_contents($outputPath, json_encode($results, JSON_PRETTY_PRINT));
    }

    private function parallelParse(string $inputPath, int $fileSize): array
    {
        // Align split to newline boundary: seek to midpoint, read to next \n
        $fp = fopen($inputPath, 'rb');
        fseek($fp, (int) ($fileSize / 2));
        fgets($fp);
        $mid = ftell($fp);
        fclose($fp);

        // Degenerate case: fgets() reached EOF (file has very few lines)
        if ($mid >= $fileSize) {
            return $this->processSegment($inputPath, 0, $fileSize);
        }

        $tmp1 = tempnam(sys_get_temp_dir(), 'p_1_');
        $tmp2 = tempnam(sys_get_temp_dir(), 'p_2_');

        $pid1 = pcntl_fork();
        if ($pid1 === 0) {
            $r = $this->processSegment($inputPath, 0, $mid);
            file_put_contents($tmp1, $this->ipcSerialize($r));
            exit(0);
        }

        $pid2 = pcntl_fork();
        if ($pid2 === 0) {
            $r = $this->processSegment($inputPath, $mid, $fileSize);
            file_put_contents($tmp2, $this->ipcSerialize($r));
            exit(0);
        }

        pcntl_waitpid($pid1, $s1);
        pcntl_waitpid($pid2, $s2);

        // Deserialize and free raw bytes immediately to minimize peak memory
        $raw1 = file_get_contents($tmp1);
        $r1   = $this->ipcUnserialize($raw1);
        unset($raw1);
        unlink($tmp1);

        $raw2 = file_get_contents($tmp2);
        $r2   = $this->ipcUnserialize($raw2);
        unset($raw2);
        unlink($tmp2);

        // Merge r2 into r1; with random input most (path,date) pairs
        // appear in both halves so the isset branch dominates
        foreach ($r2 as $path => $dates) {
            if (isset($r1[$path])) {
                foreach ($dates as $date => $count) {
                    if (isset($r1[$path][$date])) {
                        $r1[$path][$date] += $count;
                    } else {
                        $r1[$path][$date] = $count;
                    }
                }
            } else {
                $r1[$path] = $dates;
            }
        }

        return $r1;
    }

    private function processSegment(string $inputPath, int $start, int $end): array
    {
        $fp = fopen($inputPath, 'rb');
        if ($start > 0) {
            fseek($fp, $start);
        }

        $bufSize = self::BUFFER_SIZE;
        $tail    = '';
        $results = [];
        $pos     = $start;

        while ($pos < $end) {
            $chunk = fread($fp, min($bufSize, $end - $pos));
            if ($chunk === false || $chunk === '') {
                break;
            }
            $pos += strlen($chunk);

            $lines = explode("\n", $tail !== '' ? $tail . $chunk : $chunk);
            $tail  = array_pop($lines);

            foreach ($lines as $line) {
                if ($line === '') {
                    continue;
                }
                // Timestamp is exactly 25 chars (YYYY-MM-DDTHH:MM:SS+00:00).
                // Comma is at strlen($line) - 26. strlen() is O(1) in PHP 8
                // (reads zend_string.len header), avoiding memchr scan.
                $comma = strlen($line) - 26;

                // Prefix "https://stitcher.io" = 19 chars.
                // Offset 19 preserves the leading "/" in path keys.
                $path = substr($line, 19, $comma - 19);
                $date = substr($line, $comma + 1, 10);

                // isset avoids double outer hash lookup (FETCH_DIM_R + FETCH_DIM_RW)
                // that occurs with ($results[$p][$d] ?? 0) + 1 pattern.
                // After warmup, branch predictor converges on the isset path.
                if (isset($results[$path][$date])) {
                    ++$results[$path][$date];
                } else {
                    $results[$path][$date] = 1;
                }
            }

            // Release intermediate string array before next fread()
            unset($lines);
        }

        // Defensive: process remaining tail after loop.
        // When generator appends \n to every row (including last),
        // $tail is '' here and this block is a no-op.
        if ($tail !== '') {
            $comma = strlen($tail) - 26;
            $path  = substr($tail, 19, $comma - 19);
            $date  = substr($tail, $comma + 1, 10);
            if (isset($results[$path][$date])) {
                ++$results[$path][$date];
            } else {
                $results[$path][$date] = 1;
            }
        }

        fclose($fp);
        return $results;
    }

    /**
     * igbinary uses string deduplication (~1,826 unique dates stored once
     * rather than repeated per inner array). Estimated IPC payload: ~2-5 MB
     * per child. Falls back to native serialize() when igbinary unavailable.
     */
    private function ipcSerialize(array $data): string
    {
        return function_exists('igbinary_serialize')
            ? igbinary_serialize($data)
            : serialize($data);
    }

    private function ipcUnserialize(string $data): array
    {
        return function_exists('igbinary_unserialize')
            ? igbinary_unserialize($data)
            : unserialize($data, ['allowed_classes' => false]);
    }
}
