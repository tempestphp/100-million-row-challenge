<?php

declare(strict_types=1);

namespace App;

final class Parser
{
    // 1 MB read buffer fits well in L3 cache for strpos scanning
    private const BUFFER_SIZE = 1 * 1024 * 1024;

    // 8 MB discover scan covers ~109K lines, ensuring all ~1818 unique dates
    // are found (at 1 MB only ~13.6K lines → ~1 date statistically missed)
    private const DISCOVER_BYTES = 8 * 1024 * 1024;

    public function parse(string $inputPath, string $outputPath): void
    {
        // No circular references created; eliminate cyclic GC scan overhead
        gc_disable();

        $fileSize = filesize($inputPath);

        // Build path/date-to-integer-ID mappings from the first 1MB of input
        $disc = $this->discover($inputPath);

        // Use parallel processing only when pcntl is available and file exceeds 64 MB
        if (function_exists('pcntl_fork') && $fileSize > 64 * 1024 * 1024) {
            $counts = $this->parallelParse($inputPath, $fileSize, $disc);
        } else {
            $counts = $this->processSegment($inputPath, 0, $fileSize, $disc);
        }

        $this->writeJsonOutput($outputPath, $counts, $disc);
    }

    /**
     * Scans the first scanBytes of the input file to discover all unique
     * paths and dates, assigning sequential integer IDs in first-encounter order.
     *
     * @return array{pathIds: array<string, int>, dateIds: array<string, int>,
     *               pathNames: array<int, string>, dateNames: array<int, string>,
     *               pathCount: int, dateCount: int}
     */
    public function discover(string $inputPath, int $scanBytes = self::DISCOVER_BYTES): array
    {
        $fileSize = filesize($inputPath);
        $readSize = min($scanBytes, $fileSize);

        $fp = fopen($inputPath, 'rb');
        $chunk = fread($fp, $readSize);
        fclose($fp);

        // Trim to last complete line
        $lastNl = strrpos($chunk, "\n");
        if ($lastNl !== false) {
            $chunk = substr($chunk, 0, $lastNl);
        }

        $pathIds   = [];
        $dateIds   = [];
        $pathNames = [];
        $dateNames = [];
        $pathCount = 0;
        $dateCount = 0;

        $lines = explode("\n", $chunk);
        foreach ($lines as $line) {
            if ($line === '') {
                continue;
            }
            // Timestamp is 25 chars (YYYY-MM-DDTHH:MM:SS+00:00), comma before it
            $comma = strlen($line) - 26;
            // Prefix "https://stitcher.io" = 19 chars, offset 19 preserves leading /
            $path = substr($line, 19, $comma - 19);
            $date = substr($line, $comma + 1, 10);

            if (!isset($pathIds[$path])) {
                $pathIds[$path] = $pathCount;
                $pathNames[$pathCount] = $path;
                $pathCount++;
            }
            if (!isset($dateIds[$date])) {
                $dateIds[$date] = $dateCount;
                $dateNames[$dateCount] = $date;
                $dateCount++;
            }
        }

        return [
            'pathIds'   => $pathIds,
            'dateIds'   => $dateIds,
            'pathNames' => $pathNames,
            'dateNames' => $dateNames,
            'pathCount' => $pathCount,
            'dateCount' => $dateCount,
        ];
    }

    /**
     * Processes a file segment using in-buffer strpos scanning and flat integer
     * array indexing. Returns a flat array of counts indexed by pathId * dateCount + dateId.
     *
     * @return int[]
     */
    public function processSegment(string $inputPath, int $start, int $end, array $disc): array
    {
        $pathIds   = $disc['pathIds'];
        $dateIds   = $disc['dateIds'];
        $dateCount = $disc['dateCount'];
        $total     = $disc['pathCount'] * $dateCount;

        $counts = array_fill(0, $total, 0);

        $fp = fopen($inputPath, 'rb');
        if ($start > 0) {
            fseek($fp, $start);
        }

        $bufSize = self::BUFFER_SIZE;
        $tail    = '';
        $pos     = $start;

        while ($pos < $end) {
            $readLen = min($bufSize, $end - $pos);
            $raw = fread($fp, $readLen);
            if ($raw === false || $raw === '') {
                break;
            }
            $pos += strlen($raw);

            // Prepend leftover tail from previous chunk
            if ($tail !== '') {
                $chunk = $tail . $raw;
                $tail = '';
            } else {
                $chunk = $raw;
            }

            // Find last newline; everything after it is the tail for next iteration
            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) {
                // No newline at all in chunk; entire chunk is tail
                $tail = $chunk;
                continue;
            }

            if ($lastNl < strlen($chunk) - 1) {
                $tail = substr($chunk, $lastNl + 1);
            }

            // Scan lines within the buffer using strpos to avoid allocating intermediate arrays
            $scanPos = 0;
            while ($scanPos < $lastNl) {
                $nlPos = strpos($chunk, "\n", $scanPos);
                if ($nlPos === false || $nlPos > $lastNl) {
                    break;
                }

                $lineLen = $nlPos - $scanPos;
                // Minimum valid line: 19 (prefix) + 1 (path char) + 1 (comma) + 25 (timestamp) = 46
                if ($lineLen > 45) {
                    // Comma sits 26 chars before end of line (25-char timestamp + 1 comma)
                    $commaOffset = $scanPos + $lineLen - 26;
                    // Path starts at offset 19 from start of line
                    $pathStart = $scanPos + 19;
                    $pathLen = $commaOffset - $pathStart;
                    $path = substr($chunk, $pathStart, $pathLen);
                    $date = substr($chunk, $commaOffset + 1, 10);

                    if (isset($pathIds[$path]) && isset($dateIds[$date])) {
                        $counts[$pathIds[$path] * $dateCount + $dateIds[$date]]++;
                    }
                }

                $scanPos = $nlPos + 1;
            }
        }

        // Process the final incomplete line that had no trailing newline
        if ($tail !== '' && strlen($tail) > 45) {
            $comma = strlen($tail) - 26;
            $path = substr($tail, 19, $comma - 19);
            $date = substr($tail, $comma + 1, 10);

            if (isset($pathIds[$path]) && isset($dateIds[$date])) {
                $counts[$pathIds[$path] * $dateCount + $dateIds[$date]]++;
            }
        }

        fclose($fp);
        return $counts;
    }

    /**
     * Splits the file into 4 segments aligned to newline boundaries, forks 4 worker
     * processes, each writing its result as a packed binary file. Parent reads, unpacks,
     * and merges results via element-wise addition.
     *
     * @return int[]
     */
    private function parallelParse(string $inputPath, int $fileSize, array $disc): array
    {
        $workerCount = 4;
        $total = $disc['pathCount'] * $disc['dateCount'];

        // Calculate split points aligned to newline boundaries
        $boundaries = [0];
        $fp = fopen($inputPath, 'rb');
        for ($w = 1; $w < $workerCount; $w++) {
            $splitTarget = (int)($fileSize * $w / $workerCount);
            fseek($fp, $splitTarget);
            fgets($fp); // Read to next newline
            $aligned = ftell($fp);
            if ($aligned >= $fileSize) {
                // Degenerate case: fewer segments than workers
                break;
            }
            $boundaries[] = $aligned;
        }
        fclose($fp);
        $boundaries[] = $fileSize;

        $actualWorkers = count($boundaries) - 1;

        // Prefer /dev/shm for temp files (RAM-backed), fall back to sys_get_temp_dir
        $tmpDir = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();

        // Fork workers
        $pids = [];
        $tmpFiles = [];
        for ($w = 0; $w < $actualWorkers; $w++) {
            $tmp = tempnam($tmpDir, 'p_' . $w . '_');
            $tmpFiles[$w] = $tmp;

            $pid = pcntl_fork();
            if ($pid === 0) {
                // Child process
                $wCounts = $this->processSegment(
                    $inputPath,
                    $boundaries[$w],
                    $boundaries[$w + 1],
                    $disc
                );
                file_put_contents($tmp, $this->ipcSerialize($wCounts));
                exit(0);
            }
            $pids[$w] = $pid;
        }

        // Parent waits for all children
        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        // Read and merge results via element-wise addition
        $counts = array_fill(0, $total, 0);
        for ($w = 0; $w < $actualWorkers; $w++) {
            $raw = file_get_contents($tmpFiles[$w]);
            $wCounts = $this->ipcUnserialize($raw);
            unset($raw);
            unlink($tmpFiles[$w]);

            for ($i = 0; $i < $total; $i++) {
                $counts[$i] += $wCounts[$i];
            }
            unset($wCounts);
        }

        return $counts;
    }

    /**
     * Writes manual JSON output matching json_encode(JSON_PRETTY_PRINT) format exactly.
     * Iterates paths in first-encounter order (pathId sequence), inner dates sorted
     * chronologically. Forward slashes are escaped as \/ per default json_encode behavior.
     */
    private function writeJsonOutput(string $outputPath, array $counts, array $disc): void
    {
        $pathNames = $disc['pathNames'];
        $dateNames = $disc['dateNames'];
        $pathCount = $disc['pathCount'];
        $dateCount = $disc['dateCount'];

        $out = fopen($outputPath, 'wb');
        fwrite($out, '{');

        $firstPath = true;
        for ($pId = 0; $pId < $pathCount; $pId++) {
            // Collect non-zero date entries for this path
            $datePairs = [];
            $base = $pId * $dateCount;
            for ($dId = 0; $dId < $dateCount; $dId++) {
                $c = $counts[$base + $dId];
                if ($c > 0) {
                    $datePairs[$dateNames[$dId]] = $c;
                }
            }

            if (empty($datePairs)) {
                continue;
            }

            // Sort dates chronologically (YYYY-MM-DD lexicographic = chronological)
            ksort($datePairs);

            // Escape forward slashes in path key to match json_encode default
            $escapedPath = str_replace('/', '\\/', $pathNames[$pId]);

            if (!$firstPath) {
                fwrite($out, ',');
            }
            $firstPath = false;

            fwrite($out, "\n    \"{$escapedPath}\": {");

            $firstDate = true;
            foreach ($datePairs as $date => $count) {
                if (!$firstDate) {
                    fwrite($out, ',');
                }
                $firstDate = false;
                fwrite($out, "\n        \"{$date}\": {$count}");
            }

            fwrite($out, "\n    }");
        }

        fwrite($out, "\n}");
        fclose($out);
    }

    /**
     * Packs a flat integer array into a binary string using unsigned 32-bit
     * little-endian format for efficient IPC between parent and child processes.
     */
    private function ipcSerialize(array $data): string
    {
        return pack('V*', ...$data);
    }

    /**
     * Unpacks a binary string back into a 0-indexed flat integer array.
     * Applies array_values() because unpack('V*') returns 1-indexed keys.
     */
    private function ipcUnserialize(string $data): array
    {
        return array_values(unpack('V*', $data));
    }
}
