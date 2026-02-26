<?php

namespace App;

use Exception;

final class Parser
{
    private const int DOMAIN_PREFIX_LENGTH = 19; // strlen('https://stitcher.io')
    private const int TIMESTAMP_LENGTH = 25;      // YYYY-MM-DDTHH:MM:SS+00:00
    private const int READ_CHUNK_SIZE = 256 * 1024 * 1024;   // 32 MB
    private const int WRITE_BUFFER_SIZE = 256 * 1024 * 1024; // 32 MB

    public function parse(string $inputPath, string $outputPath): void
    {
        if (!is_file($inputPath)) {
            throw new Exception("Input file does not exist: $inputPath");
        }

        $fileSize = filesize($inputPath);

        if ($fileSize === false || $fileSize === 0) {
            file_put_contents($outputPath, '{}');
            return;
        }

        // Auto-detect workers: nproc (Linux) or sysctl (macOS), capped at 16, env-overridable
        $numWorkers = $this->getWorkerCount($fileSize);
        $boundaries  = $this->findChunkBoundaries($inputPath, $fileSize, $numWorkers);
        $actualWorkers = count($boundaries);

        // Use /dev/shm (RAM-backed tmpfs) when available — pure in-memory IPC
        $tmpDir = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();

        // Reserve temp files before forking
        $tempFiles = [];
        for ($i = 0; $i < $actualWorkers; $i++) {
            $tempFiles[$i] = tempnam($tmpDir, 'prs_');
        }

        // Fork N-1 child workers; parent handles the last chunk
        $pids = [];
        for ($i = 0; $i < $actualWorkers - 1; $i++) {
            $pid = pcntl_fork();
            if ($pid === -1) {
                // Clean up already-created temp files before throwing
                foreach ($tempFiles as $f) {
                    @unlink($f);
                }
                throw new Exception("Failed to fork worker $i");
            }
            if ($pid === 0) {
                // --- Child process ---
                file_put_contents($tempFiles[$i], $this->processChunk(
                    $inputPath,
                    $boundaries[$i][0],
                    $boundaries[$i][1],
                ));
                exit(0);
            }
            $pids[$i] = $pid;
        }

        // Parent handles last chunk directly
        $lastIdx = $actualWorkers - 1;
        file_put_contents($tempFiles[$lastIdx], $this->processChunk(
            $inputPath,
            $boundaries[$lastIdx][0],
            $boundaries[$lastIdx][1],
        ));

        // Wait for all children
        foreach ($pids as $i => $pid) {
            pcntl_waitpid($pid, $status);
            if (!pcntl_wifexited($status) || pcntl_wexitstatus($status) !== 0) {
                foreach ($tempFiles as $f) {
                    @unlink($f);
                }
                throw new Exception("Worker $i exited with failure (status=$status)");
            }
        }

        // Merge all chunk results in file order (chunk 0 first = correct first-appearance path order)
        [$pathOrder, $intCounts, $sortedDates] = $this->mergeChunks($tempFiles);

        foreach ($tempFiles as $f) {
            @unlink($f);
        }

        $this->writeJsonOutput($outputPath, $pathOrder, $intCounts, $sortedDates);
    }

    /**
     * Determine optimal worker count.
     * Reads nproc/sysctl, caps at 16, respects PARSER_WORKERS env var.
     * Never spawns more workers than can be meaningfully fed (min 1MB per worker).
     */
    private function getWorkerCount(int $fileSize): int
    {
        $env = getenv('PARSER_WORKERS');
        if ($env !== false && ctype_digit($env) && (int) $env > 0) {
            $requested = (int) $env;
        } else {
            if (PHP_OS_FAMILY === 'Darwin') {
                $detected = (int) shell_exec('sysctl -n hw.logicalcpu 2>/dev/null');
            } elseif (is_dir('/sys/devices/system/cpu')) {
                // Avoid shell_exec on Linux — count cpu[0-9]* dirs directly
                $cpuDirs  = glob('/sys/devices/system/cpu/cpu[0-9]*', GLOB_ONLYDIR);
                $detected = $cpuDirs !== false ? count($cpuDirs) : 4;
            } else {
                $detected = (int) shell_exec('nproc --all 2>/dev/null');
            }
            $requested = max(1, min(16, $detected ?: 4));
        }

        // One worker per 18 MB of input.
        // • 72 MB  (1M rows)   → 4 workers  (sweet spot benchmarked locally)
        // • 7.2 GB (100M rows) → 16 workers (capped)
        $maxBySize = max(1, (int) ceil($fileSize / (18 * 1024 * 1024)));
        return min($requested, $maxBySize);
    }

    /**
     * Divide the file into $n line-aligned byte ranges.
     * Returns array of [start, end] pairs covering [0, fileSize].
     */
    private function findChunkBoundaries(string $path, int $fileSize, int $n): array
    {
        if ($n === 1) {
            return [[0, $fileSize]];
        }

        $chunkSize  = (int) ceil($fileSize / $n);
        $boundaries = [];
        $fh         = fopen($path, 'rb');
        $start      = 0;

        for ($i = 0; $i < $n; $i++) {
            $rawEnd = $start + $chunkSize;

            if ($rawEnd >= $fileSize) {
                $boundaries[] = [$start, $fileSize];
                break;
            }

            // Advance to the end of the line that straddles rawEnd
            fseek($fh, $rawEnd);
            fgets($fh); // discard remainder of partial line
            $alignedEnd = ftell($fh);

            if ($alignedEnd >= $fileSize) {
                $boundaries[] = [$start, $fileSize];
                break;
            }

            $boundaries[] = [$start, $alignedEnd];
            $start = $alignedEnd;
        }

        fclose($fh);
        return $boundaries;
    }

    /**
     * Parse one byte range [$start, $end) of the input file.
     *
     * Uses integer path/day IDs in the hot loop (same strategy as the original
     * parser) — integer hash-table lookups in PHP are significantly faster than
     * string lookups.  Converts to compact binary output only once at the end.
     *
     * Returns a packed binary string ready to write to the IPC temp file.
     */
    private function processChunk(string $inputPath, int $start, int $end): string
    {
        $fh = fopen($inputPath, 'rb');
        if ($fh === false) {
            throw new Exception("Failed to open input file: $inputPath");
        }

        if ($start > 0) {
            fseek($fh, $start);
        }

        // Integer-keyed structures ‑ fast PHP hashtable access in hot loop
        $pathIdByPath   = [];
        $pathStrings    = [];
        $countsByPathId = []; // [int pathId => [int dayIndex => int count]]
        $dayIndexByDate = [];
        $dayDates       = []; // [int dayIndex => string date]
        $dayInts        = []; // [int dayIndex => int YYYYMMDD]

        $carry = '';

        while (!feof($fh)) {
            $pos = ftell($fh);
            if ($pos >= $end) {
                break;
            }

            $toRead = min(self::READ_CHUNK_SIZE, $end - $pos);
            $chunk  = fread($fh, $toRead);
            if ($chunk === false || $chunk === '') {
                break;
            }

            $buffer    = $carry . $chunk;
            $bufferLen = strlen($buffer);
            $lineStart = 0;

            while (true) {
                $lineEndPos = strpos($buffer, "\n", $lineStart);
                if ($lineEndPos === false) {
                    break;
                }

                $commaPos = $lineEndPos - (self::TIMESTAMP_LENGTH + 1);
                if ($commaPos <= $lineStart) {
                    $lineStart = $lineEndPos + 1;
                    continue;
                }

                $dateStart = $commaPos + 1;
                $date      = substr($buffer, $dateStart, 10);

                if (isset($dayIndexByDate[$date])) {
                    $dayIndex = $dayIndexByDate[$date];
                } else {
                    $dayIndex = count($dayIndexByDate);
                    $dayIndexByDate[$date] = $dayIndex;
                    $dayDates[$dayIndex]   = $date;
                    $dayInts[$dayIndex]    = (int) str_replace('-', '', $date);
                }

                $pathStart = $lineStart + self::DOMAIN_PREFIX_LENGTH;
                if ($pathStart > $commaPos) {
                    $lineStart = $lineEndPos + 1;
                    continue;
                }

                $pathLength = $commaPos - $pathStart;
                $path = $pathLength === 0 ? '/' : substr($buffer, $pathStart, $pathLength);

                if (isset($pathIdByPath[$path])) {
                    $pathId = $pathIdByPath[$path];
                } else {
                    $pathId = count($pathStrings);
                    $pathIdByPath[$path]    = $pathId;
                    $pathStrings[$pathId]   = $path;
                    $countsByPathId[$pathId] = [];
                }

                $daySlot = &$countsByPathId[$pathId];
                if (isset($daySlot[$dayIndex])) {
                    ++$daySlot[$dayIndex];
                } else {
                    $daySlot[$dayIndex] = 1;
                }
                unset($daySlot);

                $lineStart = $lineEndPos + 1;
            }

            $carry = $lineStart < $bufferLen ? substr($buffer, $lineStart) : '';
        }

        fclose($fh);

        // Handle final line with no trailing newline
        if ($carry !== '') {
            $lineEndPos = strlen($carry);
            $commaPos   = $lineEndPos - (self::TIMESTAMP_LENGTH + 1);

            if ($commaPos > 0) {
                $dateStart = $commaPos + 1;
                $date      = substr($carry, $dateStart, 10);

                if (isset($dayIndexByDate[$date])) {
                    $dayIndex = $dayIndexByDate[$date];
                } else {
                    $dayIndex = count($dayIndexByDate);
                    $dayIndexByDate[$date] = $dayIndex;
                    $dayDates[$dayIndex]   = $date;
                    $dayInts[$dayIndex]    = (int) str_replace('-', '', $date);
                }

                $pathStart  = self::DOMAIN_PREFIX_LENGTH;
                $pathLength = $commaPos - $pathStart;

                if ($pathLength >= 0) {
                    $path = $pathLength === 0 ? '/' : substr($carry, $pathStart, $pathLength);

                    if (isset($pathIdByPath[$path])) {
                        $pathId = $pathIdByPath[$path];
                    } else {
                        $pathId = count($pathStrings);
                        $pathIdByPath[$path]    = $pathId;
                        $pathStrings[$pathId]   = $path;
                        $countsByPathId[$pathId] = [];
                    }

                    $daySlot = &$countsByPathId[$pathId];
                    if (isset($daySlot[$dayIndex])) {
                        ++$daySlot[$dayIndex];
                    } else {
                        $daySlot[$dayIndex] = 1;
                    }
                    unset($daySlot);
                }
            }
        }

        return $this->packChunk($pathStrings, $countsByPathId, $dayDates, $dayInts);
    }

    /**
     * Merge chunk results in file order (chunk 0 -> N-1).
     *
     * Two-phase approach to eliminate string-key pressure in the merge hot loop:
     * Phase 1 -- scan metadata from all chunks to build global integer ID maps.
     * Phase 2 -- merge counts using local->global int->int translation tables.
     *   The inner hot loop only touches integer keys, no string hashing.
     *
     * Returns [pathOrder[], intCounts[globalPathId][globalDateId => int], sortedDates[]].
     */
    private function mergeChunks(array $tempFiles): array
    {
        // Phase 1: collect all unique paths + dates; keep unpacked chunk data in memory.
        $chunkMeta = []; // per chunk: [pathStrings[], localCounts[], dayDates[], dayInts[]]
        $pathOrder = [];
        $pathSeen  = [];
        $allDayIds = []; // date -> YYYYMMDD int

        foreach ($tempFiles as $tempFile) {
            [$pathStrings, $localCounts, $dayDates, $dayInts] = $this->unpackChunk(
                file_get_contents($tempFile),
            );
            $chunkMeta[] = [$pathStrings, $localCounts, $dayDates, $dayInts];

            foreach ($dayDates as $localIdx => $date) {
                if (!isset($allDayIds[$date])) {
                    $allDayIds[$date] = $dayInts[$localIdx];
                }
            }
            foreach ($pathStrings as $path) {
                if (!isset($pathSeen[$path])) {
                    $pathSeen[$path] = true;
                    $pathOrder[]     = $path;
                }
            }
        }

        // Build global integer mappings (string lookups done once, not per-row)
        $globalPathIds = array_flip($pathOrder);   // path -> int
        asort($allDayIds);                          // sort by YYYYMMDD int
        $sortedDates   = array_keys($allDayIds);    // [int dateId -> date string] in sorted order
        $globalDateIds = array_flip($sortedDates);  // date -> int

        // Phase 2: merge with integer-keyed hot loop
        $intCounts = []; // [int globalPathId][int globalDateId] = count

        foreach ($chunkMeta as [$pathStrings, $localCounts, $dayDates,]) {
            // Precompute local day index -> global date id (int->int, once per chunk)
            $localToGlobal = [];
            foreach ($dayDates as $localIdx => $date) {
                $localToGlobal[$localIdx] = $globalDateIds[$date];
            }

            foreach ($pathStrings as $localPathId => $path) {
                $gPathId = $globalPathIds[$path];
                if (!isset($intCounts[$gPathId])) {
                    $intCounts[$gPathId] = [];
                }
                $slot = &$intCounts[$gPathId];
                // Hot loop: integer keys only -- no string hashing
                foreach ($localCounts[$localPathId] as $localDayId => $count) {
                    $gDateId = $localToGlobal[$localDayId]; // int->int
                    if (isset($slot[$gDateId])) {
                        $slot[$gDateId] += $count;
                    } else {
                        $slot[$gDateId] = $count;
                    }
                }
                unset($slot);
            }
        }

        return [$pathOrder, $intCounts, $sortedDates];
    }

    /**
     * Compact binary serialisation for inter-process chunk results.
     * Takes integer-keyed internal structures from processChunk — no conversion needed.
     *
     * Format:
     *   uint32  dayCount
     *   per day:  10 bytes date + uint32 dayInt (YYYYMMDD)
     *   uint32  pathCount
     *   per path: uint16 path_len + uint32 dateCount + path_bytes
     *             per date: 10 bytes date + uint32 count
     */
    private function packChunk(
        array $pathStrings,
        array $countsByPathId,
        array $dayDates,
        array $dayInts,
    ): string {
        $dayCount = count($dayDates);
        $buf      = pack('N', $dayCount);
        for ($i = 0; $i < $dayCount; $i++) {
            $buf .= $dayDates[$i] . pack('N', $dayInts[$i]);
        }

        $pathCount = count($pathStrings);
        $buf      .= pack('N', $pathCount);
        for ($pathId = 0; $pathId < $pathCount; $pathId++) {
            $path       = $pathStrings[$pathId];
            $dateCounts = $countsByPathId[$pathId]; // [int localDayIndex => int count]
            $dateCount  = count($dateCounts);
            $buf       .= pack('nN', strlen($path), $dateCount) . $path;
            foreach ($dateCounts as $localDayIndex => $count) {
                // Store local day index (uint16) + count (uint32) = 6 bytes
                // (vs 10-byte date string + uint32 = 14 bytes previously)
                $buf .= pack('nN', $localDayIndex, $count);
            }
        }

        return $buf;
    }

    private function unpackChunk(string $data): array
    {
        $offset   = 0;
        $dayCount = unpack('N', $data, $offset)[1];
        $offset  += 4;
        $dayDates = []; // [localDayIdx => date string]
        $dayInts  = []; // [localDayIdx => YYYYMMDD int]
        for ($i = 0; $i < $dayCount; $i++) {
            $dayDates[$i] = substr($data, $offset, 10);
            $offset += 10;
            $dayInts[$i]  = unpack('N', $data, $offset)[1];
            $offset += 4;
        }

        $pathCount   = unpack('N', $data, $offset)[1];
        $offset     += 4;
        $pathStrings = []; // [localPathId => path string]
        $localCounts = []; // [localPathId][localDayIdx => count]  — integer keys throughout
        for ($i = 0; $i < $pathCount; $i++) {
            $pathLen   = unpack('n', $data, $offset)[1];
            $offset   += 2;
            $dateCount = unpack('N', $data, $offset)[1];
            $offset   += 4;
            $pathStrings[$i] = substr($data, $offset, $pathLen);
            $offset   += $pathLen;
            $counts    = [];
            for ($j = 0; $j < $dateCount; $j++) {
                $localDayIdx       = unpack('n', $data, $offset)[1]; // uint16
                $offset           += 2;
                $counts[$localDayIdx] = unpack('N', $data, $offset)[1];
                $offset           += 4;
            }
            $localCounts[$i] = $counts;
        }

        return [$pathStrings, $localCounts, $dayDates, $dayInts];
    }

    /**
     * Write pretty JSON to $outputPath.
     * Dates are pre-sorted (globalDateId 0 = earliest, N-1 = latest).
     * Uses an 8 MB write buffer to minimise fwrite syscalls.
     * All inner lookups use integer keys — no string hashing in the hot loop.
     */
    private function writeJsonOutput(
        string $outputPath,
        array  $pathOrder,
        array  $intCounts,   // [int globalPathId][int globalDateId] = count
        array  $sortedDates, // [int globalDateId => string date], pre-sorted
    ): void {
        $outputHandle = fopen($outputPath, 'wb');
        if ($outputHandle === false) {
            throw new Exception("Failed to write to output file: $outputPath");
        }

        try {
            $pathCount = count($pathOrder);

            if ($pathCount === 0) {
                fwrite($outputHandle, '{}');
                return;
            }

            $dateCount = count($sortedDates);

            // Pre-encode all date JSON keys once by integer index
            $dateJsonKeys = [];
            for ($dateId = 0; $dateId < $dateCount; $dateId++) {
                $dateJsonKeys[$dateId] = json_encode($sortedDates[$dateId]);
            }

            $flushSize = self::WRITE_BUFFER_SIZE;
            $buffer    = "{\n";
            $pathIdx   = 0;

            for ($pathId = 0; $pathId < $pathCount; $pathId++) {
                $pathIdx++;
                $pathJson   = json_encode($pathOrder[$pathId]);
                $buffer    .= "    {$pathJson}: {\n";
                $dateCounts = $intCounts[$pathId] ?? [];
                $dayCount   = count($dateCounts);
                $dayIdx     = 0;

                // Sequential integer loop — no string hashing
                for ($dateId = 0; $dateId < $dateCount; $dateId++) {
                    if (!isset($dateCounts[$dateId])) {
                        continue;
                    }
                    $dayIdx++;
                    $daySuffix = $dayIdx < $dayCount ? ',' : '';
                    $buffer   .= "        {$dateJsonKeys[$dateId]}: {$dateCounts[$dateId]}{$daySuffix}\n";
                }

                $pathSuffix = $pathIdx < $pathCount ? ',' : '';
                $buffer    .= "    }{$pathSuffix}\n";

                if (strlen($buffer) >= $flushSize) {
                    fwrite($outputHandle, $buffer);
                    $buffer = '';
                }
            }

            $buffer .= '}';
            fwrite($outputHandle, $buffer);
        } finally {
            fclose($outputHandle);
        }
    }
}
