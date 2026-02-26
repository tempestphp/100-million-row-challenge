<?php

namespace App;

use App\Commands\Visit;

/**
 * High-Performance CSV Parser - 100 Million Row Challenge
 *
 * Key innovations:
 * 1. 16 parallel workers (2× cores for I/O overlap)
 * 2. Flat integer grid counting: grid[slugId × totalDates + dateId]++
 * 3. Parallel ordering discovery - workers track first-appearance, parent merges
 * 4. Binary IPC - pack('V*') for grid, pack('v*') for ordering
 * 5. Pre-computed slug/date ID mappings
 */
final class Parser
{
    private const WORKER_COUNT = 16;      // Sweet spot for M1 (8 cores × 2)
    private const READ_BUFFER = 8388608;  // 8MB read chunks
    private const SLUG_PREFIX_LEN = 25;   // strlen("https://stitcher.io/blog/")

    public function parse(string $inputPath, string $outputPath): void
    {
        \gc_disable();

        $fileSize = \filesize($inputPath);

        // For small files, use single-threaded approach
        if ($fileSize < 50 * 1024 * 1024) {
            $this->parseSingleThread($inputPath, $outputPath);
            return;
        }

        // ══════════════════════════════════════════════════════════════════
        // PHASE 1: Pre-register all known slugs (for fast ID lookup)
        // Actual ordering will be determined per-worker and merged later
        // ══════════════════════════════════════════════════════════════════
        $slugToId = [];
        $totalSlugs = 0;

        // Register all known slugs from Visit::all() for fast lookup
        foreach (Visit::all() as $v) {
            $slug = \substr($v->uri, self::SLUG_PREFIX_LEN);
            if (!isset($slugToId[$slug])) {
                $slugToId[$slug] = $totalSlugs;
                $totalSlugs++;
            }
        }

        // Sample first 2MB to discover slugs AND detect year range
        $sampleSize = \min(2097152, $fileSize);
        $fh = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($fh, 0);
        $sample = \fread($fh, $sampleSize);
        \fclose($fh);

        $minYear = 9999;
        $maxYear = 0;
        $pos = 0;
        $bound = \strrpos($sample, "\n") ?: 0;
        while ($pos < $bound) {
            $comma = \strpos($sample, ',', $pos + 26);
            if ($comma === false) break;
            $slug = \substr($sample, $pos + 25, $comma - $pos - 25);
            if (!isset($slugToId[$slug])) {
                $slugToId[$slug] = $totalSlugs;
                $totalSlugs++;
            }
            // Extract year from date (format: YYYY-MM-DDTHH:MM:SS+00:00)
            $year = (int)\substr($sample, $comma + 1, 4);
            if ($year < $minYear) $minYear = $year;
            if ($year > $maxYear) $maxYear = $year;

            $nl = \strpos($sample, "\n", $comma);
            if ($nl === false) break;
            $pos = $nl + 1;
        }
        unset($sample);

        // Add 1-year buffer on each side for safety
        $minYear = \max(1900, $minYear - 1);
        $maxYear = \min(2100, $maxYear + 1);

        // ══════════════════════════════════════════════════════════════════
        // PHASE 2: Build date → ID mapping (dynamic range from data)
        // ══════════════════════════════════════════════════════════════════
        $dateToId = [];
        $dateList = [];  // id → "YYYY-MM-DD" (for output)
        $totalDates = 0;

        for ($yr = $minYear; $yr <= $maxYear; $yr++) {
            for ($mo = 1; $mo <= 12; $mo++) {
                // Days in month
                $dim = match ($mo) {
                    2 => (($yr % 4 === 0) ? 29 : 28),
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $moStr = $mo < 10 ? "0{$mo}" : (string)$mo;
                for ($dy = 1; $dy <= $dim; $dy++) {
                    $dyStr = $dy < 10 ? "0{$dy}" : (string)$dy;
                    $dateKey = "{$yr}-{$moStr}-{$dyStr}";
                    $dateToId[$dateKey] = $totalDates;
                    $dateList[$totalDates] = $dateKey;
                    $totalDates++;
                }
            }
        }

        // ══════════════════════════════════════════════════════════════════
        // PHASE 3: Calculate line-aligned chunk boundaries
        // ══════════════════════════════════════════════════════════════════
        $edges = [0];
        $bh = \fopen($inputPath, 'rb');
        for ($i = 1; $i < self::WORKER_COUNT; $i++) {
            \fseek($bh, (int)($fileSize * $i / self::WORKER_COUNT));
            \fgets($bh);
            $edges[] = \ftell($bh);
        }
        \fclose($bh);
        $edges[] = $fileSize;

        // ══════════════════════════════════════════════════════════════════
        // PHASE 4: Fork workers - each returns grid + ordering
        // Workers track which slugs they see in first-appearance order
        // ══════════════════════════════════════════════════════════════════
        $gridSize = $totalSlugs * $totalDates;

        $tmpGridFiles = [];
        $tmpOrderFiles = [];
        for ($i = 0; $i < self::WORKER_COUNT; $i++) {
            $tmpGridFiles[$i] = \sys_get_temp_dir() . "/parse_grid_{$i}_" . \getmypid();
            $tmpOrderFiles[$i] = \sys_get_temp_dir() . "/parse_order_{$i}_" . \getmypid();
        }

        $kids = [];
        for ($w = 0; $w < self::WORKER_COUNT; $w++) {
            $pid = \pcntl_fork();

            if ($pid === -1) {
                throw new \RuntimeException('Fork failed');
            }

            if ($pid === 0) {
                // ══════════════════════════════════════════════════════════
                // CHILD: Process chunk, track counts AND first-appearance order
                // ══════════════════════════════════════════════════════════
                [$grid, $orderedIds] = $this->processChunkWithOrdering(
                    $inputPath,
                    $edges[$w],
                    $edges[$w + 1],
                    $slugToId,
                    $dateToId,
                    $totalSlugs,
                    $totalDates
                );

                // Write grid (binary packed) and ordering (packed short ints)
                \file_put_contents($tmpGridFiles[$w], \pack('V*', ...$grid));
                \file_put_contents($tmpOrderFiles[$w], \pack('v*', ...$orderedIds));
                exit(0);
            }

            $kids[] = $pid;
        }

        // Wait for all children
        foreach ($kids as $pid) {
            \pcntl_waitpid($pid, $st);
        }

        // ══════════════════════════════════════════════════════════════════
        // PHASE 5: Merge grids AND build global ordering
        // ══════════════════════════════════════════════════════════════════
        $finalGrid = \array_fill(0, $gridSize, 0);
        $slugList = [];  // Global ordering: slug string by output position
        $seen = [];      // Track which slug IDs we've added to global order

        for ($w = 0; $w < self::WORKER_COUNT; $w++) {
            // Merge grid
            $blob = \file_get_contents($tmpGridFiles[$w]);
            \unlink($tmpGridFiles[$w]);
            $vals = \unpack('V*', $blob);
            $i = 0;
            foreach ($vals as $v) {
                $finalGrid[$i++] += $v;
            }

            // Merge ordering: add this worker's slugs to global order (preserving first-appearance)
            $orderBlob = \file_get_contents($tmpOrderFiles[$w]);
            \unlink($tmpOrderFiles[$w]);
            $workerOrder = \unpack('v*', $orderBlob);
            foreach ($workerOrder as $slugId) {
                if (!isset($seen[$slugId])) {
                    $seen[$slugId] = true;
                    $slugList[] = $slugId;  // Will convert to slug string later
                }
            }
        }

        // Convert slug IDs to slug strings using inverse lookup
        $idToSlug = \array_flip($slugToId);
        $slugListStr = [];
        foreach ($slugList as $slugId) {
            $slugListStr[] = $idToSlug[$slugId];
        }

        // ══════════════════════════════════════════════════════════════════
        // PHASE 6: Write JSON output
        // ══════════════════════════════════════════════════════════════════
        $this->writeJsonFromGridWithIds($outputPath, $finalGrid, $slugListStr, $slugToId, $dateList, $totalDates);
    }

    /**
     * Process chunk: count visits AND track first-appearance ordering
     * Returns: [grid, orderedSlugIds]
     */
    private function processChunkWithOrdering(
        string $path,
        int $from,
        int $to,
        array $slugToId,
        array $dateToId,
        int $totalSlugs,
        int $totalDates
    ): array {
        $grid = \array_fill(0, $totalSlugs * $totalDates, 0);
        $orderedIds = [];
        $seen = [];
        $td = $totalDates;  // Local var for hot loop

        $fh = \fopen($path, 'rb');
        \stream_set_read_buffer($fh, 0);
        \fseek($fh, $from);

        $remaining = $to - $from;
        $leftover = '';

        while ($remaining > 0) {
            $toRead = \min(self::READ_BUFFER, $remaining);
            $chunk = $leftover . \fread($fh, $toRead);
            $remaining -= $toRead;

            $len = \strlen($chunk);
            $lastNl = \strrpos($chunk, "\n");
            if ($lastNl === false) {
                $leftover = $chunk;
                continue;
            }

            $leftover = ($lastNl < $len - 1) ? \substr($chunk, $lastNl + 1) : '';

            // Simple tight loop with safe bounds
            $p = 0;
            while ($p + 52 < $len) {
                $nl = \strpos($chunk, "\n", $p + 52);
                if ($nl === false || $nl > $lastNl) break;
                $slug = \substr($chunk, $p + 25, $nl - $p - 51);
                if (isset($slugToId[$slug])) {
                    $sid = $slugToId[$slug];
                    $date = \substr($chunk, $nl - 25, 10);
                    if (isset($dateToId[$date])) $grid[$sid * $td + $dateToId[$date]]++;
                    if (!isset($seen[$sid])) { $seen[$sid] = 1; $orderedIds[] = $sid; }
                }
                $p = $nl + 1;
            }
        }

        // Process leftover
        if ($leftover !== '' && \strlen($leftover) > 51) {
            $ll = \strlen($leftover);
            $slug = \substr($leftover, 25, $ll - 51);
            $date = \substr($leftover, $ll - 25, 10);
            if (isset($slugToId[$slug], $dateToId[$date])) {
                $sid = $slugToId[$slug];
                $grid[$sid * $td + $dateToId[$date]]++;
                if (!isset($seen[$sid])) $orderedIds[] = $sid;
            }
        }

        \fclose($fh);
        return [$grid, $orderedIds];
    }

    private function processChunkSingleThread(string $chunk, array &$result): void
    {
        // Process line by line using position tracking with global namespace functions
        $pos = 0;
        $len = \strlen($chunk);

        while ($pos < $len) {
            // Find newline
            $newlinePos = \strpos($chunk, "\n", $pos);
            if ($newlinePos === false) $newlinePos = $len;

            // Skip empty lines
            if ($newlinePos === $pos) {
                $pos = $newlinePos + 1;
                continue;
            }

            // Find comma position
            $commaPos = \strpos($chunk, ',', $pos);
            if ($commaPos === false || $commaPos > $newlinePos) {
                $pos = $newlinePos + 1;
                continue;
            }

            // Find path start (third slash after https://)
            $slashPos = \strpos($chunk, '/', $pos + 8);
            if ($slashPos === false || $slashPos >= $commaPos) {
                $pos = $newlinePos + 1;
                continue;
            }

            // Extract path and date using substr
            $url = \substr($chunk, $slashPos, $commaPos - $slashPos);
            $date = \substr($chunk, $commaPos + 1, 10);

            // Increment counter - check isset first for speed
            if (isset($result[$url][$date])) {
                $result[$url][$date]++;
            } else {
                $result[$url][$date] = 1;
            }

            $pos = $newlinePos + 1;
        }
    }

    private function processLine(string $line, array &$result): void
    {
        $commaPos = strpos($line, ',');
        if ($commaPos === false) return;

        $slashPos = strpos($line, '/', 8);
        if ($slashPos === false || $slashPos >= $commaPos) return;

        $url = substr($line, $slashPos, $commaPos - $slashPos);
        $date = substr($line, $commaPos + 1, 10);

        if (isset($result[$url][$date])) {
            $result[$url][$date]++;
        } else {
            $result[$url][$date] = 1;
        }
    }

    private function parseSingleThread(string $inputPath, string $outputPath): void
    {
        $result = [];
        $handle = fopen($inputPath, 'rb');
        $bufferSize = 4 * 1024 * 1024;
        $leftover = '';

        while (!feof($handle)) {
            $chunk = $leftover . fread($handle, $bufferSize);
            $lastNewline = strrpos($chunk, "\n");

            if ($lastNewline === false) {
                $leftover = $chunk;
                continue;
            }

            $leftover = substr($chunk, $lastNewline + 1);
            $this->processChunkSingleThread(substr($chunk, 0, $lastNewline), $result);
        }

        if ($leftover !== '') {
            $this->processLine($leftover, $result);
        }

        fclose($handle);
        $this->writeJson($result, $outputPath);
    }

    private function writeJson(array $result, string $outputPath): void
    {
        $out = \fopen($outputPath, 'wb');
        $buffer = "{\n";
        $firstUrl = true;

        foreach ($result as $url => $dates) {
            if (!$firstUrl) $buffer .= ",\n";
            $firstUrl = false;

            $escapedUrl = \str_replace('/', '\\/', $url);
            $buffer .= '    "' . $escapedUrl . '": {';

            \ksort($dates);

            $firstDate = true;
            foreach ($dates as $date => $count) {
                if (!$firstDate) $buffer .= ',';
                $firstDate = false;
                $buffer .= "\n        \"$date\": $count";
            }

            $buffer .= "\n    }";

            // Flush every 4MB
            if (\strlen($buffer) > 4 * 1024 * 1024) {
                \fwrite($out, $buffer);
                $buffer = '';
            }
        }

        $buffer .= "\n}";
        \fwrite($out, $buffer);
        \fclose($out);
    }

    /**
     * Write JSON from grid with slugs in first-appearance order
     */
    private function writeJsonFromGridWithIds(
        string $outputPath,
        array $grid,
        array $slugListStr,  // Slugs in first-appearance order
        array $slugToId,     // slug → ID for grid lookup
        array $dateList,
        int $totalDates
    ): void {
        $out = \fopen($outputPath, 'wb');
        $buffer = "{\n";
        $firstUrl = true;

        foreach ($slugListStr as $slug) {
            $slugId = $slugToId[$slug];
            $base = $slugId * $totalDates;

            // Check if this slug has any counts
            $hasData = false;
            for ($d = 0; $d < $totalDates; $d++) {
                if ($grid[$base + $d] > 0) {
                    $hasData = true;
                    break;
                }
            }

            if (!$hasData) continue;

            if (!$firstUrl) $buffer .= ",\n";
            $firstUrl = false;

            // Escape the slug for JSON (slashes need escaping)
            $escapedSlug = \str_replace('/', '\\/', $slug);
            $buffer .= '    "\\/blog\\/' . $escapedSlug . '": {';

            $firstDate = true;
            for ($d = 0; $d < $totalDates; $d++) {
                $cnt = $grid[$base + $d];
                if ($cnt === 0) continue;

                if (!$firstDate) $buffer .= ',';
                $firstDate = false;
                $buffer .= "\n        \"{$dateList[$d]}\": {$cnt}";
            }

            $buffer .= "\n    }";

            // Flush every 4MB
            if (\strlen($buffer) > 4194304) {
                \fwrite($out, $buffer);
                $buffer = '';
            }
        }

        $buffer .= "\n}";
        \fwrite($out, $buffer);
        \fclose($out);
    }
}