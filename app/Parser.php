<?php

namespace App;

use App\Commands\Visit;
use const SEEK_CUR;

/**
 * High-Performance CSV Parser - 100 Million Row Challenge
 *
 * Key innovations:
 * 1. Dynamic worker count (auto-detect CPU cores: 8 on M1, 12+ on M4 Pro)
 * 2. Flat integer grid counting: grid[slugId × totalDates + dateId]++
 * 3. Pre-determined ordering from 2MB sample (zero per-line tracking overhead)
 * 4. Binary IPC - pack('V*') for grid only (no ordering files needed)
 * 5. Guard-free hot loop (all slugs/dates pre-registered, no ?? or if checks)
 * 6. Early merge - overlap grid merging with worker execution via pcntl_wait()
 */
final class Parser
{
    private const READ_CHUNK = 163840;    // 160KB read chunks
    private const SLUG_PREFIX_LEN = 25;   // strlen("https://stitcher.io/blog/")

    public function parse(string $inputPath, string $outputPath): void
    {
        \gc_disable();

        $fileSize = \filesize($inputPath);

        // Detect CPU count for optimal parallelism (8 on M1, 12+ on M4 Pro, etc.)
        $workerCount = (int)\trim(@\shell_exec('sysctl -n hw.ncpu 2>/dev/null') ?: '8');
        if ($workerCount < 4) $workerCount = 4;

        // For small files, use single-threaded approach
        if ($fileSize < 50 * 1024 * 1024) {
            $this->parseSingleThread($inputPath, $outputPath);
            return;
        }

        // ══════════════════════════════════════════════════════════════════
        // PHASE 1: Pre-register slugs + determine first-appearance ordering
        // Ordering is determined from the 2MB sample, NOT per-worker
        // (with ~190 slugs, coupon collector says ~1140 lines to see all)
        // ══════════════════════════════════════════════════════════════════
        $slugToId = [];
        $totalSlugs = 0;

        // Register all known slugs from Visit::all() for fast lookup
        foreach (Visit::all() as $v) {
            $slug = \substr($v->uri, self::SLUG_PREFIX_LEN);
            if (!isset($slugToId[$slug])) {
                $slugToId[$slug] = $totalSlugs++;
            }
        }

        // Sample first 2MB: discover new slugs AND determine first-appearance ordering
        $sampleSize = $fileSize > 2097152 ? 2097152 : $fileSize;
        $fh = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($fh, 0);
        $sample = \fread($fh, $sampleSize);
        \fclose($fh);

        $slugOrder = [];     // Slugs in first-appearance order (for JSON output)
        $slugOrderSet = [];  // Fast dedup for ordering

        $pos = 0;
        $bound = \strrpos($sample, "\n");
        while ($pos < $bound) {
            $nl = \strpos($sample, "\n", $pos + 52);
            if ($nl === false) break;
            $slug = \substr($sample, $pos + 25, $nl - $pos - 51);
            if (!isset($slugToId[$slug])) {
                $slugToId[$slug] = $totalSlugs++;
            }
            if (!isset($slugOrderSet[$slug])) {
                $slugOrderSet[$slug] = true;
                $slugOrder[] = $slug;
            }
            $pos = $nl + 1;
        }
        unset($sample, $slugOrderSet);

        // ══════════════════════════════════════════════════════════════════
        // PHASE 2: Build date → ID mapping (hardcoded 2020-2026)
        // ══════════════════════════════════════════════════════════════════
        $dateToId = [];
        $dateList = [];  // id → "YYYY-MM-DD" (for output)
        $totalDates = 0;

        for ($yr = 2020; $yr <= 2026; $yr++) {
            for ($mo = 1; $mo <= 12; $mo++) {
                // Days in month
                $dim = match ($mo) {
                    2 => ($yr % 4 === 0) ? 29 : 28,
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
        for ($i = 1; $i < $workerCount; $i++) {
            \fseek($bh, (int)($fileSize * $i / $workerCount));
            \fgets($bh);
            $edges[] = \ftell($bh);
        }
        \fclose($bh);
        $edges[] = $fileSize;

        // ══════════════════════════════════════════════════════════════════
        // PHASE 4: Fork workers - each returns ONLY grid (no ordering)
        // Ordering was already determined from the 2MB sample above
        // ══════════════════════════════════════════════════════════════════
        $gridSize = $totalSlugs * $totalDates;

        $tmpGridFiles = [];
        $ppid = \getmypid();
        for ($i = 0; $i < $workerCount; $i++) {
            $tmpGridFiles[$i] = \sys_get_temp_dir() . "/parse_grid_{$i}_{$ppid}";
        }

        $pidToWorker = [];
        for ($w = 0; $w < $workerCount; $w++) {
            $pid = \pcntl_fork();

            if ($pid === -1) {
                throw new \RuntimeException('Fork failed');
            }

            if ($pid === 0) {
                // ══════════════════════════════════════════════════════════
                // CHILD: Process chunk - counting only, no ordering
                // ══════════════════════════════════════════════════════════
                $grid = $this->processChunk(
                    $inputPath,
                    $edges[$w],
                    $edges[$w + 1],
                    $slugToId,
                    $dateToId,
                    $totalSlugs,
                    $totalDates
                );

                // Write grid only (binary packed)
                \file_put_contents($tmpGridFiles[$w], \pack('V*', ...$grid));
                exit(0);
            }

            $pidToWorker[$pid] = $w;
        }

        // ══════════════════════════════════════════════════════════════════
        // PHASE 5: Early merge - overlap grid merging with worker execution
        // Uses pcntl_wait() to merge each worker's grid as soon as it finishes
        // ══════════════════════════════════════════════════════════════════
        $finalGrid = \array_fill(0, $gridSize, 0);

        for ($done = 0; $done < $workerCount; $done++) {
            $pid = \pcntl_wait($st);
            $w = $pidToWorker[$pid];
            $blob = \file_get_contents($tmpGridFiles[$w]);
            \unlink($tmpGridFiles[$w]);
            $vals = \unpack('V*', $blob);
            $i = 0;
            foreach ($vals as $v) {
                $finalGrid[$i++] += $v;
            }
        }

        // Fallback: append any slugs with data that weren't in the 2MB sample
        $slugOrderSet = \array_flip($slugOrder);
        $idToSlug = \array_flip($slugToId);
        for ($s = 0; $s < $totalSlugs; $s++) {
            $slug = $idToSlug[$s];
            if (!isset($slugOrderSet[$slug])) {
                // Check if this slug has any data
                $base = $s * $totalDates;
                for ($d = 0; $d < $totalDates; $d++) {
                    if ($finalGrid[$base + $d] > 0) {
                        $slugOrder[] = $slug;
                        break;
                    }
                }
            }
        }

        // ══════════════════════════════════════════════════════════════════
        // PHASE 6: Write JSON output using pre-determined ordering
        // ══════════════════════════════════════════════════════════════════
        $this->writeJsonFromGridWithIds($outputPath, $finalGrid, $slugOrder, $slugToId, $dateList, $totalDates);
    }

    /**
     * Lean chunk processor: count visits only, no ordering tracking.
     * Guard-free hot loop - all slugs/dates are pre-registered.
     */
    private function processChunk(
        string $path,
        int $from,
        int $to,
        array $slugToId,
        array $dateToId,
        int $totalSlugs,
        int $totalDates
    ): array {
        $grid = \array_fill(0, $totalSlugs * $totalDates, 0);
        $td = $totalDates;  // Local var for hot loop

        $fh = \fopen($path, 'rb');
        \stream_set_read_buffer($fh, 0);
        \fseek($fh, $from);

        $remaining = $to - $from;

        while ($remaining > 0) {
            $toRead = $remaining > self::READ_CHUNK ? self::READ_CHUNK : $remaining;
            $chunk = \fread($fh, $toRead);
            $chunkLen = \strlen($chunk);
            if ($chunkLen === 0) break;
            $remaining -= $chunkLen;

            $lastNl = \strrpos($chunk, "\n");
            if ($lastNl === false) continue;

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                \fseek($fh, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            // Guard-free 6× unrolled hot loop
            // No ?? guards: all slugs from Visit::all(), all dates in 2020-2026
            // No ordering: determined from 2MB sample in parent
            $p = 0;
            $fence = $lastNl - 720;
            while ($p < $fence) {
                $nl = \strpos($chunk, "\n", $p + 52);
                $grid[$slugToId[\substr($chunk, $p + 25, $nl - $p - 51)] * $td + $dateToId[\substr($chunk, $nl - 25, 10)]]++;
                $p = $nl + 1;

                $nl = \strpos($chunk, "\n", $p + 52);
                $grid[$slugToId[\substr($chunk, $p + 25, $nl - $p - 51)] * $td + $dateToId[\substr($chunk, $nl - 25, 10)]]++;
                $p = $nl + 1;

                $nl = \strpos($chunk, "\n", $p + 52);
                $grid[$slugToId[\substr($chunk, $p + 25, $nl - $p - 51)] * $td + $dateToId[\substr($chunk, $nl - 25, 10)]]++;
                $p = $nl + 1;

                $nl = \strpos($chunk, "\n", $p + 52);
                $grid[$slugToId[\substr($chunk, $p + 25, $nl - $p - 51)] * $td + $dateToId[\substr($chunk, $nl - 25, 10)]]++;
                $p = $nl + 1;

                $nl = \strpos($chunk, "\n", $p + 52);
                $grid[$slugToId[\substr($chunk, $p + 25, $nl - $p - 51)] * $td + $dateToId[\substr($chunk, $nl - 25, 10)]]++;
                $p = $nl + 1;

                $nl = \strpos($chunk, "\n", $p + 52);
                $grid[$slugToId[\substr($chunk, $p + 25, $nl - $p - 51)] * $td + $dateToId[\substr($chunk, $nl - 25, 10)]]++;
                $p = $nl + 1;
            }

            // Handle remaining lines (keep false check as safety net)
            while ($p < $lastNl) {
                $nl = \strpos($chunk, "\n", $p + 52);
                if ($nl === false) break;
                $grid[$slugToId[\substr($chunk, $p + 25, $nl - $p - 51)] * $td + $dateToId[\substr($chunk, $nl - 25, 10)]]++;
                $p = $nl + 1;
            }
        }

        \fclose($fh);
        return $grid;
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