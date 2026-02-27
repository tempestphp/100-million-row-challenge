<?php

declare(strict_types=1);

namespace App;

use RuntimeException;

final class Parser
{
    private const int BUFFER_SIZE = 2_097_152;
    private const int WORKER_COUNT = 4; // tuned for 2-vCPU benchmark server

    private const int URL_PREFIX_LENGTH = 19; // "https://stitcher.io"
    private const int DATETIME_LENGTH = 25; // "YYYY-MM-DDTHH:MM:SS+00:00"
    private const int MINIMUM_LINE_LENGTH = self::URL_PREFIX_LENGTH + 1 + self::DATETIME_LENGTH;

    private const array MONTH_DAYS = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]; // index 0 unused

    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = \filesize($inputPath);

        // Pre-generate all possible dates (2020-2026) as a flat integer lookup
        [$dateIndex, $dateLabels, $dateCount] = $this->prepareDateLookup();

        // Account for all unique URL paths before parallel
        // counting to keep worker schemas identical
        [$pathOffsets, $pathLabels, $pathCount] = $this->identifyPaths(
            $inputPath, $fileSize, $dateCount,
        );

        // Split file into WORKER_COUNT ranges aligned to newline boundaries
        $segmentSize = \intdiv($fileSize, self::WORKER_COUNT);
        $boundaries = [0];
        $handle = $this->openFile($inputPath, 'rb');

        for ($i = 1; $i < self::WORKER_COUNT; $i++) {
            \fseek($handle, $i * $segmentSize);
            $line = \fgets($handle);

            if ($line === false) {
                $boundaries[] = $fileSize;
                continue;
            }

            $boundaries[] = \ftell($handle);
        }

        \fclose($handle);
        $boundaries[] = $fileSize;

        // Fork child workers for the first N-1 chunks; each
        // writes packed uint32 counts to /dev/shm
        $tmpDir = \is_dir('/dev/shm') ? '/dev/shm' : \sys_get_temp_dir();
        $tag = \getmypid();
        $childPids = [];
        $childFiles = [];

        for ($w = 0; $w < self::WORKER_COUNT - 1; $w++) {
            $file = "{$tmpDir}/chunk_{$tag}_{$w}";
            $childFiles[] = $file;

            $pid = \pcntl_fork();

            if ($pid === -1) {
                throw new RuntimeException('Failed to fork parser worker process');
            }

            if ($pid === 0) {
                $written = \file_put_contents($file, \pack('V*', ...$this->tally(
                    $inputPath, $boundaries[$w], $boundaries[$w + 1],
                    $pathOffsets, $dateIndex, $pathCount, $dateCount,
                )));

                if ($written === false) {
                    exit(1);
                }

                exit(0);
            }

            $childPids[] = $pid;
        }

        // Parent processes the last chunk while children run in parallel
        $totals = $this->tally(
            $inputPath, $boundaries[self::WORKER_COUNT - 1], $boundaries[self::WORKER_COUNT],
            $pathOffsets, $dateIndex, $pathCount, $dateCount,
        );

        foreach ($childPids as $pid) {
            \pcntl_waitpid($pid, $status);

            if (\pcntl_wifexited($status) && \pcntl_wexitstatus($status) === 0) {
                continue;
            }

            throw new RuntimeException("Parser worker {$pid} failed");
        }

        // Merge child results by adding their flat count arrays element-wise
        foreach ($childFiles as $file) {
            $packed = \file_get_contents($file);

            if ($packed === false) {
                throw new RuntimeException("Failed to read worker chunk file: {$file}");
            }

            \unlink($file);
            $idx = 0;

            $values = \unpack('V*', $packed);

            if ($values === false) {
                throw new RuntimeException("Failed to unpack worker chunk file: {$file}");
            }

            foreach ($values as $val) {
                $totals[$idx++] += $val;
            }
        }

        $this->serializeJson($outputPath, $pathLabels, $dateLabels, $totals, $dateCount);
    }

    /**
     * Generates a sequential integer ID for every calendar date from 2020-01-01
     * through 2026-12-31. Produced in chronological order so the output phase
     * can iterate by ID without sorting.
     */
    private function prepareDateLookup(): array
    {
        $index = [];
        $labels = [];
        $count = 0;

        for ($y = 2020; $y <= 2026; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $days = self::MONTH_DAYS[$m];

                if ($m === 2 && $y % 4 === 0) {
                    $days = 29;
                }

                for ($d = 1; $d <= $days; $d++) {
                    $label = $y . '-' . ($m < 10 ? '0' : '') . $m . '-' . ($d < 10 ? '0' : '') . $d;
                    $index[$label] = $count;
                    $labels[$count++] = $label;
                }
            }
        }

        return [$index, $labels, $count];
    }

    private function identifyPaths(
        string $inputPath,
        int $fileSize,
        int $dateCount,
    ): array {
        $handle = $this->openFile($inputPath, 'rb');
        \stream_set_read_buffer($handle, 0);
        $offsets = [];
        $labels = [];
        $count = 0;
        $remaining = $fileSize;
        $carry = '';

        while ($remaining > 0) {
            $chunk = \fread($handle, $remaining > self::BUFFER_SIZE ? self::BUFFER_SIZE : $remaining);

            if ($chunk === false) {
                throw new RuntimeException("Failed to read input file during path discovery: {$inputPath}");
            }

            $chunkLength = \strlen($chunk);

            if ($chunkLength === 0) {
                break;
            }

            $remaining -= $chunkLength;
            $buffer = $carry . $chunk;
            $lastNl = \strrpos($buffer, "\n");

            if ($lastNl === false) {
                $carry = $buffer;
                continue;
            }

            $end = $lastNl + 1;
            $pos = 0;

            while ($pos < $end) {
                $nl = \strpos($buffer, "\n", $pos);

                if ($nl === false) {
                    break;
                }

                $lineLength = $nl - $pos;

                if ($lineLength < self::MINIMUM_LINE_LENGTH) {
                    $pos = $nl + 1;
                    continue;
                }

                $pathLength = $lineLength - (self::URL_PREFIX_LENGTH + 1 + self::DATETIME_LENGTH);
                $path = \substr($buffer, $pos + self::URL_PREFIX_LENGTH, $pathLength);

                if (!isset($offsets[$path])) {
                    $offsets[$path] = $count * $dateCount;
                    $labels[$count++] = $path;
                }

                $pos = $nl + 1;
            }

            $carry = $end < \strlen($buffer) ? \substr($buffer, $end) : '';
        }

        \fclose($handle);

        return [$offsets, $labels, $count];
    }

    /**
     * Counts visits in the byte range [$start, $end) using a dense flat array
     * indexed by (pathOffset + dateId). Avoids nested hash tables entirely.
     */
    private function tally(
        string $inputPath,
        int $start,
        int $end,
        array $pathOffsets,
        array $dateIndex,
        int $pathCount,
        int $dateCount,
    ): array {
        $counts = \array_fill(0, $pathCount * $dateCount, 0);

        $handle = $this->openFile($inputPath, 'rb');
        \stream_set_read_buffer($handle, 0);
        \fseek($handle, $start);

        $remaining = $end - $start;

        while ($remaining > 0) {
            $buffer = \fread($handle, $remaining > self::BUFFER_SIZE ? self::BUFFER_SIZE : $remaining);
            $bufLen = \strlen($buffer);
            $remaining -= $bufLen;
            $lastNl = \strrpos($buffer, "\n");

            if ($lastNl === false) {
                break;
            }

            // Seek back past any partial trailing line so the next read picks it up
            if ($lastNl < $bufLen - 1) {
                $excess = $bufLen - $lastNl - 1;
                \fseek($handle, -$excess, \SEEK_CUR);
                $remaining += $excess;
            }

            $pos = 0;

            while ($pos < $lastNl) {
                $nl = \strpos($buffer, "\n", $pos);

                if ($nl === false) {
                    break;
                }

                $lineLength = $nl - $pos;

                if ($lineLength < self::MINIMUM_LINE_LENGTH) {
                    $pos = $nl + 1;
                    continue;
                }

                $pathLength = $lineLength - (self::URL_PREFIX_LENGTH + 1 + self::DATETIME_LENGTH);
                $path = \substr($buffer, $pos + self::URL_PREFIX_LENGTH, $pathLength);
                $offset = $pathOffsets[$path] ?? null;

                if ($offset === null) {
                    $pos = $nl + 1;
                    continue;
                }

                // Extract YYYY-MM-DD (10 chars from the datetime suffix)
                $date = \substr($buffer, $nl - self::DATETIME_LENGTH, 10);
                $dateId = $dateIndex[$date] ?? null;

                if ($dateId === null) {
                    $pos = $nl + 1;
                    continue;
                }

                $counts[$offset + $dateId]++;
                $pos = $nl + 1;
            }
        }

        \fclose($handle);

        return $counts;
    }

    /**
     * Writes the pretty-printed JSON by iterating dates sequentially (already
     * chronological from prepareDateLookup), skipping zero-count entries.
     */
    private function serializeJson(
        string $outputPath,
        array $pathLabels,
        array $dateLabels,
        array $counts,
        int $dateCount,
    ): void {
        $handle = $this->openFile($outputPath, 'wb');
        \stream_set_write_buffer($handle, 1_048_576);
        $separator = '{';

        foreach ($pathLabels as $pathId => $path) {
            $encodedPath = \json_encode($path, \JSON_THROW_ON_ERROR);
            $chunk = "{$separator}\n    {$encodedPath}: {";

            $base = $pathId * $dateCount;
            $glue = "\n";

            for ($d = 0; $d < $dateCount; $d++) {
                $visits = $counts[$base + $d];

                if ($visits === 0) {
                    continue;
                }

                $chunk .= "{$glue}        \"{$dateLabels[$d]}\": {$visits}";
                $glue = ",\n";
            }

            $chunk .= "\n    }";
            \fwrite($handle, $chunk);
            $separator = ',';
        }

        \fwrite($handle, "\n}");
        \fclose($handle);
    }

    private function openFile(string $path, string $mode)
    {
        $handle = \fopen($path, $mode);

        if ($handle !== false) {
            return $handle;
        }

        throw new RuntimeException("Unable to open file: {$path}");
    }
}
