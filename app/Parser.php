<?php

namespace App;

use Exception;

final class Parser
{
    // Every generated URL currently starts with this fixed host, so we can slice path fast.
    private const URL_PREFIX_LENGTH = 19;

    public function parse(string $inputPath, string $outputPath): void
    {
        $visits = $this->parseData($inputPath);

        $json = json_encode($visits, JSON_PRETTY_PRINT);

        if ($json === false) {
            throw new Exception('Failed to encode JSON output.');
        }

        if (file_put_contents($outputPath, $json) === false) {
            throw new Exception("Unable to write output file: {$outputPath}");
        }
    }

    private function parseData(string $inputPath): array
    {
        // Prefer sequential mode for unsupported runtimes or small files where fork overhead dominates.
        if (! function_exists('pcntl_fork')) {
            return $this->parseSequential($inputPath);
        }

        $fileSize = filesize($inputPath);

        if ($fileSize === false || $fileSize < 16 * 1024 * 1024) {
            return $this->parseSequential($inputPath);
        }

        $workerCount = $this->detectWorkerCount();

        if ($workerCount < 2) {
            return $this->parseSequential($inputPath);
        }

        return $this->parseParallel($inputPath, $fileSize, $workerCount);
    }

    private function parseSequential(string $inputPath): array
    {
        $input = fopen($inputPath, 'rb');

        if ($input === false) {
            throw new Exception("Unable to open input file: {$inputPath}");
        }

        $counts = [];

        while (($line = fgets($input)) !== false) {
            $commaPosition = strpos($line, ',');

            if ($commaPosition === false) {
                continue;
            }

            $path = $this->extractPath($line, $commaPosition);
            $date = substr($line, $commaPosition + 1, 10);
            $counts[$path][$date] = ($counts[$path][$date] ?? 0) + 1;
        }

        fclose($input);

        foreach ($counts as &$dates) {
            ksort($dates);
        }
        unset($dates);

        return $counts;
    }

    private function parseParallel(string $inputPath, int $fileSize, int $workerCount): array
    {
        // Split by byte ranges; each worker aligns itself to full CSV lines.
        $chunkSize = intdiv($fileSize, $workerCount);
        $workerPids = [];
        $workerFiles = [];

        for ($workerIndex = 0; $workerIndex < $workerCount; $workerIndex++) {
            $startOffset = $workerIndex * $chunkSize;
            $endOffset = $workerIndex === $workerCount - 1
                ? $fileSize
                : ($workerIndex + 1) * $chunkSize;

            $tempPath = tempnam(sys_get_temp_dir(), 'parser-');

            if ($tempPath === false) {
                throw new Exception('Failed to create temporary file for parser worker.');
            }

            $pid = pcntl_fork();

            if ($pid === -1) {
                throw new Exception('Failed to fork parser worker process.');
            }

            if ($pid === 0) {
                try {
                    // Workers write serialized partial aggregates to temp files for parent merge.
                    $payload = $this->parseRange($inputPath, $startOffset, $endOffset, $workerIndex === $workerCount - 1);
                    file_put_contents($tempPath, serialize($payload));
                    exit(0);
                } catch (\Throwable) {
                    exit(1);
                }
            }

            $workerPids[] = $pid;
            $workerFiles[] = $tempPath;
        }

        foreach ($workerPids as $pid) {
            pcntl_waitpid($pid, $status);

            if (pcntl_wexitstatus($status) !== 0) {
                foreach ($workerFiles as $workerFile) {
                    @unlink($workerFile);
                }

                throw new Exception('A parser worker failed.');
            }
        }

        [$counts, $firstSeenOffsets] = $this->mergeWorkerResults($workerFiles);

        return $this->finalizeCounts($counts, $firstSeenOffsets);
    }

    private function parseRange(string $inputPath, int $startOffset, int $endOffset, bool $isLastChunk): array
    {
        $input = fopen($inputPath, 'rb');

        if ($input === false) {
            throw new Exception("Unable to open input file: {$inputPath}");
        }

        if ($startOffset > 0) {
            // Move to the next full line to avoid splitting one CSV row between two workers.
            fseek($input, $startOffset - 1);
            fgets($input);
        } else {
            fseek($input, 0);
        }

        $counts = [];
        $firstSeenOffsets = [];

        while (! feof($input)) {
            $lineOffset = ftell($input);

            if (! $isLastChunk && $lineOffset >= $endOffset) {
                break;
            }

            $line = fgets($input);

            if ($line === false) {
                break;
            }

            $commaPosition = strpos($line, ',');

            if ($commaPosition === false) {
                continue;
            }

            $path = $this->extractPath($line, $commaPosition);
            $date = substr($line, $commaPosition + 1, 10);

            $counts[$path][$date] = ($counts[$path][$date] ?? 0) + 1;

            if (! isset($firstSeenOffsets[$path])) {
                // Keep first byte offset per path so global output key order matches input order.
                $firstSeenOffsets[$path] = $lineOffset;
            }
        }

        fclose($input);

        return ['counts' => $counts, 'firstSeenOffsets' => $firstSeenOffsets];
    }

    private function mergeWorkerResults(array $workerFiles): array
    {
        $mergedCounts = [];
        $firstSeenOffsets = [];

        foreach ($workerFiles as $workerFile) {
            $content = file_get_contents($workerFile);
            @unlink($workerFile);

            if ($content === false || $content === '') {
                continue;
            }

            $payload = unserialize($content);

            if (! is_array($payload)) {
                continue;
            }

            foreach ($payload['counts'] ?? [] as $path => $dates) {
                foreach ($dates as $date => $count) {
                    $mergedCounts[$path][$date] = ($mergedCounts[$path][$date] ?? 0) + $count;
                }
            }

            foreach ($payload['firstSeenOffsets'] ?? [] as $path => $offset) {
                if (! isset($firstSeenOffsets[$path]) || $offset < $firstSeenOffsets[$path]) {
                    $firstSeenOffsets[$path] = $offset;
                }
            }
        }

        return [$mergedCounts, $firstSeenOffsets];
    }

    private function finalizeCounts(array $counts, array $firstSeenOffsets): array
    {
        // Rule: visits per path must be sorted by date ascending.
        foreach ($counts as &$dates) {
            ksort($dates);
        }
        unset($dates);

        // Rule: preserve path insertion order from original stream.
        asort($firstSeenOffsets, SORT_NUMERIC);

        $orderedCounts = [];

        foreach ($firstSeenOffsets as $path => $_) {
            if (isset($counts[$path])) {
                $orderedCounts[$path] = $counts[$path];
                unset($counts[$path]);
            }
        }

        foreach ($counts as $path => $dates) {
            $orderedCounts[$path] = $dates;
        }

        return $orderedCounts;
    }

    private function extractPath(string $line, int $commaPosition): string
    {
        return substr($line, self::URL_PREFIX_LENGTH, $commaPosition - self::URL_PREFIX_LENGTH);
    }

    private function detectWorkerCount(): int
    {
        // Optional manual override for benchmarking/tuning.
        $override = getenv('PARSER_WORKERS');

        if (is_string($override) && $override !== '') {
            $forcedWorkers = (int) $override;

            if ($forcedWorkers > 0) {
                return min(16, $forcedWorkers);
            }
        }

        $cpuCount = 0;

        $getconf = @shell_exec('getconf _NPROCESSORS_ONLN 2>/dev/null');

        if (is_string($getconf)) {
            $cpuCount = (int) trim($getconf);
        }

        if ($cpuCount < 1) {
            $sysctl = @shell_exec('sysctl -n hw.ncpu 2>/dev/null');

            if (is_string($sysctl)) {
                $cpuCount = (int) trim($sysctl);
            }
        }

        if ($cpuCount < 1) {
            $cpuCount = 2;
        }

        // Keep a conservative cap; too many workers increased merge overhead in local tests.
        return min(7, max(2, $cpuCount));
    }
}