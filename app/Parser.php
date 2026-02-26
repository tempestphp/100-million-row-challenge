<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();
        $workers = max(1, (int) (getenv('PARSER_WORKERS') ?: '1'));

        if ($workers > 1 && function_exists('pcntl_fork')) {
            $this->parseParallel($inputPath, $outputPath, $workers);
        } else {
            $this->parseFast($inputPath, $outputPath);
        }
    }

    private function parseParallel(string $inputPath, string $outputPath, int $workers): void
    {
        $ranges = $this->splitInputRanges($inputPath, $workers);

        if (count($ranges) <= 1) {
            $this->parseFast($inputPath, $outputPath);

            return;
        }

        $tmpFiles = [];
        $pids = [];

        foreach ($ranges as $index => &$range) {
            $tmpFile = tempnam(sys_get_temp_dir(), "parser_worker_{$index}_");

            if ($tmpFile === false) {
                throw new Exception('Unable to create temporary file for parallel parsing');
            }

            $tmpFiles[] = $tmpFile;
            $pid = pcntl_fork();

            if ($pid === -1) {
                throw new Exception('Failed to fork worker process');
            }

            if ($pid === 0) {
                try {
                    $payload = $this->parseRange($inputPath, $range['start'], $range['end']);
                    $serializedPayload = serialize($payload);

                    if (file_put_contents($tmpFile, $serializedPayload) === false) {
                        throw new Exception('Failed to persist worker output');
                    }

                    exit(0);
                } catch (\Throwable $e) {
                    fwrite(STDERR, "[parser-worker-error] {$e->getMessage()}\n");
                    exit(1);
                }
            }

            $pids[] = $pid;
        }

        $childFailure = false;

        foreach ($pids as $pid) {
            $status = 0;
            $waitResult = pcntl_waitpid($pid, $status);

            if ($waitResult <= 0 || ! pcntl_wifexited($status) || pcntl_wexitstatus($status) !== 0) {
                $childFailure = true;
            }
        }

        if ($childFailure) {
            foreach ($tmpFiles as $tmpFile) {
                if (is_file($tmpFile)) {
                    unlink($tmpFile);
                }
            }

            throw new Exception('Parallel parser worker failed');
        }

        $countsByPath = [];
        $pathFirstOffsets = [];
        $dateStringByKey = [];

        foreach ($tmpFiles as $tmpFile) {
            $serializedPayload = file_get_contents($tmpFile);
            unlink($tmpFile);

            if ($serializedPayload === false) {
                throw new Exception('Failed to read worker output');
            }

            $payload = unserialize($serializedPayload, ['allowed_classes' => false]);

            if (! is_array($payload)) {
                throw new Exception('Invalid worker output payload');
            }

            $workerCountsByPath = $payload['countsByPath'] ?? null;
            $workerPathFirstOffsets = $payload['pathFirstOffsets'] ?? null;
            $workerDateStringByKey = $payload['dateStringByKey'] ?? null;

            if (! is_array($workerCountsByPath) || ! is_array($workerPathFirstOffsets) || ! is_array($workerDateStringByKey)) {
                throw new Exception('Malformed worker output payload');
            }

            foreach ($workerPathFirstOffsets as $path => $offset) {
                if (! isset($pathFirstOffsets[$path]) || $offset < $pathFirstOffsets[$path]) {
                    $pathFirstOffsets[$path] = $offset;
                }
            }

            foreach ($workerDateStringByKey as $dateKey => $dateString) {
                $dateStringByKey[(int) $dateKey] = $dateString;
            }

            foreach ($workerCountsByPath as $path => &$workerDateCounts) {
                if (! isset($countsByPath[$path])) {
                    $countsByPath[$path] = [];
                }

                foreach ($workerDateCounts as $dateKey => $count) {
                    $normalizedDateKey = (int) $dateKey;

                    if (isset($countsByPath[$path][$normalizedDateKey])) {
                        $countsByPath[$path][$normalizedDateKey] += $count;
                    } else {
                        $countsByPath[$path][$normalizedDateKey] = $count;
                    }
                }
            }
        }

        $paths = array_keys($pathFirstOffsets);
        usort(
            $paths,
            static fn (string $a, string $b): int => $pathFirstOffsets[$a] <=> $pathFirstOffsets[$b]
        );

        $visits = $this->buildVisitsFromPathCounts($paths, $countsByPath, $dateStringByKey);
        $this->writeOutput($visits, $outputPath);
    }

    /**
     * @return array<int, array{start: int, end: int}>
     */
    private function splitInputRanges(string $inputPath, int $workers): array
    {
        $fileSize = filesize($inputPath);

        if ($fileSize === false) {
            throw new Exception("Unable to stat input file: {$inputPath}");
        }

        if ($fileSize === 0 || $workers <= 1) {
            return [['start' => 0, 'end' => $fileSize]];
        }

        $input = fopen($inputPath, 'r');

        if ($input === false) {
            throw new Exception("Unable to open input file: {$inputPath}");
        }

        $starts = [0];

        for ($i = 1; $i < $workers; ++$i) {
            $target = (int) (($fileSize * $i) / $workers);

            if ($target <= 0 || $target >= $fileSize) {
                continue;
            }

            if (fseek($input, $target) !== 0) {
                continue;
            }

            fgets($input);
            $start = ftell($input);

            if (! is_int($start) || $start <= $starts[count($starts) - 1] || $start >= $fileSize) {
                continue;
            }

            $starts[] = $start;
        }

        fclose($input);

        $starts[] = $fileSize;
        $ranges = [];

        for ($i = 0, $rangeCount = count($starts) - 1; $i < $rangeCount; ++$i) {
            if ($starts[$i] < $starts[$i + 1]) {
                $ranges[] = [
                    'start' => $starts[$i],
                    'end' => $starts[$i + 1],
                ];
            }
        }

        return $ranges;
    }

    /**
     * @return array{
     *   countsByPath: array<string, array<int, int>>,
     *   pathFirstOffsets: array<string, int>,
     *   dateStringByKey: array<int, string>
     * }
     */
    private function parseRange(string $inputPath, int $start, int $end): array
    {
        $input = fopen($inputPath, 'r');

        if ($input === false) {
            throw new Exception("Unable to open input file: {$inputPath}");
        }

        stream_set_read_buffer($input, 1024 * 1024);

        if (fseek($input, $start) !== 0) {
            fclose($input);

            throw new Exception("Unable to seek input file: {$inputPath}");
        }

        $countsByPath = [];
        $pathFirstOffsets = [];
        $dateCache = [];
        $offset = $start;

        while (($line = fgets($input)) !== false) {
            $nextOffset = $offset + strlen($line);

            if ($nextOffset > $end) {
                break;
            }

            $path = substr($line, 25, -27);

            if (! isset($pathFirstOffsets[$path])) {
                $pathFirstOffsets[$path] = $offset;
            }

            $dateString = substr($line, -26, 10);
            $dateKey = $dateCache[$dateString] ??= (
                (ord($dateString[0]) - 48) * 10000000 +
                (ord($dateString[1]) - 48) * 1000000 +
                (ord($dateString[2]) - 48) * 100000 +
                (ord($dateString[3]) - 48) * 10000 +
                (ord($dateString[5]) - 48) * 1000 +
                (ord($dateString[6]) - 48) * 100 +
                (ord($dateString[8]) - 48) * 10 +
                (ord($dateString[9]) - 48)
            );

            $countsByPath[$path][$dateKey] ??= 0;
            ++$countsByPath[$path][$dateKey];

            $offset = $nextOffset;
        }

        fclose($input);

        return [
            'countsByPath' => $countsByPath,
            'pathFirstOffsets' => $pathFirstOffsets,
            'dateStringByKey' => array_flip($dateCache),
        ];
    }

    private function parseFast(string $inputPath, string $outputPath): void
    {
        $input = fopen($inputPath, 'r');

        if ($input === false) {
            throw new Exception("Unable to open input file: {$inputPath}");
        }

        stream_set_read_buffer($input, 1024 * 1024);

        $aggregationMode = getenv('PARSER_AGGREGATE_MODE') ?: 'indexed';
        $dateCache = [];

        if ($aggregationMode === 'string') {
            $countsByPath = [];

            while (($line = fgets($input)) !== false) {
                $path = substr($line, 25, -27);

                $dateString = substr($line, -26, 10);
                $dateKey = $dateCache[$dateString] ??= (
                    (ord($dateString[0]) - 48) * 10000000 +
                    (ord($dateString[1]) - 48) * 1000000 +
                    (ord($dateString[2]) - 48) * 100000 +
                    (ord($dateString[3]) - 48) * 10000 +
                    (ord($dateString[5]) - 48) * 1000 +
                    (ord($dateString[6]) - 48) * 100 +
                    (ord($dateString[8]) - 48) * 10 +
                    (ord($dateString[9]) - 48)
                );

                $countsByPath[$path][$dateKey] ??= 0;
                ++$countsByPath[$path][$dateKey];
            }

            fclose($input);

            $visits = $this->buildVisitsFromPathCounts(array_keys($countsByPath), $countsByPath, array_flip($dateCache));
            $this->writeOutput($visits, $outputPath);

            return;
        }

        $countsByPathIndex = [];
        $pathIndexByUrl = [];
        $pathIndexByPath = [];
        $pathCount = 0;

        while (($line = fgets($input)) !== false) {
            $path = substr($line, 25, -27);
            $pathIndex = $pathIndexByUrl[$path] ??= $this->resolvePathIndex($path, $pathIndexByPath, $pathCount);

            $dateString = substr($line, -26, 10);
            $dateKey = $dateCache[$dateString] ??= (
                (ord($dateString[0]) - 48) * 10000000 +
                (ord($dateString[1]) - 48) * 1000000 +
                (ord($dateString[2]) - 48) * 100000 +
                (ord($dateString[3]) - 48) * 10000 +
                (ord($dateString[5]) - 48) * 1000 +
                (ord($dateString[6]) - 48) * 100 +
                (ord($dateString[8]) - 48) * 10 +
                (ord($dateString[9]) - 48)
            );

            $countsByPathIndex[$pathIndex][$dateKey] ??= 0;
            ++$countsByPathIndex[$pathIndex][$dateKey];
        }

        fclose($input);

        $visits = $this->buildVisitsFromIndexedCounts($pathIndexByPath, $countsByPathIndex, $dateCache);
        $this->writeOutput($visits, $outputPath);
    }

    private function buildVisitsFromIndexedCounts(array $pathIndexByPath, array $countsByPathIndex, array $dateCache): array
    {
        $outputMode = getenv('PARSER_OUTPUT_MODE') ?: 'manual';
        $visits = [];
        $dateStringByKey = array_flip($dateCache);
        $paths = array_flip($pathIndexByPath);

        foreach ($paths as $pathIndex => &$path) {
            $dateCounts = $countsByPathIndex[$pathIndex] ?? [];
            ksort($dateCounts, SORT_NUMERIC);

            $dates = [];
            foreach ($dateCounts as $dateKey => $count) {
                $dates[$dateStringByKey[$dateKey]] = $count;
            }

            if ($outputMode === 'json') {
                $visits["/blog/{$path}"] = $dates;
            } else {
                $visits[$path] = $dates;
            }
        }

        return $visits;
    }

    private function buildVisitsFromPathCounts(array $paths, array $countsByPath, array $dateStringByKey): array
    {
        $outputMode = getenv('PARSER_OUTPUT_MODE') ?: 'manual';
        $visits = [];

        foreach ($paths as &$path) {
            $dateCounts = $countsByPath[$path] ?? [];
            ksort($dateCounts, SORT_NUMERIC);

            $dates = [];
            foreach ($dateCounts as $dateKey => $count) {
                $dates[$dateStringByKey[$dateKey]] = $count;
            }

            if ($outputMode === 'json') {
                $visits["/blog/{$path}"] = $dates;
            } else {
                $visits[$path] = $dates;
            }
        }

        return $visits;
    }

    private function writeOutput(array $visits, string $outputPath): void
    {
        $outputMode = getenv('PARSER_OUTPUT_MODE') ?: 'manual';

        if ($outputMode === 'json') {
            file_put_contents($outputPath, json_encode($visits, JSON_PRETTY_PRINT));
            return;
        }

        $file = fopen($outputPath, 'w');
        $buffer = '{' . PHP_EOL;
        $pathCount = count($visits);
        foreach ($visits as $path => &$dates) {
            $buffer .= '    "\/blog\/' . $path . '": {' . PHP_EOL;
            $dateCount = count($dates);
            foreach ($dates as $date => $count) {
                $dateComma = (--$dateCount) ? ',' : '';
                $buffer .= '        "' . $date . '": ' . $count . $dateComma . PHP_EOL;
            }
            $pathComma = (--$pathCount) ? ',' : '';
            $buffer .= '    }' . $pathComma . PHP_EOL;
        }
        $buffer .= '}';
        fwrite($file, $buffer);
        fclose($file);
    }

    private function resolvePathIndex(string $path, array &$pathIndexByPath, int &$pathCount): int {
        // if (isset($pathIndexByPath[$path])) {
        //     return $pathIndexByPath[$path];
        // }

        return $pathIndexByPath[$path] ??= $pathCount++;
    }
}
