<?php

namespace App;

use Exception;

final class Parser
{
    private const DEFAULT_READ_CHUNK_SIZE = 262_144;

    private ?int $readChunkSize = null;
    private ?array $generatedDateKeyByString = null;
    private ?array $generatedDateStringByKey = null;

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();
        $this->initializeGeneratedDateCache();
        $workers = max(2, (int) (getenv('PARSER_WORKERS') ?: '4'));

        if (! function_exists('pcntl_fork')) {
            throw new Exception('pcntl_fork is required (parser assumes at least 2 workers)');
        }

        $this->parseParallel($inputPath, $outputPath, $workers);
    }

    private function parseParallel(string $inputPath, string $outputPath, int $workers): void
    {
        $ranges = $this->splitInputRanges($inputPath, $workers);

        if (count($ranges) <= 1) {
            $this->parseSingleRange($inputPath, $outputPath);

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
                    $serializedPayload = $this->serializeWorkerPayload($payload);

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
        foreach ($tmpFiles as $tmpFile) {
            $serializedPayload = file_get_contents($tmpFile);
            unlink($tmpFile);

            if ($serializedPayload === false) {
                throw new Exception('Failed to read worker output');
            }

            $payload = $this->unserializeWorkerPayload($serializedPayload);

            if (! is_array($payload)) {
                throw new Exception('Invalid worker output payload');
            }

            $workerCountsByPath = $payload['countsByPath'] ?? null;

            if (! is_array($workerCountsByPath)) {
                throw new Exception('Malformed worker output payload');
            }

            foreach ($workerCountsByPath as $path => $workerDateCounts) {
                if (! isset($countsByPath[$path])) {
                    $countsByPath[$path] = [];
                }
                $row = &$countsByPath[$path];
                foreach ($workerDateCounts as $dateKey => $count) {
                    $row[$dateKey] = ($row[$dateKey] ?? 0) + $count;
                }
            }
        }

        $this->buildVisitsAndWriteOutput($countsByPath, $outputPath);
    }

    private function parseSingleRange(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);

        if (! is_int($fileSize)) {
            throw new Exception("Unable to stat input file: {$inputPath}");
        }

        $payload = $this->parseRange($inputPath, 0, $fileSize);
        $countsByPath = $payload['countsByPath'] ?? [];

        if (! is_array($countsByPath)) {
            throw new Exception('Invalid single-range payload');
        }

        $this->buildVisitsAndWriteOutput($countsByPath, $outputPath);
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

            $this->skipToNextLine($input);
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
     * Advances the file pointer to the byte after the next newline.
     *
     * @param resource $input
     */
    private function skipToNextLine($input): void
    {
        while (! feof($input)) {
            $chunk = fread($input, 4096);

            if ($chunk === false) {
                throw new Exception('Unable to read input while splitting ranges');
            }

            if ($chunk === '') {
                return;
            }

            $newlinePos = strpos($chunk, "\n");

            if ($newlinePos === false) {
                continue;
            }

            $remainingBytes = strlen($chunk) - $newlinePos - 1;

            if ($remainingBytes <= 0) {
                return;
            }

            if (fseek($input, -$remainingBytes, SEEK_CUR) !== 0) {
                throw new Exception('Unable to align range start to line boundary');
            }

            return;
        }
    }

    /**
     * @return array{
     *   countsByPath: array<string, array<int, int>>
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
        $dateCache = &$this->generatedDateKeyByString;
        $buffer = '';
        $bufferStartOffset = $start;
        $reachedEnd = false;

        while (! $reachedEnd && ! feof($input)) {
            $chunk = fread($input, $this->getReadChunkSize());

            if ($chunk === false) {
                fclose($input);

                throw new Exception("Unable to read input file: {$inputPath}");
            }

            if ($chunk === '') {
                break;
            }

            $buffer .= $chunk;
            $lineStart = 0;

            while (($newlinePos = strpos($buffer, "\n", $lineStart)) !== false) {
                if ($bufferStartOffset + $newlinePos + 1 > $end) {
                    $reachedEnd = true;
                    break 2;
                }
                $path = substr($buffer, $lineStart + 25, $newlinePos - $lineStart + 1 - 52);
                $dateKey = $dateCache[substr($buffer, $newlinePos - 23, 8)];
                $countsByPath[$path][$dateKey] = ($countsByPath[$path][$dateKey] ?? 0) + 1;

                $lineStart = $newlinePos + 1;
            }

            if ($lineStart > 0) {
                $buffer = substr($buffer, $lineStart);
                $bufferStartOffset += $lineStart;
            }
        }

        fclose($input);

        return [
            'countsByPath' => $countsByPath,
        ];
    }

    private function buildVisitsAndWriteOutput(array $countsByPath, string $outputPath): void
    {
        $file = fopen($outputPath, 'w');
        if ($file === false) {
            throw new Exception("Unable to open output file: {$outputPath}");
        }

        $dateStringByKey = &$this->generatedDateStringByKey;
        $buffer = '{' . PHP_EOL;
        $pathCount = count($countsByPath);

        foreach ($countsByPath as $path => &$dateCounts) {
            ksort($dateCounts, SORT_NUMERIC);

            $buffer .= '    "\/blog\/' . $path . '": {' . PHP_EOL;
            $dateCount = count($dateCounts);

            foreach ($dateCounts as $dateKey => $count) {
                $dateComma = (--$dateCount) ? ',' : '';
                $buffer .= '        "20' . $dateStringByKey[$dateKey] . '": ' . $count . $dateComma . PHP_EOL;
            }

            $pathComma = (--$pathCount) ? ',' : '';
            $buffer .= '    }' . $pathComma . PHP_EOL;
        }

        $buffer .= '}';
        fwrite($file, $buffer);
        fclose($file);
    }

    private function serializeWorkerPayload(array $payload): string
    {
        if (function_exists('igbinary_serialize')) {
            $serialized = igbinary_serialize($payload);

            if (! is_string($serialized)) {
                throw new Exception('Failed to serialize worker payload with igbinary');
            }

            return $serialized;
        }

        return serialize($payload);
    }

    private function unserializeWorkerPayload(string $serializedPayload): array
    {
        if ($serializedPayload === '') {
            throw new Exception('Empty worker output payload');
        }

        if (function_exists('igbinary_unserialize')) {
            $payload = @igbinary_unserialize($serializedPayload);

            if (is_array($payload)) {
                return $payload;
            }
        }

        $payload = @unserialize($serializedPayload, ['allowed_classes' => false]);

        if (is_array($payload)) {
            return $payload;
        }

        throw new Exception('Invalid worker output payload serialization');
    }

    private function initializeGeneratedDateCache(): void
    {
        if ($this->generatedDateKeyByString !== null && $this->generatedDateStringByKey !== null) {
            return;
        }

        $startDayTimestamp = strtotime('2021-01-01 00:00:00 UTC');
        $endDayTimestamp = strtotime('2026-12-31 00:00:00 UTC');
        $oneDaySeconds = 60 * 60 * 24;

        if (! is_int($startDayTimestamp) || ! is_int($endDayTimestamp)) {
            throw new Exception('Unable to initialize fixed date cache bounds');
        }

        $dateKeyByString = [];
        $dateStringByKey = [];

        for ($timestamp = $startDayTimestamp; $timestamp <= $endDayTimestamp; $timestamp += $oneDaySeconds) {
            $dateString = gmdate('y-m-d', $timestamp);
            $dateKey = (
                (ord($dateString[0]) - 48) * 100000 +
                (ord($dateString[1]) - 48) * 10000 +
                (ord($dateString[3]) - 48) * 1000 +
                (ord($dateString[4]) - 48) * 100 +
                (ord($dateString[6]) - 48) * 10 +
                (ord($dateString[7]) - 48)
            );

            $dateKeyByString[$dateString] = $dateKey;
            $dateStringByKey[$dateKey] = $dateString;
        }

        $this->generatedDateKeyByString = $dateKeyByString;
        $this->generatedDateStringByKey = $dateStringByKey;
    }

    private function getReadChunkSize(): int
    {
        if ($this->readChunkSize !== null) {
            return $this->readChunkSize;
        }

        $size = (int) (getenv('PARSER_READ_CHUNK_SIZE') ?: self::DEFAULT_READ_CHUNK_SIZE);

        if ($size < 65_536) {
            $size = 65_536;
        } elseif ($size > 16_777_216) {
            $size = 16_777_216;
        }

        return $this->readChunkSize = $size;
    }

}
