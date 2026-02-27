<?php

namespace App;

use App\Commands\Visit;

final class Parser
{
    private const DEFAULT_READ_CHUNK_SIZE = 262_144;

    private ?int $readChunkSize = null;
    private ?array $pathIdByPath = null;
    private ?array $pathLinePrefix = null;
    private ?array $generatedDateKeyByString = null;
    private ?array $generatedDateLinePrefix = null;

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();
        $this->initializePathCache();
        $this->initializeGeneratedDateCache();
        $workers = max(2, (int) (getenv('PARSER_WORKERS') ?: '4'));

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

            $tmpFiles[] = $tmpFile;
            $pid = pcntl_fork();

            if ($pid === 0) {
                try {
                    $this->parseRange($inputPath, $range['start'], $range['end'], $tmpFile);
                    exit(0);
                } catch (\Throwable $e) {
                    fwrite(STDERR, "[parser-worker-error] {$e->getMessage()}\n");
                    exit(1);
                }
            }

            $pids[] = $pid;
        }

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $countsByPath = [];
        foreach ($tmpFiles as $tmpFile) {
            $workerCountsByPath = unserialize(file_get_contents($tmpFile));
            unlink($tmpFile);

            foreach ($workerCountsByPath as $pathId => $workerDateCounts) {
                if (! isset($countsByPath[$pathId])) {
                    $countsByPath[$pathId] = $workerDateCounts;
                } else {
                    $row = &$countsByPath[$pathId];
                    foreach ($workerDateCounts as $dateKey => $count) {
                        $row[$dateKey] = ($row[$dateKey] ?? 0) + $count;
                    }
                }
            }
        }

        $this->buildVisitsAndWriteOutput($countsByPath, $outputPath);
    }

    private function parseSingleRange(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);

        $countsByPath = $this->parseRange($inputPath, 0, $fileSize);

        $this->buildVisitsAndWriteOutput($countsByPath, $outputPath);
    }

    /**
     * @return array<int, array{start: int, end: int}>
     */
    private function splitInputRanges(string $inputPath, int $workers): array
    {
        $fileSize = filesize($inputPath);

        if ($fileSize === 0 || $workers <= 1) {
            return [['start' => 0, 'end' => $fileSize]];
        }

        $input = fopen($inputPath, 'r');

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

            if ($start <= $starts[count($starts) - 1] || $start >= $fileSize) {
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

            fseek($input, -$remainingBytes, SEEK_CUR);

            return;
        }
    }

    /**
     * @return ($tmpFile is null ? array<int, array<int, int>> : void)
     */
    private function parseRange(string $inputPath, int $start, int $end, ?string $tmpFile = null): array|null
    {
        $input = fopen($inputPath, 'r');

        stream_set_read_buffer($input, 1024 * 1024);

        fseek($input, $start);

        $countsByPath = [];
        $pathIdByPath = &$this->pathIdByPath;
        $buffer = '';
        $chunkSize = $this->getReadChunkSize();
        $remaining = $end - $start;

        while ($remaining > 0) {
            $chunk = fread($input, min($chunkSize, $remaining));

            if ($chunk === '' || $chunk === false) {
                break;
            }

            $remaining -= strlen($chunk);
            $buffer .= $chunk;
            $lineStart = 0;

            while (($newlinePos = strpos($buffer, "\n", $lineStart)) !== false) {
                $row = &$countsByPath[$pathIdByPath[substr($buffer, $lineStart + 25, $newlinePos - $lineStart - 51)]];
                $dateKey =
                    (ord($buffer[$newlinePos - 22]) - 48) * 10000 +
                    (ord($buffer[$newlinePos - 20]) - 48) * 1000 +
                    (ord($buffer[$newlinePos - 19]) - 48) * 100 +
                    (ord($buffer[$newlinePos - 17]) - 48) * 10 +
                    (ord($buffer[$newlinePos - 16]) - 48);

                $row[$dateKey] = ($row[$dateKey] ?? 0) + 1;

                $lineStart = $newlinePos + 1;
            }

            if ($lineStart > 0) {
                $buffer = substr($buffer, $lineStart);
            }
        }

        fclose($input);

        if ($tmpFile !== null) {
            file_put_contents($tmpFile, serialize($countsByPath));

            return null;
        }

        return $countsByPath;
    }

    private function buildVisitsAndWriteOutput(array $countsByPath, string $outputPath): void
    {
        $file = fopen($outputPath, 'w');

        $dateLinePrefix = &$this->generatedDateLinePrefix;
        $pathLinePrefix = &$this->pathLinePrefix;
        $buffer = '{' . PHP_EOL;
        $pathCount = count($countsByPath);

        foreach ($countsByPath as $pathId => &$dateCounts) {
            ksort($dateCounts, SORT_NUMERIC);

            $buffer .= $pathLinePrefix[$pathId];
            $dateCount = count($dateCounts);

            foreach ($dateCounts as $dateKey => $count) {
                $dateComma = (--$dateCount) ? ',' : '';
                $buffer .= $dateLinePrefix[$dateKey] . $count . $dateComma . PHP_EOL;
            }

            $pathComma = (--$pathCount) ? ',' : '';
            $buffer .= '    }' . $pathComma . PHP_EOL;
        }

        $buffer .= '}';
        fwrite($file, $buffer);
        fclose($file);
    }

    private function initializePathCache(): void
    {
        if ($this->pathIdByPath !== null) {
            return;
        }

        $pathIdByPath = [];
        $pathLinePrefix = [];

        foreach (Visit::all() as $pathId => $visit) {
            $path = substr($visit->uri, 25);
            $pathIdByPath[$path] = $pathId;
            $pathLinePrefix[$pathId] = '    "\/blog\/' . $path . '": {' . PHP_EOL;
        }

        $this->pathIdByPath = $pathIdByPath;
        $this->pathLinePrefix = $pathLinePrefix;
    }

    private function initializeGeneratedDateCache(): void
    {
        if ($this->generatedDateKeyByString !== null) {
            return;
        }

        $startDayTimestamp = strtotime('2021-01-01 00:00:00 UTC');
        $endDayTimestamp = strtotime('2026-12-31 00:00:00 UTC');
        $oneDaySeconds = 60 * 60 * 24;

        $dateKeyByString = [];
        $dateLinePrefix = [];

        for ($timestamp = $startDayTimestamp; $timestamp <= $endDayTimestamp; $timestamp += $oneDaySeconds) {
            $dateString = gmdate('y-m-d', $timestamp);
            $shortDate = substr($dateString, 1); // "4-06-15" instead of "24-06-15"
            $dateKey = (
                (ord($dateString[1]) - 48) * 10000 +
                (ord($dateString[3]) - 48) * 1000 +
                (ord($dateString[4]) - 48) * 100 +
                (ord($dateString[6]) - 48) * 10 +
                (ord($dateString[7]) - 48)
            );

            $dateKeyByString[$shortDate] = $dateKey;
            $dateLinePrefix[$dateKey] = '        "202' . $shortDate . '": ';
        }

        $this->generatedDateKeyByString = $dateKeyByString;
        $this->generatedDateLinePrefix = $dateLinePrefix;
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
