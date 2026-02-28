<?php

namespace App;

use App\Commands\Visit;

final class Parser
{
    private const DEFAULT_READ_CHUNK_SIZE = 262_144;

    private ?int $workers = 12;
    private ?array $pathIdByPath = null;
    private ?array $pathLinePrefix = null;
    private ?array $generatedDateKeyByString = null;
    private ?array $generatedDateLinePrefix = null;
    private ?array $tmpFiles = [];
    private ?array $tmpHandles = [];

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();
        $this->initializePathCache();
        $this->initializeGeneratedDateCache();

        $this->workers = max(2, (int) (getenv('PARSER_WORKERS') ?: '12'));

        $this->parseParallel($inputPath, $outputPath);
    }

    private function parseParallel(string $inputPath, string $outputPath): void
    {
        $ranges = $this->splitInputRanges($inputPath, $this->workers);

        if (count($ranges) <= 1) {
            $this->parseSingleRange($inputPath, $outputPath);

            return;
        }

        // $tmpFiles = [];
        // $tmpHandles = [];
        $pids = [];

        foreach ($ranges as $index => &$range) {
            $tmpFile = tempnam(sys_get_temp_dir(), "parser_worker_{$index}_");
            $this->tmpFiles[] = $tmpFile;
            $this->tmpHandles[] = fopen($tmpFile, 'w+');
        }

        foreach ($ranges as $index => &$range) {
            $pid = pcntl_fork();

            if ($pid === 0) {
                try {
                    $this->parseRange($inputPath, $range['start'], $range['end'], $index);
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

        $this->mergeAndWriteOutput($outputPath);
    }

    private function parseSingleRange(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);

        $countsByPath = $this->parseRange($inputPath, 0, $fileSize, $this->tmpHandles[0]);

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
    private function parseRange(string $inputPath, int $start, int $end, $index): void
    {
        $input = fopen($inputPath, 'r');
        stream_set_read_buffer($input, 0);
        fseek($input, $start);


        $pathIdByPath = &$this->pathIdByPath;
        $countsByPath = [];
        $dateKeyByString = &$this->generatedDateKeyByString;
        $buffer = '';
        $lineStart = $bufferOffset = 0;
        $chunkSize = max(65_536, min(16_777_216, (int) (getenv('PARSER_READ_CHUNK_SIZE') ?: self::DEFAULT_READ_CHUNK_SIZE)));
        $remaining = $end - $start;

        while ($remaining > 0) {
            $bufferOffset = $lineStart;
            if ($bufferOffset > 0) {
                $buffer = substr($buffer, $bufferOffset);
                $bufferOffset = 0;
            }

            $buffer .= $chunk = fread($input, $remaining > $chunkSize ? $chunkSize : $remaining);

            if ($chunk === false) {

                break;
            }

            $remaining -= $chunkSize;
            $lineStart = 0;

            while (($newlinePos = strpos($buffer, "\n", $lineStart)) !== false) {
                $row = &$countsByPath[$pathIdByPath[substr($buffer, $lineStart + 25, $newlinePos - $lineStart - 51)]];
                $dateKey = $dateKeyByString[substr($buffer, $newlinePos - 22, 7)];
                $lineStart = $newlinePos + 1;
                $row[$dateKey] = ($row[$dateKey] ?? 0) + 1;
            }
        }

        fclose($input);
        fwrite($this->tmpHandles[$index], serialize($countsByPath));
        fclose($this->tmpHandles[$index]);

    }

    private function mergeAndWriteOutput(string $outputPath): void
    {
        $startTime = microtime(true);
        set_error_handler(fn () => true);
        $file = fopen($outputPath, 'w');
        stream_set_write_buffer($file, 0);

        $pathIds = [];
        for($i = 0; $i < $this->workers; $i++) {
            $pathIds += $allWorkerResults[] = unserialize(file_get_contents($this->tmpFiles[$i]));
            unlink($this->tmpFiles[$i]);
        }

        // Collect all unique pathIds — array + union on pathId-keyed arrays
        // $pathIds = $allWorkerResults[0] + $allWorkerResults[1] + $allWorkerResults[2] + $allWorkerResults[3] + $allWorkerResults[4] + $allWorkerResults[5] + $allWorkerResults[6] + $allWorkerResults[7] + $allWorkerResults[8] + $allWorkerResults[9];

        $dateLinePrefix = &$this->generatedDateLinePrefix;
        $pathLinePrefix = &$this->pathLinePrefix;
        $pathCount = count($pathIds);

        $w0_results = &$allWorkerResults[0];
        fwrite($file, '{' . PHP_EOL);
        // $buffer = '{' . PHP_EOL;
        // Single loop: for each path, merge all workers, sort, write
        foreach ($pathIds as $pathId => $_) {
            $dateCounts = &$w0_results[$pathId];

            for ($w = 1; $w < $this->workers; ++$w) {
                foreach ($allWorkerResults[$w][$pathId] as $dateKey => $count) {
                    $dateCounts[$dateKey] = $dateCounts[$dateKey] + $count;
                }
            }

            ksort($dateCounts, SORT_NUMERIC);

            $buffer = $pathLinePrefix[$pathId];
            $dateCount = count($dateCounts);

            foreach ($dateCounts as $dateKey => $count) {
                $buffer .= $dateLinePrefix[$dateKey] . $count . ((--$dateCount) ? ',' : '') . PHP_EOL;
            }


            $buffer .= ((--$pathCount) ? '    },' : '    }') . PHP_EOL;

            fwrite($file, $buffer);
        }

        fwrite($file, '}');
        fclose($file);
        $endTime = microtime(true);
        $executionTime = $endTime - $startTime;
        echo "mergeAndWriteOutput time: $executionTime seconds" . PHP_EOL;
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
                $buffer .= $dateLinePrefix[$dateKey] . $count . ((--$dateCount) ? ',' : '') . PHP_EOL;
            }


            $buffer .= ((--$pathCount) ? '    },' : '    }') . PHP_EOL;
            // fwrite($file, $buffer . '    }' . $pathComma . PHP_EOL);
            // $buffer = '';

        }

        fwrite($file, $buffer . '}');
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
            $dateLinePrefix[$dateKey] = '        "20' . $dateString . '": ';
        }

        $this->generatedDateKeyByString = $dateKeyByString;
        $this->generatedDateLinePrefix = $dateLinePrefix;
    }

}
