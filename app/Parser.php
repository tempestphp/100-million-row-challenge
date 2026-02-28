<?php

namespace App;

use App\Commands\Visit;

final class Parser
{
    private const DEFAULT_READ_CHUNK_SIZE = 131_072; // 262_144;

    private ?int $workers = 10;
    private ?array $pathIdByPath = null;
    private ?array $pathLinePrefix = null;
    private ?array $paths = null;
    private int $pathCount = 0;
    private ?array $generatedDateKeyByString = null;
    private ?array $generatedDateLinePrefix = null;
    private ?array $tmpFiles = [];
    private ?array $tmpHandles = [];

    public function parse(string $inputPath, string $outputPath): void
    {
        ini_set('memory_limit', '10G');
        gc_disable();
        $this->initializePathCache($inputPath);
        $this->initializeGeneratedDateCache();

        $this->workers = max(2, (int) (getenv('PARSER_WORKERS') ?: $this->workers));

        $this->parseParallel($inputPath, $outputPath);
    }

    private function parseParallel(string $inputPath, string $outputPath): void
    {
        $ranges = $this->splitInputRanges($inputPath, $this->workers);

        if (count($ranges) <= 1) {
            $this->parseSingleRange($inputPath, $outputPath);

            return;
        }

        $workerCount = count($ranges);
        $socketPairs = [];
        for ($i = 0; $i < $workerCount; $i++) {
            $socketPairs[$i] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        }

        $pids = [];
        foreach ($ranges as $index => &$range) {
            $pid = pcntl_fork();

            if ($pid === 0) {
                // Child: close all read ends + other workers' write ends
                foreach ($socketPairs as $i => $pair) {
                    fclose($pair[0]);
                    if ($i !== $index) {
                        fclose($pair[1]);
                    }
                }

                try {
                    $this->parseRange($inputPath, $range['start'], $range['end'], $socketPairs[$index][1]);
                    exit(0);
                } catch (\Throwable $e) {
                    fwrite(STDERR, "[parser-worker-error] {$e->getMessage()}\n");
                    exit(1);
                }
            }

            $pids[] = $pid;
        }

        // Parent: close all write ends after all forks
        foreach ($socketPairs as $pair) {
            fclose($pair[1]);
        }

        // Parent: read from all sockets concurrently
        $readEnds = [];
        foreach ($socketPairs as $i => $pair) {
            $readEnds[$i] = $pair[0];
            stream_set_blocking($pair[0], false);
        }

        $buffers = array_fill(0, $workerCount, '');
        $active = $readEnds;

        while (!empty($active)) {
            $read = array_values($active);
            $write = null;
            $except = null;
            stream_select($read, $write, $except, 30);

            foreach ($read as $socket) {
                $idx = array_search($socket, $active, true);
                $chunk = fread($socket, 2097152);
                if ($chunk !== '' && $chunk !== false) {
                    $buffers[$idx] .= $chunk;
                }
                if (feof($socket)) {
                    fclose($socket);
                    unset($active[$idx]);
                }
            }
        }

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $this->mergeAndWriteOutput($outputPath, $buffers);
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
    private function parseRange(string $inputPath, int $start, int $end, $socket): void
    {
        set_error_handler(fn () => true);
        $t0 = hrtime(true);
        $input = fopen($inputPath, 'r');
        stream_set_read_buffer($input, 0);
        fseek($input, $start);

        $pathIdByPath = &$this->pathIdByPath;
        $countsByPath = $this->paths;
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

            $buffer .= $chunk = fread($input, min($remaining, $chunkSize));

            if ($chunk === false) {
                break;
            }

            $remaining -= $chunkSize;
            $lineStart = 0;

            while (($newlinePos = strpos($buffer, "\n", $lineStart))) {
                $pathId = $pathIdByPath[substr($buffer, $lineStart + 25, $newlinePos - $lineStart - 51)];
                $dateKey = $dateKeyByString[substr($buffer, $newlinePos - 22, 7)];
                $lineStart = $newlinePos + 1;
                $countsByPath[$pathId][$dateKey]++;
            }
        }

        fclose($input);
        $data = function_exists('igbinary_serialize') ? igbinary_serialize($countsByPath) : serialize($countsByPath);
        $len = strlen($data);
        $written = 0;
        while ($written < $len) {
            $w = fwrite($socket, substr($data, $written, 8_388_608));
            if ($w === false || $w === 0) {
                break;
            }
            $written += $w;
        }
        fclose($socket);
        echo "[worker] parseRange time: " . ((hrtime(true) - $t0) / 1e9) * 1000 . " ms" . PHP_EOL;
    }

    private function mergeAndWriteOutput(string $outputPath, array &$buffers): void
    {
        $startTime = microtime(true);
        set_error_handler(fn () => true);
        $file = fopen($outputPath, 'w');
        stream_set_write_buffer($file, 0);

        $unserialize = function_exists('igbinary_unserialize') ? 'igbinary_unserialize' : 'unserialize';
        foreach ($buffers as $buf) {
            $allWorkerResults[] = $unserialize($buf);
        }
        $dateLinePrefix = &$this->generatedDateLinePrefix;
        $pathLinePrefix = &$this->pathLinePrefix;
        $pathCount = $this->pathCount;
        $lastPathId = $pathCount - 1;

        fwrite($file, '{' . PHP_EOL);

        for ($pathId = 0; $pathId <= $lastPathId; $pathId++) {
            $dateCounts = $allWorkerResults[0][$pathId];
            for ($w = 1; $w < $this->workers; ++$w) {
                foreach ($allWorkerResults[$w][$pathId] as $dateKey => $count) {
                    $dateCounts[$dateKey] += $count;
                }
            }

            ksort($dateCounts, SORT_NUMERIC);
            $dateCount = count($dateCounts);

            $buffer = $pathLinePrefix[$pathId];
            foreach ($dateCounts as $dateKey => $count) {
                $buffer .= $dateLinePrefix[$dateKey] . $count . ((--$dateCount) ? ',' : '') . PHP_EOL;
            }
            $buffer .= ((--$pathCount) ? '    },' : '    }') . PHP_EOL;

            fwrite($file, $buffer);
        }

        fwrite($file, '}');
        fclose($file);
        $endTime = microtime(true);
        $executionTime = ($endTime - $startTime)*1000;
        echo "mergeAndWriteOutput total: $executionTime ms" . PHP_EOL;
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

    private function initializePathCache(string $inputPath): void
    {
        if ($this->pathIdByPath !== null) {
            return;
        }

        $totalPaths = count(Visit::all());
        $pathIdByPath = [];
        $pathLinePrefix = [];
        $nextId = 0;

        $input = fopen($inputPath, 'r');

        while (($line = fgets($input)) !== false) {
            $commaPos = strpos($line, ',', 25);
            $path = substr($line, 25, $commaPos - 25);

            if (!isset($pathIdByPath[$path])) {
                $pathIdByPath[$path] = $nextId;
                $pathLinePrefix[$nextId] = '    "\/blog\/' . $path . '": {' . PHP_EOL;
                $nextId++;
                if ($nextId >= $totalPaths) {
                    break;
                }
            }
        }

        fclose($input);

        $this->pathIdByPath = $pathIdByPath;
        $this->pathLinePrefix = $pathLinePrefix;
        $this->paths = array_fill(0, $nextId, []);
        $this->pathCount = $nextId;
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
