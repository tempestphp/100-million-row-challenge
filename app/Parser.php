<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);
        $numWorkers = $this->getWorkerCount($fileSize);

        $splitPoints = [0];
        $fp = fopen($inputPath, 'r');
        for ($i = 1; $i < $numWorkers; $i++) {
            fseek($fp, (int)($fileSize * $i / $numWorkers));
            fgets($fp);
            $splitPoints[] = ftell($fp);
        }
        $splitPoints[] = $fileSize;
        fclose($fp);

        [$dateNames, $yearOffsets, $yearMonthOffsets, $numDates] = $this->buildDateData();
        [$pathMap, $pathNames, $numPaths] = $this->scanPathNames($inputPath, $fileSize);
        $pathJson = $this->buildPathJson($pathNames);

        if ($numWorkers === 1) {
            // Avoid fork + IPC overhead for smaller inputs.
            $merged = $this->processChunk(
                $inputPath,
                $splitPoints[0],
                $splitPoints[1],
                $pathMap,
                $yearOffsets,
                $yearMonthOffsets,
                $numPaths,
                $numDates
            );

            $this->writeOutput($outputPath, $pathJson, $dateNames, $numDates, $merged);
            return;
        }

        $countsBytes = $numPaths * $numDates * 4;
        $totalCounts = $numPaths * $numDates;
        $useShmop = true;
        $shmKeys = [];
        $shmSegments = [];

        if ($useShmop) {
            for ($w = 0; $w < $numWorkers; $w++) {
                $proj = chr(65 + ($w % 26));
                $key = ftok($inputPath, $proj);
                if ($key === false || $key === -1) {
                    $useShmop = false;
                    break;
                }
                $shm = $this->tryShmopOpen($key, 'c', 0644, $countsBytes);
                if ($shm === false) {
                    $useShmop = false;
                    break;
                }
                $shmKeys[$w] = $key;
                $shmSegments[$w] = $shm;
            }

            if (! $useShmop) {
                foreach ($shmSegments as $shm) {
                    @shmop_delete($shm);
                }
                $shmSegments = [];
                $shmKeys = [];
            }
        }

        $tempDir = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
        $tempFiles = [];
        $pids = [];

        for ($w = 0; $w < $numWorkers; $w++) {
            if (! $useShmop) {
                $tempFiles[$w] = tempnam($tempDir, "p{$w}_");
            }
            $pid = pcntl_fork();
            if ($pid === 0) {
                $counts = $this->processChunk(
                    $inputPath, $splitPoints[$w], $splitPoints[$w + 1],
                    $pathMap, $yearOffsets, $yearMonthOffsets, $numPaths, $numDates
                );
                if ($useShmop) {
                    $shm = $this->tryShmopOpen($shmKeys[$w], 'w', 0, 0);
                    if ($shm !== false) {
                        shmop_write($shm, pack('V*', ...$counts), 0);
                    }
                } else {
                    file_put_contents($tempFiles[$w], pack('V*', ...$counts));
                }
                exit(0);
            }
            $pids[] = $pid;
        }

        $merged = array_fill(0, $totalCounts, 0);
        for ($w = 0; $w < $numWorkers; $w++) {
            pcntl_waitpid($pids[$w], $s);
            if ($useShmop) {
                $raw = shmop_read($shmSegments[$w], 0, $countsBytes);
                shmop_delete($shmSegments[$w]);
            } else {
                $raw = file_get_contents($tempFiles[$w]);
                unlink($tempFiles[$w]);
            }

            if ($raw === false || $raw === '') {
                continue;
            }

            $workerData = unpack('V' . $totalCounts, $raw);
            if ($workerData === false) {
                continue;
            }

            for ($i = 1; $i <= $totalCounts; $i++) {
                $merged[$i - 1] += $workerData[$i];
            }
        }

        $this->writeOutput($outputPath, $pathJson, $dateNames, $numDates, $merged);
    }

    private function writeOutput(string $outputPath, array $pathJson, array $dateNames, int $numDates, array $merged): void
    {
        $fp = fopen($outputPath, 'w');
        if ($fp === false) {
            return;
        }
        stream_set_write_buffer($fp, 1024 * 1024);

        $buffer = "{\n";
        $firstPath = true;
        $flushThreshold = 1024 * 1024;

        foreach ($pathJson as $pIdx => $path) {
            $base = $pIdx * $numDates;
            $dateLines = [];

            for ($d = 0; $d < $numDates; $d++) {
                $count = $merged[$base + $d];
                if ($count > 0) {
                    $dateLines[] = "        \"{$dateNames[$d]}\": $count";
                }
            }

            if ($dateLines === []) {
                continue;
            }

            if (! $firstPath) {
                $buffer .= ",\n";
            }
            $firstPath = false;

            $buffer .= "    {$path}: {\n" . implode(",\n", $dateLines) . "\n    }";

            if (strlen($buffer) >= $flushThreshold) {
                fwrite($fp, $buffer);
                $buffer = '';
            }
        }

        $buffer .= "\n}";
        fwrite($fp, $buffer);
        fclose($fp);
    }

    private function buildDateData(): array
    {
        $names = [];
        $yearOffsets = [];
        $monthOffsets = [
            1 => 0,
            2 => 31,
            3 => 59,
            4 => 90,
            5 => 120,
            6 => 151,
            7 => 181,
            8 => 212,
            9 => 243,
            10 => 273,
            11 => 304,
            12 => 334,
        ];
        $monthOffsetsLeap = [
            1 => 0,
            2 => 31,
            3 => 60,
            4 => 91,
            5 => 121,
            6 => 152,
            7 => 182,
            8 => 213,
            9 => 244,
            10 => 274,
            11 => 305,
            12 => 335,
        ];
        $yearMonthOffsets = [];

        $idx = 0;
        for ($y = 2020; $y <= 2026; $y++) {
            $yearOffsets[$y] = $idx;
            $yearMonthOffsets[$y] = ($y % 4 === 0) ? $monthOffsetsLeap : $monthOffsets;
            for ($m = 1; $m <= 12; $m++) {
                $days = match ($m) {
                    2 => ($y % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                for ($d = 1; $d <= $days; $d++) {
                    $names[$idx] = $y . '-' . ($m < 10 ? '0' : '') . $m . '-' . ($d < 10 ? '0' : '') . $d;
                    $idx++;
                }
            }
        }

        return [$names, $yearOffsets, $yearMonthOffsets, $idx];
    }

    private function scanPathNames(string $inputPath, int $fileSize): array
    {
        $scanSize = min($fileSize, 8 * 1024 * 1024);
        $fp = fopen($inputPath, 'r');
        stream_set_read_buffer($fp, 0);
        $chunk = fread($fp, $scanSize);
        fclose($fp);

        $lastNl = strrpos($chunk, "\n");
        $map = [];
        $names = [];
        $idx = 0;
        $pos = 0;

        while ($pos < $lastNl) {
            $nlPos = strpos($chunk, "\n", $pos + 45);
            if ($nlPos === false) break;

            $path = substr($chunk, $pos + 19, $nlPos - $pos - 45);
            if (!isset($map[$path])) {
                $map[$path] = $idx;
                $names[$idx] = $path;
                $idx++;
            }

            $pos = $nlPos + 1;
        }

        return [$map, $names, $idx];
    }

    private function processChunk(
        string $filePath,
        int $start,
        int $end,
        array $pathMap,
        array $yearOffsets,
        array $yearMonthOffsets,
        int $numPaths,
        int $numDates
    ): array {
        $counts = array_fill(0, $numPaths * $numDates, 0);
        $fp = fopen($filePath, 'rb');
        fseek($fp, $start);
        stream_set_read_buffer($fp, 0);

        $bufferSize = 8 * 1024 * 1024;
        $remaining = $end - $start;
        $leftover = '';

        while ($remaining > 0) {
            $readSize = min($bufferSize, $remaining);
            $raw = fread($fp, $readSize);
            if ($raw === false || $raw === '') {
                break;
            }
            $remaining -= strlen($raw);
            $rawLen = strlen($raw);
            $pos = 0;

            if ($leftover !== '') {
                $firstNl = strpos($raw, "\n");
                if ($firstNl === false) {
                    $leftover .= $raw;
                    continue;
                }
                $line = $leftover . substr($raw, 0, $firstNl);
                $lineLen = strlen($line);
                if ($lineLen > 45) {
                    $pathStr = substr($line, 19, $lineLen - 45);
                    $pIdx = $pathMap[$pathStr] ?? -1;
                    if ($pIdx >= 0) {
                        $datePos = $lineLen - 25;
                        $y = (ord($line[$datePos]) - 48) * 1000
                            + (ord($line[$datePos + 1]) - 48) * 100
                            + (ord($line[$datePos + 2]) - 48) * 10
                            + (ord($line[$datePos + 3]) - 48);
                        $yOffset = $yearOffsets[$y] ?? -1;
                        if ($yOffset >= 0) {
                            $m = (ord($line[$datePos + 5]) - 48) * 10 + (ord($line[$datePos + 6]) - 48);
                            $d = (ord($line[$datePos + 8]) - 48) * 10 + (ord($line[$datePos + 9]) - 48);
                            $monthOffset = $yearMonthOffsets[$y][$m];
                            $dIdx = $yOffset + $monthOffset + $d - 1;
                            $counts[$pIdx * $numDates + $dIdx]++;
                        }
                    }
                }
                $leftover = '';
                $pos = $firstNl + 1;
            }

            $lastNl = strrpos($raw, "\n");
            if ($lastNl === false || $lastNl < $pos) {
                $leftover = ($pos === 0) ? $raw : substr($raw, $pos);
                continue;
            }
            $leftover = ($lastNl < $rawLen - 1) ? substr($raw, $lastNl + 1) : '';

            while ($pos < $lastNl) {
                $nlPos = strpos($raw, "\n", $pos + 45);
                if ($nlPos === false) break;

                $pathStr = substr($raw, $pos + 19, $nlPos - $pos - 45);
                $pIdx = $pathMap[$pathStr] ?? -1;
                if ($pIdx >= 0) {
                    $datePos = $nlPos - 25;
                    $y = (ord($raw[$datePos]) - 48) * 1000
                        + (ord($raw[$datePos + 1]) - 48) * 100
                        + (ord($raw[$datePos + 2]) - 48) * 10
                        + (ord($raw[$datePos + 3]) - 48);
                    $yOffset = $yearOffsets[$y] ?? -1;
                    if ($yOffset >= 0) {
                        $m = (ord($raw[$datePos + 5]) - 48) * 10 + (ord($raw[$datePos + 6]) - 48);
                        $d = (ord($raw[$datePos + 8]) - 48) * 10 + (ord($raw[$datePos + 9]) - 48);
                        $monthOffset = $yearMonthOffsets[$y][$m];
                        $dIdx = $yOffset + $monthOffset + $d - 1;
                        $counts[$pIdx * $numDates + $dIdx]++;
                    }
                }

                $pos = $nlPos + 1;
            }
        }

        if ($leftover !== '' && strlen($leftover) > 45) {
            $len = strlen($leftover);
            $pathStr = substr($leftover, 19, $len - 45);
            $pIdx = $pathMap[$pathStr] ?? -1;
            if ($pIdx >= 0) {
                $datePos = $len - 25;
                $y = (ord($leftover[$datePos]) - 48) * 1000
                    + (ord($leftover[$datePos + 1]) - 48) * 100
                    + (ord($leftover[$datePos + 2]) - 48) * 10
                    + (ord($leftover[$datePos + 3]) - 48);
                $yOffset = $yearOffsets[$y] ?? -1;
                if ($yOffset >= 0) {
                    $m = (ord($leftover[$datePos + 5]) - 48) * 10 + (ord($leftover[$datePos + 6]) - 48);
                    $d = (ord($leftover[$datePos + 8]) - 48) * 10 + (ord($leftover[$datePos + 9]) - 48);
                    $monthOffset = $yearMonthOffsets[$y][$m];
                    $dIdx = $yOffset + $monthOffset + $d - 1;
                    $counts[$pIdx * $numDates + $dIdx]++;
                }
            }
        }

        fclose($fp);
        return $counts;
    }

    private function getWorkerCount(int $fileSize): int
    {
        $singleProcessThreshold = 64 * 1024 * 1024;
        if ($fileSize <= $singleProcessThreshold) {
            return 1;
        }
        return 2;
    }

    private function tryShmopOpen(int $key, string $flags, int $mode, int $size)
    {
        $handler = set_error_handler(static function () {
            return true;
        });
        try {
            return shmop_open($key, $flags, $mode, $size);
        } finally {
            if ($handler !== null) {
                restore_error_handler();
            }
        }
    }

    private function buildPathJson(array $pathNames): array
    {
        $pathJson = [];
        foreach ($pathNames as $idx => $path) {
            $pathJson[$idx] = '"' . str_replace('/', '\\/', $path) . '"';
        }
        return $pathJson;
    }
}
