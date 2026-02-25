<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);
        $numWorkers = $this->getWorkerCount();

        $splitPoints = [0];
        $fp = fopen($inputPath, 'r');
        for ($i = 1; $i < $numWorkers; $i++) {
            fseek($fp, (int)($fileSize * $i / $numWorkers));
            fgets($fp);
            $splitPoints[] = ftell($fp);
        }
        $splitPoints[] = $fileSize;
        fclose($fp);

        $tempDir = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
        $tempFiles = [];
        $pids = [];

        for ($w = 0; $w < $numWorkers; $w++) {
            $tempFiles[$w] = tempnam($tempDir, "p{$w}_");
            $pid = pcntl_fork();
            if ($pid === 0) {
                $data = $this->processChunk($inputPath, $splitPoints[$w], $splitPoints[$w + 1]);
                file_put_contents($tempFiles[$w], serialize($data));
                exit(0);
            }
            $pids[] = $pid;
        }

        $merged = null;
        for ($w = 0; $w < $numWorkers; $w++) {
            pcntl_waitpid($pids[$w], $s);
            $chunk = unserialize(file_get_contents($tempFiles[$w]));
            unlink($tempFiles[$w]);

            if ($w === 0) {
                $merged = $chunk;
                continue;
            }

            foreach ($chunk as $path => $dates) {
                if (isset($merged[$path])) {
                    foreach ($dates as $date => $count) {
                        if (isset($merged[$path][$date])) {
                            $merged[$path][$date] += $count;
                        } else {
                            $merged[$path][$date] = $count;
                        }
                    }
                } else {
                    $merged[$path] = $dates;
                }
            }
        }

        $parts = [];
        foreach ($merged as $path => &$dates) {
            ksort($dates);
            $dateLines = [];
            foreach ($dates as $date => $count) {
                $dateLines[] = "        \"$date\": $count";
            }
            $parts[] = '    ' . json_encode($path) . ": {\n" . implode(",\n", $dateLines) . "\n    }";
        }
        unset($dates);

        file_put_contents($outputPath, "{\n" . implode(",\n", $parts) . "\n}");
    }

    private function processChunk(string $filePath, int $start, int $end): array
    {
        $data = [];
        $fp = fopen($filePath, 'r');
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
                    $date = substr($line, $lineLen - 25, 10);
                    if (isset($data[$pathStr][$date])) {
                        $data[$pathStr][$date]++;
                    } else {
                        $data[$pathStr][$date] = 1;
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

            preg_match_all('/\.io([^,\n]+),(\d{4}-\d{2}-\d{2})\S+\n/', $raw, $m, 0, $pos);

            $paths = $m[1];
            $dates = $m[2];
            for ($i = 0, $c = count($paths); $i < $c; $i++) {
                if (isset($data[$paths[$i]][$dates[$i]])) {
                    $data[$paths[$i]][$dates[$i]]++;
                } else {
                    $data[$paths[$i]][$dates[$i]] = 1;
                }
            }
        }

        if ($leftover !== '' && strlen($leftover) > 45) {
            $len = strlen($leftover);
            $pathStr = substr($leftover, 19, $len - 45);
            $date = substr($leftover, $len - 25, 10);
            if (isset($data[$pathStr][$date])) {
                $data[$pathStr][$date]++;
            } else {
                $data[$pathStr][$date] = 1;
            }
        }

        fclose($fp);
        return $data;
    }

    private function getWorkerCount(): int
    {
        if (PHP_OS_FAMILY === 'Linux') {
            return max(3, (int)trim(shell_exec('nproc') ?? '3'));
        }
        if (PHP_OS_FAMILY === 'Darwin') {
            return max(3, (int)trim(shell_exec('sysctl -n hw.ncpu') ?? '3'));
        }
        return 3;
    }
}
