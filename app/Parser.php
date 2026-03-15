<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        ini_set('memory_limit', '4G');
        $fileSize = filesize($inputPath);
        $numWorkers = max(1, min(12, intdiv($fileSize, 10000)));
        $chunkSize = intdiv($fileSize, $numWorkers);

        $dir = dirname($outputPath);
        $pids = [];
        $tempFiles = [];

        for ($i = 0; $i < $numWorkers; $i++) {
            $start = $i * $chunkSize;
            $end = ($i === $numWorkers - 1) ? $fileSize : ($i + 1) * $chunkSize;
            $tempFile = "$dir/.chunk_$i.tmp";
            $tempFiles[] = $tempFile;

            $pid = pcntl_fork();

            if ($pid === 0) {
                ini_set('memory_limit', '1G');
                $this->processChunk($inputPath, $start, $end, $tempFile);
                exit(0);
            }

            $pids[] = $pid;
        }

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $this->mergeAndWrite($tempFiles, $outputPath);

        foreach ($tempFiles as $f) {
            @unlink($f);
        }
    }

    private function processChunk(string $inputPath, int $start, int $end, string $tempFile): void
    {
        $fp = fopen($inputPath, 'rb');

        if ($start > 0) {
            fseek($fp, $start);
            $start += strlen(fgets($fp));
        }

        $data = [];
        $order = [];
        $leftover = '';
        $pos = $start;

        while ($pos < $end) {
            $toRead = min(2097152, $end - $pos);
            $raw = fread($fp, $toRead);
            if ($raw === false || $raw === '') {
                break;
            }

            $pos += strlen($raw);

            $lines = explode("\n", $leftover . $raw);
            $leftover = array_pop($lines);

            foreach ($lines as $line) {
                $len = strlen($line);
                if ($len < 46) {
                    continue;
                }

                $p = substr($line, 19, $len - 45);
                $d = substr($line, $len - 25, 10);

                if (isset($data[$p][$d])) {
                    $data[$p][$d]++;
                } elseif (isset($data[$p])) {
                    $data[$p][$d] = 1;
                } else {
                    $data[$p] = [$d => 1];
                    $order[] = $p;
                }
            }
        }

        // Complete the last line straddling the chunk boundary
        if ($leftover !== '') {
            $rest = fgets($fp);
            if ($rest !== false) {
                $leftover .= $rest;
            }
            $len = strlen(rtrim($leftover, "\n"));
            if ($len >= 46) {
                $p = substr($leftover, 19, $len - 45);
                $d = substr($leftover, $len - 25, 10);

                if (isset($data[$p][$d])) {
                    $data[$p][$d]++;
                } elseif (isset($data[$p])) {
                    $data[$p][$d] = 1;
                } else {
                    $data[$p] = [$d => 1];
                    $order[] = $p;
                }
            }
        }

        fclose($fp);
        file_put_contents($tempFile, serialize([$order, $data]));
    }

    private function mergeAndWrite(array $tempFiles, string $outputPath): void
    {
        $merged = [];
        $pathOrder = [];
        $pathSeen = [];

        foreach ($tempFiles as $tempFile) {
            [$order, $data] = unserialize(file_get_contents($tempFile));

            foreach ($order as $path) {
                if (!isset($pathSeen[$path])) {
                    $pathSeen[$path] = true;
                    $pathOrder[] = $path;
                }
            }

            foreach ($data as $path => $dates) {
                if (!isset($merged[$path])) {
                    $merged[$path] = $dates;
                } else {
                    $ref = &$merged[$path];
                    foreach ($dates as $date => $count) {
                        if (isset($ref[$date])) {
                            $ref[$date] += $count;
                        } else {
                            $ref[$date] = $count;
                        }
                    }
                    unset($ref);
                }
            }
        }

        // Build result in original path order with sorted dates
        $result = [];
        foreach ($pathOrder as $path) {
            ksort($merged[$path]);
            $result[$path] = $merged[$path];
        }

        file_put_contents($outputPath, json_encode($result, JSON_PRETTY_PRINT));
    }
}
