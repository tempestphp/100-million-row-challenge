<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);

        if ($fileSize > 10_000_000 && function_exists('pcntl_fork')) {
            $this->parseParallel($inputPath, $outputPath, $fileSize);
            return;
        }

        $flat = $this->processChunk($inputPath, 0, $fileSize);
        $this->writeOutput($flat, $outputPath);
    }

    private function parseParallel(string $inputPath, string $outputPath, int $fileSize): void
    {
        $workers = 2;

        $points = [0];
        $h = fopen($inputPath, 'r');
        for ($i = 1; $i < $workers; $i++) {
            fseek($h, (int) ($fileSize * $i / $workers));
            fgets($h);
            $points[] = ftell($h);
        }
        $points[] = $fileSize;
        fclose($h);

        $tmps = [];
        $pids = [];

        for ($i = 1; $i < $workers; $i++) {
            $tmps[$i] = tempnam(sys_get_temp_dir(), 'p');
            $pid = pcntl_fork();

            if ($pid === -1) {
                $tmps[$i] = null;
                continue;
            }

            if ($pid === 0) {
                $d = $this->processChunk($inputPath, $points[$i], $points[$i + 1]);
                file_put_contents($tmps[$i], serialize($d));
                exit(0);
            }

            $pids[$i] = $pid;
        }

        $flat = $this->processChunk($inputPath, $points[0], $points[1]);

        foreach ($pids as $i => $pid) {
            pcntl_waitpid($pid, $status);
            $child = unserialize(file_get_contents($tmps[$i]));
            @unlink($tmps[$i]);

            foreach ($child as $key => $count) {
                if (isset($flat[$key])) {
                    $flat[$key] += $count;
                } else {
                    $flat[$key] = $count;
                }
            }
            unset($child);
        }

        foreach ($tmps as $i => $tmp) {
            if ($tmp === null) {
                $extra = $this->processChunk($inputPath, $points[$i], $points[$i + 1]);
                foreach ($extra as $key => $count) {
                    if (isset($flat[$key])) {
                        $flat[$key] += $count;
                    } else {
                        $flat[$key] = $count;
                    }
                }
            }
        }

        $this->writeOutput($flat, $outputPath);
    }

    private function processChunk(string $inputPath, int $start, int $end): array
    {
        $handle = fopen($inputPath, 'r');
        stream_set_read_buffer($handle, 0);

        if ($start > 0) {
            fseek($handle, $start);
        }

        $flat = [];
        $remaining = $end - $start;
        $leftover = '';

        while ($remaining > 0) {
            $raw = fread($handle, min(8388608, $remaining));
            if ($raw === false || $raw === '') {
                break;
            }
            $remaining -= strlen($raw);

            $off = 0;
            if ($leftover !== '') {
                $nl = strpos($raw, "\n");
                if ($nl !== false) {
                    $line = $leftover . substr($raw, 0, $nl);
                    $c = strpos($line, ',', 19);
                    if ($c !== false) {
                        $key = substr($line, 19, $c - 19 + 11);
                        if (isset($flat[$key])) {
                            $flat[$key]++;
                        } else {
                            $flat[$key] = 1;
                        }
                    }
                    $off = $nl + 1;
                    $leftover = '';
                } else {
                    $leftover .= $raw;
                    continue;
                }
            }

            $lastNl = strrpos($raw, "\n");
            if ($lastNl === false) {
                $leftover = $off > 0 ? substr($raw, $off) : $raw;
                continue;
            }

            $rawLen = strlen($raw);
            if ($lastNl + 1 < $rawLen) {
                $leftover = substr($raw, $lastNl + 1);
            }

            $len = $lastNl + 1;

            while ($off < $len) {
                $c = strpos($raw, ',', $off + 19);
                if ($c === false) {
                    break;
                }

                $key = substr($raw, $off + 19, $c - $off - 19 + 11);
                $off = $c + 27;

                if (isset($flat[$key])) {
                    $flat[$key]++;
                } else {
                    $flat[$key] = 1;
                }
            }
        }

        if ($leftover !== '') {
            $c = strpos($leftover, ',', 19);
            if ($c !== false) {
                $key = substr($leftover, 19, $c - 19 + 11);
                if (isset($flat[$key])) {
                    $flat[$key]++;
                } else {
                    $flat[$key] = 1;
                }
            }
        }

        fclose($handle);
        return $flat;
    }

    private function writeOutput(array $flat, string $outputPath): void
    {
        $data = [];
        foreach ($flat as $key => $count) {
            $data[substr($key, 0, -11)][substr($key, -10)] = $count;
        }

        foreach ($data as &$visits) {
            ksort($visits);
        }
        unset($visits);

        file_put_contents($outputPath, json_encode($data, JSON_PRETTY_PRINT));
    }
}
