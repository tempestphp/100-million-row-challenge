<?php

namespace App;

final class Parser
{
    private const NUM_WORKERS = 8;

    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);

        if ($fileSize < 1024 * 1024 * 2 || !function_exists('pcntl_fork')) {
            [$result, $order] = $this->processChunk($inputPath, 0, $fileSize);
            $this->writeOutput($result, $order, $outputPath);
            return;
        }

        $chunkSize = (int) ceil($fileSize / self::NUM_WORKERS);
        $chunks = [];
        $fp = fopen($inputPath, 'rb');
        $start = 0;

        for ($i = 0; $i < self::NUM_WORKERS; $i++) {
            $end = $start + $chunkSize;
            if ($end >= $fileSize) {
                $chunks[] = [$start, $fileSize];
                break;
            }
            fseek($fp, $end);
            fgets($fp);
            $end = ftell($fp);
            $chunks[] = [$start, $end];
            $start = $end;
        }
        fclose($fp);

        $tempDir = sys_get_temp_dir() . '/php_parser_' . getmypid();
        @mkdir($tempDir, 0777, true);

        $pids = [];
        foreach ($chunks as $index => $chunk) {
            $pid = pcntl_fork();
            if ($pid === -1) {
                die("Could not fork");
            } elseif ($pid === 0) {
                [$result, $order] = $this->processChunk($inputPath, $chunk[0], $chunk[1]);
                file_put_contents($tempDir . '/w' . $index, serialize([$result, $order]));
                exit(0);
            } else {
                $pids[] = $pid;
            }
        }

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        // Merge
        $finalResult = [];
        $finalOrder = [];

        $numChunks = count($chunks);
        for ($i = 0; $i < $numChunks; $i++) {
            $tempFile = $tempDir . '/w' . $i;
            [$workerResult, $workerOrder] = unserialize(file_get_contents($tempFile));
            unlink($tempFile);

            $orderOffset = $i * 1000000000;
            foreach ($workerOrder as $path => $localOrd) {
                $globalOrd = $orderOffset + $localOrd;
                if (!isset($finalOrder[$path]) || $globalOrd < $finalOrder[$path]) {
                    $finalOrder[$path] = $globalOrd;
                }
            }

            foreach ($workerResult as $path => $dates) {
                if (!isset($finalResult[$path])) {
                    $finalResult[$path] = $dates;
                } else {
                    foreach ($dates as $date => $count) {
                        if (isset($finalResult[$path][$date])) {
                            $finalResult[$path][$date] += $count;
                        } else {
                            $finalResult[$path][$date] = $count;
                        }
                    }
                }
            }
        }

        @rmdir($tempDir);
        $this->writeOutput($finalResult, $finalOrder, $outputPath);
    }

    private function processChunk(string $inputPath, int $start, int $end): array
    {
        $fp = fopen($inputPath, 'rb');
        stream_set_read_buffer($fp, 65536);
        fseek($fp, $start);

        $result = [];
        $order = [];
        $orderCounter = 0;
        $pos = $start;
        $bufferSize = 4194304; // 4MB
        $tail = '';

        while ($pos < $end) {
            $bytesToRead = $end - $pos;
            if ($bytesToRead > $bufferSize)
                $bytesToRead = $bufferSize;
            $raw = fread($fp, $bytesToRead);
            $pos += $bytesToRead;

            if ($tail !== '') {
                $chunk = $tail . $raw;
            } else {
                $chunk = $raw;
            }
            $offset = 0;

            while (($nlPos = strpos($chunk, "\n", $offset)) !== false) {
                // Line: https://stitcher.io/path/here,2026-01-24T01:16:58+00:00
                // Domain prefix is always 19 chars: "https://stitcher.io"
                $commaPos = strpos($chunk, ",", $offset + 20);
                if ($commaPos !== false && $commaPos < $nlPos) {
                    $path = substr($chunk, $offset + 19, $commaPos - $offset - 19);
                    $date = substr($chunk, $commaPos + 1, 10);

                    if (isset($result[$path])) {
                        if (isset($result[$path][$date])) {
                            $result[$path][$date]++;
                        } else {
                            $result[$path][$date] = 1;
                        }
                    } else {
                        $result[$path] = [$date => 1];
                        $order[$path] = $orderCounter++;
                    }
                }
                $offset = $nlPos + 1;
            }
            $tail = ($offset < strlen($chunk)) ? substr($chunk, $offset) : '';
        }

        // Handle remaining tail
        if ($tail !== '') {
            $commaPos = strpos($tail, ",", 19);
            if ($commaPos !== false) {
                $path = substr($tail, 19, $commaPos - 19);
                $date = substr($tail, $commaPos + 1, 10);
                if (isset($result[$path])) {
                    if (isset($result[$path][$date])) {
                        $result[$path][$date]++;
                    } else {
                        $result[$path][$date] = 1;
                    }
                } else {
                    $result[$path] = [$date => 1];
                    $order[$path] = $orderCounter++;
                }
            }
        }

        fclose($fp);
        return [$result, $order];
    }

    private function writeOutput(array $result, array $order, string $outputPath): void
    {
        // Sort paths by first-appearance order
        asort($order);

        // Sort dates within each path
        foreach ($result as &$dates) {
            ksort($dates);
        }
        unset($dates);

        // Build JSON output in memory, write once
        $out = "{\n";
        $first = true;
        foreach ($order as $path => $_) {
            if (!isset($result[$path]))
                continue;
            if (!$first) {
                $out .= ",\n";
            }
            $first = false;
            $escapedPath = str_replace('/', '\\/', $path);
            $out .= '    "' . $escapedPath . '": {' . "\n";
            $firstDate = true;
            foreach ($result[$path] as $date => $count) {
                if (!$firstDate) {
                    $out .= ",\n";
                }
                $firstDate = false;
                $out .= '        "' . $date . '": ' . $count;
            }
            $out .= "\n    }";
        }
        $out .= "\n}";
        file_put_contents($outputPath, $out);
    }
}