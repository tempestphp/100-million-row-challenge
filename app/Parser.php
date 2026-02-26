<?php

namespace App;

final class Parser
{
    private const NUM_WORKERS = 8;

    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);

        if ($fileSize < 1024 * 1024 * 2 || !function_exists('pcntl_fork')) {
            $resultWithOrder = $this->processChunk($inputPath, 0, $fileSize, 0);
            $this->saveResult($resultWithOrder, $outputPath);
            return;
        }

        $chunkSize = (int) ceil($fileSize / self::NUM_WORKERS);
        $chunks = [];
        $fp = fopen($inputPath, 'r');
        $start = 0;

        for ($i = 0; $i < self::NUM_WORKERS; $i++) {
            $end = $start + $chunkSize;
            if ($end >= $fileSize) {
                $end = $fileSize;
                $chunks[] = [$start, $end];
                break;
            }
            fseek($fp, $end);
            fgets($fp);
            $end = ftell($fp);
            $chunks[] = [$start, $end];
            $start = $end;
        }
        fclose($fp);

        $tempDir = __DIR__ . '/../var/tmp_parser';
        if (!is_dir($tempDir)) {
            @mkdir($tempDir, 0777, true);
        }

        $pids = [];
        foreach ($chunks as $index => $chunk) {
            $pid = pcntl_fork();
            if ($pid === -1) {
                die("Could not fork");
            } elseif ($pid === 0) {
                // To maintain global ordering, we pass a large base order for each chunk
                $resultWithOrder = $this->processChunk($inputPath, $chunk[0], $chunk[1], $index * 1000000000);
                $tempFile = $tempDir . '/worker_' . $index . '.json';
                file_put_contents($tempFile, json_encode($resultWithOrder));
                exit(0);
            } else {
                $pids[] = $pid;
            }
        }

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $finalResult = [];
        for ($i = 0; $i < count($chunks); $i++) {
            $tempFile = $tempDir . '/worker_' . $i . '.json';
            if (file_exists($tempFile)) {
                $workerData = json_decode(file_get_contents($tempFile), true);

                foreach ($workerData as $path => $data) {
                    $order = $data['order'];
                    $counts = $data['counts'];

                    if (!isset($finalResult[$path])) {
                        $finalResult[$path] = ['order' => $order, 'counts' => $counts];
                    } else {
                        if ($order < $finalResult[$path]['order']) {
                            $finalResult[$path]['order'] = $order;
                        }
                        foreach ($counts as $date => $count) {
                            if (!isset($finalResult[$path]['counts'][$date])) {
                                $finalResult[$path]['counts'][$date] = $count;
                            } else {
                                $finalResult[$path]['counts'][$date] += $count;
                            }
                        }
                    }
                }
                unlink($tempFile);
            }
        }

        $this->saveResult($finalResult, $outputPath);
    }

    private function processChunk(string $inputPath, int $start, int $end, int $orderBase): array
    {
        $fp = fopen($inputPath, 'rb');
        fseek($fp, $start);
        $result = [];
        $domainLen = 19; // "https://stitcher.io" length
        $orderCounter = 0;

        $bufferSize = 4 * 1024 * 1024; // 4MB
        $tail = '';

        while (ftell($fp) < $end && !feof($fp)) {
            $bytesToRead = min($bufferSize, $end - ftell($fp));
            $chunk = $tail . fread($fp, $bytesToRead);
            $offset = 0;
            $chunkLen = strlen($chunk);

            while (($nlPos = strpos($chunk, "\n", $offset)) !== false) {
                $lineLen = $nlPos - $offset;
                if ($lineLen > $domainLen) {
                    $commaPos = strpos($chunk, ",", $offset);
                    if ($commaPos !== false && $commaPos < $nlPos) {
                        $pathLen = $commaPos - ($offset + $domainLen);
                        $path = substr($chunk, $offset + $domainLen, $pathLen);
                        $date = substr($chunk, $commaPos + 1, 10);

                        if (!isset($result[$path])) {
                            $result[$path] = ['order' => $orderBase + $orderCounter, 'counts' => [$date => 1]];
                            $orderCounter++;
                        } else {
                            if (!isset($result[$path]['counts'][$date])) {
                                $result[$path]['counts'][$date] = 1;
                            } else {
                                $result[$path]['counts'][$date]++;
                            }
                        }
                    }
                }
                $offset = $nlPos + 1;
            }
            $tail = substr($chunk, $offset);
        }

        // Handle any remaining tail (if the chunk didn't end with a newline)
        if ($tail !== '') {
            $commaPos = strpos($tail, ",");
            if ($commaPos !== false) {
                $pathLen = $commaPos - $domainLen;
                $path = substr($tail, $domainLen, $pathLen);
                $date = substr($tail, $commaPos + 1, 10);

                if (!isset($result[$path])) {
                    $result[$path] = ['order' => $orderBase + $orderCounter, 'counts' => [$date => 1]];
                    $orderCounter++;
                } else {
                    if (!isset($result[$path]['counts'][$date])) {
                        $result[$path]['counts'][$date] = 1;
                    } else {
                        $result[$path]['counts'][$date]++;
                    }
                }
            }
        }

        fclose($fp);
        return $result;
    }

    private function saveResult(array $resultWithOrder, string $outputPath): void
    {
        // Sort paths by original appearance order
        uasort($resultWithOrder, function ($a, $b) {
            return $a['order'] <=> $b['order'];
        });

        $finalResult = [];
        foreach ($resultWithOrder as $path => $data) {
            $dates = $data['counts'];
            ksort($dates);
            $finalResult[$path] = $dates;
        }

        file_put_contents($outputPath, json_encode($finalResult, JSON_PRETTY_PRINT));
    }
}