<?php

namespace App;

final class Parser
{
    private const NUM_WORKERS = 4;

    public function parse(string $inputPath, string $outputPath): void
    {
        $startTime = microtime(true);

        $fileSize = filesize($inputPath);
        $chunkSize = (int) ceil($fileSize / self::NUM_WORKERS);

        $pids = [];
        $pipes = [];

        for ($i = 0; $i < self::NUM_WORKERS; $i++) {
            $startByte = $i * $chunkSize;
            $endByte = ($i + 1) * $chunkSize - 1;

            $pipes[$i] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            if ($pipes[$i] === false) {
                die("pipe failed");
            }

            stream_set_chunk_size($pipes[$i][0], 1024 * 1024);
            stream_set_chunk_size($pipes[$i][1], 1024 * 1024);

            $pid = pcntl_fork();
            if ($pid === -1) {
                die("fork failed");
            } elseif ($pid === 0) {
                fclose($pipes[$i][0]);
                $result = $this->processChunk($inputPath, $startByte, $endByte);
                $serialized = serialize($result);
                fwrite($pipes[$i][1], $serialized);
                fclose($pipes[$i][1]);
                exit(0);
            } else {
                $pids[$i] = $pid;
                fclose($pipes[$i][1]);
            }
        }

        $mergedData = [];
        $mergedOrder = [];
        $workerTimings = [];

        for ($i = 0; $i < self::NUM_WORKERS; $i++) {
            $data = '';
            while (!feof($pipes[$i][0])) {
                $data .= fread($pipes[$i][0], 8192);
            }
            fclose($pipes[$i][0]);
            pcntl_waitpid($pids[$i], $status);

            $result = unserialize($data);
            $workerTimings[$i] = $result['timing'];

            foreach ($result['order'] as $path) {
                if (!isset($mergedData[$path])) {
                    $mergedData[$path] = [];
                    $mergedOrder[] = $path;
                }
                foreach ($result['data'][$path] as $date => $count) {
                    if ($count > 0) {
                        if (!isset($mergedData[$path][$date])) {
                            $mergedData[$path][$date] = $count;
                        } else {
                            $mergedData[$path][$date] += $count;
                        }
                    }
                }
            }
        }

        foreach ($mergedData as $path => &$dates) {
            ksort($dates, SORT_STRING);
        }

        $sortTime = microtime(true);

        $output = [];
        foreach ($mergedOrder as $path) {
            $output[$path] = $mergedData[$path];
        }

        $json = json_encode($output, JSON_PRETTY_PRINT);
        file_put_contents($outputPath, $json);

        $endTime = microtime(true);
        $peakMemory = memory_get_peak_usage(true);

        $totalTime = $endTime - $startTime;

        $totalLines = 0;
        foreach ($workerTimings as $t) {
            $totalLines += $t['lines'];
        }

        echo "\n=== Parser Performance ===" . PHP_EOL;
        echo "Total time:     " . number_format($totalTime, 3) . "s" . PHP_EOL;
        echo "Total lines:    " . number_format($totalLines) . PHP_EOL;
        echo "Peak memory:    " . number_format($peakMemory / 1024 / 1024, 2) . " MB" . PHP_EOL;
        echo "Worker times:" . PHP_EOL;
        for ($i = 0; $i < self::NUM_WORKERS; $i++) {
            $t = $workerTimings[$i];
            echo "  Worker $i:     " . number_format($t['total'], 3) . "s (" . number_format($t['lines']) . " lines, fileRead: " . number_format($t['fileRead'], 3) . "s, fileSeek: " . number_format($t['fileSeek'], 3) . "s, template: " . number_format($t['template'], 3) . "s, path: " . number_format($t['path'], 3) . "s, key: " . number_format($t['key'], 3) . "s, agg: " . number_format($t['agg'], 3) . "s, filter: " . number_format($t['filter'], 3) . "s)" . PHP_EOL;
        }
        echo "===========================" . PHP_EOL;
    }

    private function processChunk(string $inputPath, int $startByte, int $endByte): array
    {
        $startTime = microtime(true);

        $order = [];

        $t0 = microtime(true);
        $dateTemplate = [];
        for ($year = 2021; $year <= 2026; $year++) {
            for ($month = 1; $month <= 12; $month++) {
                for ($day = 1; $day <= 31; $day++) {
                    $dateStr = sprintf('%04d-%02d-%02d', $year, $month, $day);
                    $dateTemplate[$dateStr] = 0;
                }
            }
        }
        $templateTime = microtime(true) - $t0;

        $pathExtractTime = 0;
        $keyExtractTime = 0;
        $aggTime = 0;
        $linesProcessed = 0;
        $fileReadTime = 0;
        $fileSeekTime = 0;
        $bytesProcessed = 0;

        $allPathsFound = false;
        $linesSinceLastNew = 0;

        // $tFile = microtime(true);
        $handle = fopen($inputPath, 'r');
        // $fileReadTime += microtime(true) - $tFile;

        // $tFile = microtime(true);
        fseek($handle, $startByte);
        // $fileSeekTime += microtime(true) - $tFile;

        // ignore partial lines, as they will have been picked up by the previous worker.
        if ($startByte > 0) {
            // $tFile = microtime(true);
            $line = fgets($handle);
            $lineLen = strlen($line);
            $bytesProcessed += $lineLen;
            // $fileReadTime += microtime(true) - $tFile;
        }

        while ($bytesProcessed < ($endByte - $startByte)) {
            // $tFile = microtime(true);
            $line = fgets($handle);
            // $fileReadTime += microtime(true) - $tFile;

            if ($line === false) {
                break;
            }

            $lineLen = strlen($line);
            $bytesProcessed += $lineLen;

            if ($lineLen < 30) {
                continue;
            }

            $linesProcessed++;

            // $t1 = microtime(true);
            $path = substr($line, 19, $lineLen - 46);
            // $pathExtractTime += microtime(true) - $t1;

            // $t2 = microtime(true);
            $date = substr($line, $lineLen - 26, 10);
            // $keyExtractTime += microtime(true) - $t2;

            // $t3 = microtime(true);
            if (!$allPathsFound) {
                if (!isset($$path)) {
                    $$path = $dateTemplate;
                    $order[] = $path;
                    $linesSinceLastNew = 0;
                } else {
                    $linesSinceLastNew++;
                }
                if ($linesSinceLastNew > 2000) {
                    $allPathsFound = true;
                }
            }
            $$path[$date]++;
            // $aggTime += microtime(true) - $t3;
        }

        fclose($handle);

        $t4 = microtime(true);
        $filteredData = [];
        foreach ($order as $path) {
            foreach ($$path as $date => $count) {
                if ($count > 0) {
                    $filteredData[$path][$date] = $count;
                }
            }
        }
        $filterTime = microtime(true) - $t4;

        $endTime = microtime(true);

        return [
            'data' => $filteredData,
            'order' => $order,
            'timing' => [
                'total' => $endTime - $startTime,
                'template' => $templateTime,
                'path' => $pathExtractTime,
                'key' => $keyExtractTime,
                'agg' => $aggTime,
                'filter' => $filterTime,
                'lines' => $linesProcessed,
                'fileRead' => $fileReadTime,
                'fileSeek' => $fileSeekTime
            ]
        ];
    }
}
