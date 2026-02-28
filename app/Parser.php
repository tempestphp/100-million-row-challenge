<?php

namespace App;

final class Parser
{
    private const int NUM_WORKERS = 8;

    public function parse(string $inputPath, string $outputPath): void
    {
        ini_set('memory_limit', '-1');
        gc_disable();

        $fileSize = filesize($inputPath);

        if ($fileSize < 1024 * 1024 * 2 || !function_exists('pcntl_fork') || self::NUM_WORKERS === 1) {
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

        $tempDir = (is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir()) . '/php_parser_' . getmypid();
        @mkdir($tempDir, 0777, true);

        $useIgbinary = \function_exists('igbinary_serialize');

        $pids = [];
        foreach ($chunks as $index => $chunk) {
            $pid = pcntl_fork();
            if ($pid === -1) {
                die("Could not fork");
            } elseif ($pid === 0) {
                [$result, $order] = $this->processChunk($inputPath, $chunk[0], $chunk[1]);
                $tempFile = $tempDir . '/w' . $index;
                if ($useIgbinary) {
                    \file_put_contents($tempFile, \igbinary_serialize([$result, $order]));
                } else {
                    \file_put_contents($tempFile, \serialize([$result, $order]));
                }
                exit(0);
            } else {
                $pids[] = $pid;
            }
        }

        // Merge results as workers complete
        $finalResult = [];
        $finalOrder = [];

        $pidToIndex = \array_combine($pids, \array_keys($chunks));

        $numChunks = \count($chunks);
        for ($i = 0; $i < $numChunks; $i++) {
            $pid = \pcntl_wait($status);
            $workerIndex = $pidToIndex[$pid];

            $tempFile = $tempDir . '/w' . $workerIndex;
            $raw = \file_get_contents($tempFile);
            \unlink($tempFile);

            if ($useIgbinary) {
                [$workerResult, $workerOrder] = \igbinary_unserialize($raw);
            } else {
                [$workerResult, $workerOrder] = \unserialize($raw);
            }
            unset($raw);

            $orderOffset = $workerIndex * 1000000000;
            foreach ($workerOrder as $path => $localOrd) {
                $globalOrd = $orderOffset + $localOrd;
                if (!isset($finalOrder[$path]) || $globalOrd < $finalOrder[$path]) {
                    $finalOrder[$path] = $globalOrd;
                }
            }
            unset($workerOrder);

            foreach ($workerResult as $path => $dates) {
                if (!isset($finalResult[$path])) {
                    $finalResult[$path] = $dates;
                } else {
                    $existing = &$finalResult[$path];
                    $needsSort = false;
                    foreach ($dates as $date => $count) {
                        if (isset($existing[$date])) {
                            $existing[$date] += $count;
                        } else {
                            $existing[$date] = $count;
                            $needsSort = true;
                        }
                    }
                    if ($needsSort) {
                        \ksort($existing, SORT_NUMERIC);
                    }
                    unset($existing);
                }
            }
            unset($workerResult);
        }

        @rmdir($tempDir);

        // Dates are already fully sorted from the merge phase.

        $this->writeOutput($finalResult, $finalOrder, $outputPath);
    }

    private function processChunk(string $inputPath, int $start, int $end): array
    {
        $fp = \fopen($inputPath, 'rb');
        \fseek($fp, $start);

        $result = [];
        $order = [];
        $orderCounter = 0;
        $dateCache = [];

        $bytesRead = 0;
        $totalBytes = $end - $start;

        while (($line = \fgets($fp)) !== false) {
            $bytesRead += \strlen($line);

            $path = \substr($line, 19, -27); // 19 = strlen('https://stitcher.io')
            $dateStr = \substr($line, -26, 10);

            if (!isset($dateCache[$dateStr])) {
                $dateCache[$dateStr] = (int) \str_replace('-', '', $dateStr);
            }
            $dateInt = $dateCache[$dateStr];

            $order[$path] ??= ++$orderCounter;

            // Fast hash lookup
            if (isset($result[$path][$dateInt])) {
                $result[$path][$dateInt]++;
            } else {
                $result[$path][$dateInt] = 1;
            }

            if ($bytesRead >= $totalBytes) {
                break;
            }
        }

        \fclose($fp);

        // Pre-sort dates in worker (Numeric sort because keys are integers)
        foreach ($result as &$dates) {
            \ksort($dates, SORT_NUMERIC);
        }
        unset($dates);

        return [$result, $order];
    }

    private function writeOutput(array $result, array $order, string $outputPath): void
    {
        // Sort paths by first-appearance order
        \asort($order, SORT_NUMERIC);

        // Build ordered result (dates already sorted)
        $ordered = [];
        foreach (\array_keys($order) as $path) {
            if (isset($result[$path])) {
                $formattedDates = [];
                foreach ($result[$path] as $dateInt => $count) {
                    // Use math to extract YYYY, MM, DD
                    $d = $dateInt % 100;
                    $m = \intdiv($dateInt, 100) % 100;
                    $y = \intdiv($dateInt, 10000);

                    $formattedKey = \sprintf('%04d-%02d-%02d', $y, $m, $d);
                    $formattedDates[$formattedKey] = $count;
                }
                $ordered[$path] = $formattedDates;
            }
        }

        \file_put_contents($outputPath, \json_encode($ordered, JSON_PRETTY_PRINT));
    }
}
