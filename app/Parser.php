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
                $finalOrder[$path] = min($finalOrder[$path] ?? PHP_INT_MAX, $globalOrd);
            }

            foreach ($workerResult as $path => $dates) {
                if (!isset($finalResult[$path])) {
                    $finalResult[$path] = $dates;
                } else {
                    foreach ($dates as $date => $count) {
                        $finalResult[$path][$date] = ($finalResult[$path][$date] ?? 0) + $count;
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
        $maxLineLen = 1024;

        while ($pos < $end && ($line = \stream_get_line($fp, $maxLineLen, "\n")) !== false) {
            $pos += \strlen($line) + 1;

            $commaPos = \strpos($line, ",", 20);
            if ($commaPos === false)
                continue;

            $path = \substr($line, 19, $commaPos - 19);
            $date = \substr($line, $commaPos + 1, 10);

            $order[$path] ??= $orderCounter++;
            $result[$path][$date] = ($result[$path][$date] ?? 0) + 1;
        }

        fclose($fp);

        // Pre-sort dates in worker so parent doesn't have to
        foreach ($result as &$dates) {
            ksort($dates);
        }
        unset($dates);

        return [$result, $order];
    }

    private function writeOutput(array $result, array $order, string $outputPath): void
    {
        // Sort paths by first-appearance order
        asort($order);

        // Build ordered result (dates already sorted by workers)
        $ordered = [];
        foreach ($order as $path => $_) {
            if (isset($result[$path]))
                $ordered[$path] = $result[$path];
        }

        file_put_contents($outputPath, json_encode($ordered, JSON_PRETTY_PRINT));
    }
}