<?php

namespace App;

use Fidry\CpuCoreCounter\CpuCoreCounter;

final class Parser
{
    private int $workers;

    public function __construct()
    {
        $this->workers = (new CpuCoreCounter())->getCount();
    }

    public function parse(string $inputPath, string $outputPath): void
    {
        $data = $this->parseParallel($inputPath);

        // Sort the data for each URL (SORT_STRING is faster for ISO dates)
        foreach ($data as &$urlData) {
            \ksort($urlData, SORT_STRING);
        }

        \file_put_contents($outputPath, \json_encode($data, JSON_PRETTY_PRINT));
    }

    private function parseParallel(string $inputPath): array
    {
        $fileSize = \filesize($inputPath);
        $chunkSize = (int) \ceil($fileSize / $this->workers);
        $offsets = $this->calculateChunkOffsets($inputPath, $chunkSize);

        $pids = [];
        $tempFiles = [];

        for ($i = 0; $i < $this->workers; $i++) {
            $tempFiles[$i] = \sys_get_temp_dir() . "/parser_chunk_{$i}_" . \getmypid() . ".dat";

            $pid = \pcntl_fork();

            if ($pid === -1) {
                throw new \RuntimeException('Failed to fork worker process');
            } elseif ($pid === 0) {
                // Child: parse chunk and write result
                $data = $this->parseChunk($inputPath, $offsets[$i], $offsets[$i + 1]);
                \file_put_contents($tempFiles[$i], \igbinary_serialize($data));
                exit(0);
            } else {
                $pids[$i] = $pid;
            }
        }

        // Wait for all workers
        foreach ($pids as $pid) {
            \pcntl_waitpid($pid, $status);
        }

        return $this->mergeResults($tempFiles);
    }

    private function calculateChunkOffsets(string $inputPath, int $chunkSize): array
    {
        $handle = \fopen($inputPath, 'r');
        $fileSize = \filesize($inputPath);
        $offsets = [0];

        for ($i = 1; $i < $this->workers; $i++) {
            \fseek($handle, $chunkSize * $i);
            \fgets($handle); // Align to line boundary
            $offsets[] = \ftell($handle);
        }
        $offsets[] = $fileSize;

        \fclose($handle);

        return $offsets;
    }

    private function parseChunk(string $inputPath, int $startOffset, int $endOffset): array
    {
        $data = [];
        $handle = \fopen($inputPath, 'r');
        \fseek($handle, $startOffset);

        $chunkSize = $endOffset - $startOffset;
        $bytesRead = 0;

        while ($bytesRead < $chunkSize && ($line = \fgets($handle)) !== false) {
            $bytesRead += \strlen($line);

            [$url, $date] = \explode(',', $line, 2);
            $url = \substr($url, 19); // Remove 'https://stitcher.io'
            $date = \substr($date, 0, 10); // Keep 'YYYY-MM-DD'

            $data[$url][$date] ??= 0;
            $data[$url][$date]++;
        }

        \fclose($handle);

        return $data;
    }

    private function mergeResults(array $tempFiles): array
    {
        $data = [];

        foreach ($tempFiles as $tempFile) {
            $chunk = \igbinary_unserialize(\file_get_contents($tempFile));

            foreach ($chunk as $url => $dates) {
                if (!isset($data[$url])) {
                    $data[$url] = $dates;
                    continue;
                }

                foreach ($dates as $date => $count) {
                    $data[$url][$date] = ($data[$url][$date] ?? 0) + $count;
                }
            }

            \unlink($tempFile);
        }

        return $data;
    }
}
