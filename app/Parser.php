<?php

namespace App;

use Fidry\CpuCoreCounter\CpuCoreCounter;

final class Parser
{
    private int $workers;

    public function __construct(
    ) {
        $this->workers = (new CpuCoreCounter())->getCount();
    }

    public function parse(string $inputPath, string $outputPath): void
    {
        // Parse input file in parallel
        $data = $this->parseParallel($inputPath);

        // Sort the data for each URL (SORT_STRING is faster for ISO dates)
        foreach ($data as &$urlData) {
            \ksort($urlData, SORT_STRING);
        }

        // Write data
        \file_put_contents($outputPath, \json_encode($data, JSON_PRETTY_PRINT));
    }

    private function parseParallel(string $inputPath): array
    {
        $fileSize = \filesize($inputPath);
        $chunkSize = (int) \ceil($fileSize / $this->workers);

        // Calculate offsets (aligned to line boundaries)
        $offsets = $this->calculateChunkOffsets($inputPath, $chunkSize);

        // Fork workers
        $pids = [];
        $tempFiles = [];

        for ($i = 0; $i < $this->workers; $i++) {
            $tempFile = \sys_get_temp_dir() . "/parser_chunk_{$i}_" . \getmypid() . ".dat";
            $tempFiles[$i] = $tempFile;

            $pid = \pcntl_fork();

            if ($pid === -1) {
                throw new \RuntimeException('Failed to fork worker process');
            } elseif ($pid === 0) {
                // Child process: parse chunk and write to temp file
                $data = $this->parseChunk($inputPath, $offsets[$i], $offsets[$i + 1]);
                \file_put_contents($tempFile, \serialize($data));
                exit(0);
            } else {
                $pids[$i] = $pid;
            }
        }

        // Wait for all workers to complete
        foreach ($pids as $pid) {
            \pcntl_waitpid($pid, $status);
        }

        // Merge results from all workers
        return $this->mergeResults($tempFiles);
    }

    private function calculateChunkOffsets(string $inputPath, int $chunkSize): array
    {
        $handle = \fopen($inputPath, 'r');
        $fileSize = \filesize($inputPath);
        $offsets = [0];

        for ($i = 1; $i < $this->workers; $i++) {
            \fseek($handle, $chunkSize * $i);
            \fgets($handle); // Move to end of current line
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

        while (\ftell($handle) < $endOffset && ($line = \fgets($handle)) !== false) {
            [$url, $date] = \explode(',', $line, 2);
            $url = \substr($url, 19);
            $date = \substr($date, 0, 10);

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
            $chunk = \unserialize(\file_get_contents($tempFile));

            foreach ($chunk as $url => $dates) {
                foreach ($dates as $date => $count) {
                    $data[$url][$date] ??= 0;
                    $data[$url][$date] += $count;
                }
            }

            \unlink($tempFile);
        }

        return $data;
    }
}
