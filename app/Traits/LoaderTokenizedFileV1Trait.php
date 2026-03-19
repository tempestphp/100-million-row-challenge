<?php

namespace App\Traits;

use function substr;
use function strpos;
use function strlen;
use const SEEK_CUR;

trait LoaderTokenizedFileV1Trait {
    private function load(): array
    {
        $chunks = $this->calculateChunkBoundaries();
        $workerCount = count($chunks);  // Derived from self::WORKER_COUNT

        // -- Setup temporary file ----
        $tmpDir = is_dir('/dev/shm') && is_writable('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
        $runId = uniqid('bg_loader_', true);

        $workerFiles = [];
        for ($i = 0; $i < $workerCount; $i++) {
            $workerFiles[$i] = "{$tmpDir}/{$runId}_worker$i.bin";
        }

        // Fork workers
        $pids = [];
        $lastIndex = $workerCount - 1;

        foreach ($chunks as $index => [$start, $end]) {
            if ($index === $lastIndex) break;

            $pid = pcntl_fork();

            if ($pid === -1) {
                throw new \RuntimeException("pcntl_fork failed for worker $index");
            }

            if ($pid === 0) {
                // Child process
                $this->work($start, $end, $index, $workerFiles[$index]);
                exit(0);
            }

            // Parent process
            $pids[$index] = $pid;
        }

        // Parent process the last chunk while children run in parallel
        [$lastStart, $lastEnd] = $chunks[$lastIndex];
        $this->work($lastStart, $lastEnd, $lastIndex, $workerFiles[$lastIndex]);

        // All forks closed
        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $rawSlice = $this->urlCount * $this->dateCount * 4;
        $accumulator = $this->makeAccumulator();

        for ($i = 0; $i < $workerCount; $i++) {
            $path = $workerFiles[$i];

            $data = file_get_contents($path);

            $j = 0;
            foreach (unpack('V*', substr($data, 0, $rawSlice)) as $v) {
                $accumulator[$j++] += $v;
            }
        }

        return $accumulator;
    }

    private function makeAccumulator(): array
    {
        return array_fill(0, $this->urlCount * $this->dateCount, 0);
    }

    private function calculateChunkBoundaries(): array
    {
        $fileSize = filesize($this->inputPath);
        $chunkSize = (int)ceil($fileSize / self::WORKER_COUNT);
        $chunks = [];
        $start = 0;
        $handle = fopen($this->inputPath, 'rb', false);

        for ($i = 0; $i < self::WORKER_COUNT; $i++) {
            if ($start >= $fileSize) break;

            if ($i === self::WORKER_COUNT - 1) {
                $chunks[] = [$start, $fileSize];
                break;
            }

            $end = min($start + $chunkSize, $fileSize);

            if ($end < $fileSize) {
                fseek($handle, $end);
                fgets($handle);
                $end = ftell($handle);
            }

            $chunks[] = [$start, $end];
            $start = $end;
        }

        fclose($handle);

        return $chunks;
    }
}