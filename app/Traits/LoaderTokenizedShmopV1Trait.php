<?php

namespace App\Traits;

use function substr;
use function strpos;
use function strlen;
use const SEEK_CUR;

/**
 * While this showed promise locally, it seems the Mac Mini
 * system defaults set the max shared memory allocation far
 * too small to be useful for this application. Converting
 * this over to use /tmp/ files for communication instead
 */
trait LoaderTokenizedShmopV1Trait {
    private function load(): array
    {
        $chunks = $this->calculateChunkBoundaries();
        $workerCount = count($chunks);  // Derived from self::WORKER_COUNT

        // -- Setup shared memory ----

        // Round up to nearest 128 bytes to match M1 cache line size
        $rawSlice = $this->urlCount * $this->dateCount * 4;
        $sliceSize = (int) ceil($rawSlice/128) * 128;

        $totalSize = $workerCount * $sliceSize;

        $shmKey = ftok(__FILE__, 'p');

        if ($shm = @shmop_open($shmKey, 'a', 0644, 0)) {
            shmop_delete($shm);
            unset($shm);
        }

        $shm = shmop_open($shmKey, 'n', 0644, $totalSize);
        if ($shm === false) {
            throw new \RuntimeException("shmop_open failed");
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
                $this->work($start, $end, $index, $shm, $sliceSize);
                exit(0);
            }

            // Parent process
            $pids[$index] = $pid;
        }

        // Parent process the last chunk while children run in parallel
        [$lastStart, $lastEnd] = $chunks[$lastIndex];
        $this->work($lastStart, $lastEnd, $lastIndex, $shm, $sliceSize);

        // All forks closed
        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $accumulator = $this->makeAccumulator();

        for ($i = 0; $i < $workerCount; $i++) {
            $j = 0;
            foreach (unpack('V*', shmop_read($shm, $i * $sliceSize, $rawSlice)) as $v) {
                $accumulator[$j++] += $v;
            }
        }

        shmop_delete($shm);

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