<?php

namespace App\Traits;

use function substr;
use function strpos;
use function strlen;
use const SEEK_CUR;

trait LoaderTokenizedSocketV1Trait {
    private function load(): array
    {
        $chunks = $this->calculateChunkBoundaries();
        $workerCount = count($chunks);  // Derived from self::WORKER_COUNT

        // Setup socket pairs before forking from parent process
        $socketPairs = [];
        for ($i = 0; $i < $workerCount - 1; $i++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            if ($pair ===  false) {
                throw new \RuntimeException("stream_socket_pair failed for worker $i");
            }
            $socketPairs[$i] = $pair; // [0] parent reads, [1] child writes
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

                // Close parent-read portion of all pairs
                foreach ($socketPairs as $i => $pair) {
                    fclose($pair[0]);
                }

                $this->work($start, $end, $index, $socketPairs[$index][1]);

                fclose($socketPairs[$index][1]);
                exit(0);
            }

            // Parent process
            fclose($socketPairs[$index][1]);    // Close child-write portion of workers pair
            $pids[$index] = $pid;
        }

        // Parent process the last chunk while children run in parallel
        [$lastStart, $lastEnd] = $chunks[$lastIndex];
        $parentCounts = $this->work($lastStart, $lastEnd, $lastIndex, null);

        // Async merge loop
        $accumulator = $this->makeAccumulator();

        foreach ($parentCounts as $i => $count) {
            $accumulator[$i] += $count;
        }

        $readSockets = [];
        $socketBuffers = [];
        $closed = [];

        foreach ($socketPairs as $i => $pair) {
            stream_set_blocking($pair[0], false);
            $readSockets[$i] = $pair[0];
            $socketBuffers[$i] = '';
            $closed[$i] = false;
        }

        $closedCount = 0;

        while ($closedCount < $workerCount) {
            $readSet = [];
            foreach ($readSockets as $i => $socket) {
                if (!$closed[$i]) $readSet[$i] = $socket;
            }

            if (empty($readSet)) break;

            $write = null;
            $except = null;
            $ready = $readSet;

            if (stream_select($ready, $write, $except, 0, 50_000) < 1) {
                continue;
            }

            foreach ($ready as $socket) {
                $i = array_search($socket, $readSockets, strict: true);
                if ($i === false) continue;

                $chunk = fread($socket, 65536);

                if ($chunk === false || $chunk === '') {
                    if (feof($socket)) {
                        // Drain any bytes that didn't form a complete 12-byte record
                        if ($socketBuffers[$i] !== '') {
                            $this->mergeBinaryBatch($socketBuffers[$i], $accumulator);
                            $socketBuffers[$i] = '';
                        }
                        fclose($socket);
                        $closed[$i] = true;
                        $closedCount++;
                    }
                    continue;
                }

                $socketBuffers[$i] .= $chunk;
            }
        }

        // All sockets closed
        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        return $accumulator;
    }

    private function mergeBinaryBatch(string $raw, array &$accumulator): void
    {
        if ($raw === '') return;

        $isV16 = (ord($raw[0]) === 0);
        $fmt = $isV16 ? 'v*' : 'V*';
        $j = 0;

        foreach (unpack($fmt, $raw, 1) as $v) {
            $accumulator[$j++] += $v;
        }
    }

    private function makeAccumulator(): array
    {
        return array_fill(0, $this->urlCount * $this->dateCount, 0);
    }

    private function calculateChunkBoundaries(): array
    {
        $t0 = hrtime(true);

        $fileSize = filesize($this->inputPath);
        $chunkSize = (int) ceil($fileSize / self::WORKER_COUNT);
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

        printf("Chunk Calculation took %.2fms\n", (hrtime(true) - $t0) / 1e6);

        return $chunks;
    }
}