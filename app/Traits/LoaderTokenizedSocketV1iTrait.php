<?php

namespace App\Traits;

use function substr;
use function strpos;
use function strlen;
use function hrtime;
use const SEEK_CUR;

trait LoaderTokenizedSocketV1iTrait {
    private function load(): array
    {
        $chunks = $this->calculateChunkBoundaries();
        $workerCount = count($chunks);  // Derived from self::WORKER_COUNT

        // Setup socket pairs before forking from parent process
        $socketPairs = [];
        for ($i = 0; $i < $workerCount; $i++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            if ($pair ===  false) {
                throw new \RuntimeException("stream_socket_pair failed for worker $i");
            }
            $socketPairs[$i] = $pair; // [0] parent reads, [1] child writes
        }

        // Fork workers
        $pids = [];
        foreach ($chunks as $index => [$start, $end]) {
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

                $this->processChunk($start, $end, $index, $socketPairs[$index][1]);

                fclose($socketPairs[$index][1]);
                exit(0);
            }

            // Parent process
            fclose($socketPairs[$index][1]);    // Close child-write portion of workers pair
            $pids[$index] = $pid;
        }

        // Async merge loop
        $accumulator = $this->makeAccumulator();
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

            if (stream_select($ready, $write, $except, 50_000) < 1) {
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
        $step = $isV16 ? 16_384 : 32_768;
        $len = strlen($raw);
        $j = 0;

        for  ($off = 1; $off < $len; $off += $step) {
            foreach (unpack($fmt, substr($raw, $off, $step)) as $v) {
                $accumulator[$j++] += $v;
            }
        }
    }

    private function makeAccumulator(): array
    {
        return array_fill(0, $this->urlCount * $this->dateCount, 0);
    }

    private function calculateChunkBoundaries(): array
    {
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

        return $chunks;
    }

    private function processChunk(int $start, int $end, int $index, $writeSocket): void
    {
        $tChunkStart = hrtime(true);

        // Hoist these properties to prevent Zend engine hash lookups from $this within the hot path
        $urlTokens  = $this->urlTokens;
        $dateChars  = $this->dateChars;
        $minLineLen = $this->minLineLength;

        $tAlloc = hrtime(true);
        $buckets = array_fill(0, $this->urlCount, '');
        $tAllocMs = round((hrtime(true) - $tAlloc) / 1e6, 2);

        $handle = fopen($this->inputPath, 'rb', false);
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);

        $totalRowCount = 0;
        $tReadTotal = 0;

        $remaining = $end - $start;

        $tLoop = hrtime(true);

        while ($remaining > 0) {

            // -- Read one window ----
            $tRead = hrtime(true);
            $toRead = min($remaining, self::READ_BUFFER);
            $window = fread($handle, $toRead);
            $tReadTotal += hrtime(true) - $tRead;

            if ($window === false || $window === '') break;

            $windowLen = strlen($window);
            $lastNl = strrpos($window, "\n");

            if ($lastNl === false) {
                $remaining -= $windowLen;
                continue;
            }

            $tail = $windowLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
            }

            $remaining -= ($windowLen - $tail);
            $windowEnd = $lastNl;
            $wStart = 0;

            // 5x Unrolled fast path
            $fence = $windowEnd - 600;

            while ($wStart < $fence) {
                $wEnd = strpos($window, "\n", $wStart + $minLineLen);
                $buckets[$urlTokens[substr($window, $wStart + self::DOMAIN_LENGTH, $wEnd - $wStart - self::DOMAIN_LENGTH - self::DATE_WIDTH - 1)] ?? -1]
                    .= $dateChars[substr($window, $wEnd - self::DATE_WIDTH, self::DATE_LENGTH)] ?? '';
                $wStart = $wEnd + 1;

                $wEnd = strpos($window, "\n", $wStart + $minLineLen);
                $buckets[$urlTokens[substr($window, $wStart + self::DOMAIN_LENGTH, $wEnd - $wStart - self::DOMAIN_LENGTH - self::DATE_WIDTH - 1)] ?? -1]
                    .= $dateChars[substr($window, $wEnd - self::DATE_WIDTH, self::DATE_LENGTH)] ?? '';
                $wStart = $wEnd + 1;

                $wEnd = strpos($window, "\n", $wStart + $minLineLen);
                $buckets[$urlTokens[substr($window, $wStart + self::DOMAIN_LENGTH, $wEnd - $wStart - self::DOMAIN_LENGTH - self::DATE_WIDTH - 1)] ?? -1]
                    .= $dateChars[substr($window, $wEnd - self::DATE_WIDTH, self::DATE_LENGTH)] ?? '';
                $wStart = $wEnd + 1;

                $wEnd = strpos($window, "\n", $wStart + $minLineLen);
                $buckets[$urlTokens[substr($window, $wStart + self::DOMAIN_LENGTH, $wEnd - $wStart - self::DOMAIN_LENGTH - self::DATE_WIDTH - 1)] ?? -1]
                    .= $dateChars[substr($window, $wEnd - self::DATE_WIDTH, self::DATE_LENGTH)] ?? '';
                $wStart = $wEnd + 1;

                $wEnd = strpos($window, "\n", $wStart + $minLineLen);
                $buckets[$urlTokens[substr($window, $wStart + self::DOMAIN_LENGTH, $wEnd - $wStart - self::DOMAIN_LENGTH - self::DATE_WIDTH - 1)] ?? -1]
                    .= $dateChars[substr($window, $wEnd - self::DATE_WIDTH, self::DATE_LENGTH)] ?? '';
                $wStart = $wEnd + 1;

                $totalRowCount += 5;
            }

            // -- Cleanup loop for rows after the fence ----
            while ($wStart < $windowEnd) {
                $wEnd = strpos($window, "\n", $wStart + $minLineLen);

                if ($wEnd === false || $wEnd > $windowEnd) break;

                $urlToken = $urlTokens[substr($window, $wStart + self::DOMAIN_LENGTH, $wEnd - $wStart - self::DOMAIN_LENGTH - self::DATE_WIDTH - 1)] ?? null;
                $dateChar = $dateChars[substr($window, $wEnd - self::DATE_WIDTH, self::DATE_LENGTH)] ?? null;

                if ($urlToken !== null && $dateChar !== null) {
                    $buckets[$urlToken] .= $dateChar;
                }

                $wStart = $wEnd + 1;
                $totalRowCount++;
            }
        }

        fclose($handle);

        // Convert buckets to flat counts array
        $tConvert = hrtime(true);
        $counts = array_fill(0, $this->urlCount * $this->dateCount, 0);

        for ($s = 0; $s < $this->urlCount; $s++) {
            if ($buckets[$s] === '') continue;
            $base = $s * $this->dateCount;
            foreach (array_count_values(unpack('v*', $buckets[$s])) as $dateId => $count) {
                $counts[$base + $dateId] = $count;
            }
        }
        $tConvertMs = round((hrtime(true) - $tConvert) / 1e6, 2);

        // Send flat counts to parent via socket
        $tSend = hrtime(true);
        $maxVal = count($counts) > 0 ? max($counts) : 0;
        $v16 = $maxVal <= 65535;

        fwrite($writeSocket, $v16 ? "\x00" : "\x01");
        $fmt = $v16 ? 'v*' : 'V*';
        foreach (array_chunk($counts, 8192) as $batch) {
            fwrite($writeSocket, pack($fmt, ...$batch));
        }
        $tSendMs = round((hrtime(true) - $tSend) / 1e6, 2);

        // -- Diagnostics ----
        $tLoopMs  = round((hrtime(true) - $tLoop)  / 1e6, 2);
        $tTotalMs = round((hrtime(true) - $tChunkStart) / 1e6, 2);
        $tReadMs  = round($tReadTotal / 1e6, 2);

        $perRow = $totalRowCount > 0
            ? round(($tLoopMs / $totalRowCount) * 1000, 3)
            : 0;

        $report = implode(PHP_EOL, [
            "Child#$index ── $totalRowCount rows (" . ($v16 ? 'v16' : 'V32') . " IPC) ──────────────",
            "  Alloc:           {$tAllocMs}ms",
            "  Loop total:      {$tLoopMs}ms  ({$perRow}µs/row)",
            "    file read:     {$tReadMs}ms",
            "  Bucket convert:  {$tConvertMs}ms",
            "  IPC send:        {$tSendMs}ms",
            "  Chunk total:     {$tTotalMs}ms",
            "",
        ]);

        fwrite(STDERR, $report);
    }
}