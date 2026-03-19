<?php

namespace App\Traits;

use function substr;
use function strpos;
use function strlen;
use const SEEK_CUR;

/**
 * V2 Goals
 * - Calibration phase
 * - Calculate boundaries based on calibration scores
 * - Derive parent score
 *
 * Trying to account for the difference in processing speed between performance and economy
 * cores so that we're not sitting around waiting for the 4 economy cores because we assigned
 * all cores the same workload. We're taking a 50ms hit on overhead to assign work optimally.
 */
trait LoaderTokenizedSocketV2Trait {
    private function load(): array
    {
        // Hoist any class variables
        $inputPath = $this->inputPath;

        $workerCount = self::WORKER_COUNT -1;   // Remove this process acts as last worker
        $sockets = [];
        $pids = [];

        // Phase 1: For all workers immediately and calibrate
        for ($i = 0; $i < $workerCount; $i++) {
            $pair = stream_socket_pair(AF_UNIX, SOCK_STREAM, 0);
            [$parentSocket, $childSocket] = $pair;

            $pid = pcntl_fork();

            // Child process
            if ($pid === 0) {
                foreach ($sockets as $inheritedSocket) {
                    fclose($inheritedSocket);
                }
                fclose($parentSocket);

                // Calculate calibration score
                $score = $this->calibrate(self::CALIBRATION_DUR);
                fwrite($childSocket, pack('V', $score));

                // -- Hot Loop ----
                stream_set_blocking($childSocket, false);
                $rangeData = '';

                while (strlen($rangeData) < 16) {
                    $chunk = fread($childSocket, 16 - strlen($rangeData));
                    if ($chunk !== false && $chunk !== '') {
                        $rangeData .= $chunk;
                    }
                }

                stream_set_blocking($childSocket, true);
                // -- End Hot Loop ----

                ['start' => $start, 'end' => $end] = unpack('Pstart/Pend', $rangeData);
                $this->work($start, $end, $i, $childSocket);
                exit(0);
            }

            // Parent process
            fclose($childSocket);
            $sockets[$i]    = $parentSocket;
            $pids[$i]       = $pid;
        }

        // Parent Calibration
        $parentScore = $this->calibrate(self::CALIBRATION_DUR);
        $parentScore *= 0.6;    // Parent load reduction to account for result accumulation

        // Phase 2: collect all calibration scores
        $scores = [];
        for ($i = 0; $i < $workerCount; $i++) {
            $data = fread($sockets[$i], 4);
            $scores[$i] = unpack('V', $data)[1];
        }
        $scores[$workerCount] = $parentScore;  // Set the last score for the parent

        $totalScore = array_sum($scores);

        fprintf(STDERR, "Calibration: [%s] total=%d\n", implode(', ', $scores), $totalScore);

        // Phase 3: Calculate boundaries based on calibration scores
        $fileSize = filesize($inputPath);
        $handle = fopen($inputPath, 'rb', false);
        $boundaries = $this->calculateChunkBoundaries($handle, $fileSize, $scores, $totalScore);
        fclose($handle);

        // Phase 4: Assign work to workers based on boundaries
        for ($i = 0; $i < $workerCount; $i++) {
            fwrite($sockets[$i], pack('PP', $boundaries[$i]['start'], $boundaries[$i]['end']));
        }

        // Parent processes the last chunk while children run in parallel
        $parentBoundary = $boundaries[$workerCount];
        $parentCounts = $this->work(
            $parentBoundary['start'],
            $parentBoundary['end'],
            $workerCount,
            null
        );

        // Phase 5: collect results from workers
        $socketMap = [];
        foreach ($sockets as $i => $socket) {
            $socketMap[(int)$socket] = $i;
        }

        $merged     = $parentCounts;
        $pending    = $sockets;

        while (!empty($pending)) {
            $read = $pending;
            $write = null;
            $except = null;

            stream_select($read, $write, $except, 0, 50_000);

            foreach ($read as $ready) {
                $i = $socketMap[(int)$ready];

                $header = fread($ready, 1);
                $v16    = $header === "\x00";
                $fmt    = $v16 ? 'v*' : 'V*';
                $bytes  = $v16 ? 2 : 4;
                $total = $this->urlCount * $this->dateCount * $bytes;

                $raw = '';
                while (strlen($raw) < $total) {
                    $chunk = fread($ready, $total - strlen($raw));
                    if ($chunk === false || $chunk === '') break;
                    $raw .= $chunk;
                }

                $counts = array_values(unpack($fmt, $raw));
                foreach ($counts as $j => $count) {
                    $merged[$j] += $count;
                }

                fclose($ready);
                unset($pending[array_search($ready, $pending)]);
            }
        }

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status, WNOHANG);
        }

        return $merged;
    }

    private function calibrate(int $windowMs = 50): int
    {
        $rowLen = $this->minLineLength + self::DOMAIN_LENGTH + 10;

        $syntheticWindow = str_repeat(
            str_repeat('x', self::DOMAIN_LENGTH) .
            str_repeat('y', $rowLen - self::DOMAIN_LENGTH - self::DATE_WIDTH - 2) .
            "\t" .
            str_repeat('z', self::DATE_WIDTH) . "\n",
            1000
        );

        $windowLen = strlen($syntheticWindow);
        $iterations = 0;
        $deadline = hrtime(true) + ($windowMs * 1e6);

        while (hrtime(true) < $deadline) {
            $wStart = 0;

            for ($i = 0; $i < 100; $i++) {
                $wEnd = strpos($syntheticWindow, "\n", $wStart + $this->minLineLength);
                if ($wEnd === false) { $wStart = 0; continue; }
                $url = substr($syntheticWindow, $wStart + self::DOMAIN_LENGTH,
                    $wEnd - $wStart - self::DOMAIN_LENGTH - self::DATE_WIDTH - 1);
                $date = substr($syntheticWindow, $wEnd - self::DATE_WIDTH, self::DATE_WIDTH);
                $wStart = ($wEnd + 1) % ($windowLen - $rowLen);
                $iterations++;
            }
        }

        return $iterations;
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

    private function calculateChunkBoundaries(
        $handle,
        int $fileSize,
        array $scores,
        int $totalScore
    ): array {
        $t0 = hrtime(true);

        $boundaries = [];
        $cursor = 0;

        for ($i = 0; $i < count($scores); $i++) {
            $start = $cursor;

            // Early exit for last chunk
            if ($i === count($scores) - 1) {
                $boundaries[$i] = ['start' => $start, 'end' => $fileSize];
                break;
            }

            // Proportional end point
            $rawEnd = (int)($fileSize * (array_sum(array_slice($scores, 0, $i + 1)) / $totalScore));

            // Jump to next newline so we dont split mid-row
            fseek($handle, $rawEnd);
            $peek = fread($handle, 256);
            $nl = strpos($peek, "\n");

            $end = $nl !== false ? $rawEnd + $nl + 1 : $rawEnd;
            $cursor = $end;

            $boundaries[$i] = ['start' => $start, 'end' => $end];
        }

        printf("Chunk Calculation took %.2fms\n", (hrtime(true) - $t0) / 1e6);

        return $boundaries;
    }
}