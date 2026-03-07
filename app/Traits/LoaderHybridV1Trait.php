<?php

namespace App\Traits;

use function substr;
use function strpos;
use function strlen;
use const SEEK_CUR;

/**
 * Hybrid Goals
 * Take advantage of the calibration optimization sockets allow us via two-way communication
 * but also take advantage of the speed of dumping all of the results into a file as opposed
 * to the speed hit we take trying to read all the sockets from the processes all finishing
 * at the same time with a pure socket setup.
 */
trait LoaderHybridV1Trait {

    /**
     * Called from Parser, responsible for preparation of shared resources
     * and configuration for workers, spawning of workers, aggregation of
     * worker output. Returns everything for processing.
     */
    private function load(): array
    {
        // Hoist any class variables
        $inputPath = $this->inputPath;

        $workerCount = self::WORKER_COUNT -1;   // Parent thread takes the last slot
        $sockets = [];
        $pids = [];
        $workerFiles = [];

        // Phase 1: For all workers immediately and calibrate
        for ($i = 0; $i < $workerCount; $i++) {

            // Initialize a socket pair to communicate with child process for
            // calibration phase and work orders.
            $pair = stream_socket_pair(AF_UNIX, SOCK_STREAM, 0);
            [$parentSocket, $childSocket] = $pair;

            $resultsFile = sys_get_temp_dir() . '/worker_' . $i . '.bin';

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
                // Using a hot loop here to try and prevent the scheduler from moving us
                // to a different cpu core than the one we calibrated on. Since we cant
                // set cpu affinity, best case is to hold the cpu for the duration of the
                // process.
                stream_set_blocking($childSocket, false);
                $rangeData = '';

                while (strlen($rangeData) < 16) {
                    $chunk = fread($childSocket, 16 - strlen($rangeData));
                    if ($chunk !== false && $chunk !== '') {
                        $rangeData .= $chunk;
                    }
                }

                // We have received the boundaries of our assigned chunk
                ['start' => $start, 'end' => $end] = unpack('Pstart/Pend', $rangeData);
                fclose($childSocket);
                // -- End Hot Loop ----

                $this->work($start, $end, $i, $resultsFile);
                exit(0);
            }

            // Parent process
            fclose($childSocket);
            $sockets[$i]        = $parentSocket;
            $pids[$i]           = $pid;
            $workerFiles[$i]    = $resultsFile;
        }

        // Parent Calibration
        $parentScore = $this->calibrate(self::CALIBRATION_DUR);
        $parentScore *= 0.8;    // Parent load reduction to account for result accumulation

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

        // Phase 5: Wait for subprocesses to complete and collect results
        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status, WNOHANG);
        }

        $merged = $parentCounts;

        print("Starting with ". count($merged) ." counts\n");
        print_r($workerFiles);

        for ($i = 0; $i < $workerCount; $i++) {
            $path = $workerFiles[$i];
            echo "Loading $path\n";

            $raw = file_get_contents($path);

            print("Loaded ". strlen($raw) ." bytes from $path\n");

            $counts = array_values(unpack("V*", $raw));
            foreach ($counts as $j => $count) {
                $merged[$j] += $count;
            }
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