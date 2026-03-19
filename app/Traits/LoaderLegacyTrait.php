<?php

namespace App\Traits;

trait LoaderLegacyTrait {

    private function load(): array
    {
        // Hoist any class variables
        $inputPath = $this->inputPath;
        $workerCount = self::WORKER_COUNT -1;

        // -- Shared Memory Layout ----
        $workerDataBytes = $this->urlCount * $this->dateCount;
        $workersPerSegment = max(1, (int) floor(self::SHM_MAX_SEGMENT_SIZE / $workerDataBytes));
        $segmentCount = (int) ceil($workerCount / $workersPerSegment);

        if ($segmentCount > self::SHM_MAX_SEGMENTS) {
            throw new \OverflowException("Need $segmentCount segments, but only "
                . self::SHM_MAX_SEGMENTS . " are allowed");
        }

        fprintf(STDERR,
            "SHM layout: %d worker(s), %d bytes each → %d worker(s)/segment, %d segment(s)\n",
            $workerCount, $workerDataBytes, $workersPerSegment, $segmentCount
        );

        // Allocate shared memory segments
        $segments = [];
        for ($s = 0; $s < $segmentCount; $s++) {
            $key = ftok(__FILE__, $s);
            $id = shmop_open($key, 'c', 0600, self::SHM_MAX_SEGMENT_SIZE);
            if ($id === false) {
                throw new \RuntimeException("shmop_open failed");
            }
            $segments[$s] = $id;
        }

        // Pre-computer workers segment and offset
        $shmLayout = [];
        for ($i = 0; $i < $workerCount;  $i++) {
            $shmLayout[$i] = [
                'seg' => (int) floor($i / $workersPerSegment),
                'offset' => ($i % $workersPerSegment) * $workerDataBytes,
            ];
        }

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
                $this->work(
                    $start, $end, $i,
                    $segments[$shmLayout[$i]['seg']],
                    $shmLayout[$i]['offset'],
                    $childSocket
                );
                exit(0);
            }

            // Parent process
            fclose($childSocket);
            $sockets[$i]    = $parentSocket;
            $pids[$i]       = $pid;
        }

        // Parent Calibration
        $parentScore = $this->calibrate(self::CALIBRATION_DUR * self::CALIBRATION_PARENT_FACTOR);

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
        $merged = $this->work(
            $parentBoundary['start'],
            $parentBoundary['end'],
            $workerCount,
            null, 0, null
        );

        // Phase 5: collect results from workers
        $socketMap = [];
        foreach ($sockets as $i => $socket) {
            $socketMap[(int)$socket] = $i;
        }

        $pending    = $sockets;

        while (!empty($pending)) {
            $read = $pending;
            $write = null;
            $except = null;

            stream_select($read, $write, $except, 0, 50_000);

            foreach ($read as $ready) {
                $i = $socketMap[(int)$ready];

                fread($ready, 1);

                $raw = shmop_read(
                    $segments[$shmLayout[$i]['seg']],
                    $shmLayout[$i]['offset'],
                    $workerDataBytes
                );

                foreach (array_values(unpack('C*', $raw)) as $j => $count) {
                    $merged[$j] += $count;
                }

                fclose($ready);
                unset($pending[array_search($ready, $pending)]);
            }
        }

        foreach ($segments as $seg) {
            shmop_delete($seg);
        }

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status, WNOHANG);
        }

        return $merged;
    }

    private function calibrate(int $windowMs = 50): int
    {
        $rowLen = 35 + 25 + 10;

        $syntheticWindow = str_repeat(
            str_repeat('x', 25) .
            str_repeat('y', $rowLen - 25 - 10 - 2) .
            "\t" .
            str_repeat('z', 25) . "\n",
            1000
        );

        $windowLen = strlen($syntheticWindow);
        $iterations = 0;
        $deadline = hrtime(true) + ($windowMs * 1e6);

        while (hrtime(true) < $deadline) {
            $wStart = 0;

            for ($i = 0; $i < 100; $i++) {
                $wEnd = strpos($syntheticWindow, "\n", $wStart + 35);
                if ($wEnd === false) { $wStart = 0; continue; }
                $url = substr($syntheticWindow, $wStart + 25,
                    $wEnd - $wStart - 25 - 25 - 1);
                $date = substr($syntheticWindow, $wEnd - 25, 25);
                $wStart = ($wEnd + 1) % ($windowLen - $rowLen);
                $iterations++;
            }
        }

        return $iterations;
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