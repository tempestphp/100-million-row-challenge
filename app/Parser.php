<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);
        $numProcs = 4; // Will work for 2 vCPUs
        $chunks = [];

        // Calculate chunk boundaries
        for ($i = 0; $i < $numProcs; $i++) {
            $start = $i === 0 ? 0 : $this->findLineBoundary($inputPath, (int)($fileSize * $i / $numProcs));
            $end = $i === $numProcs - 1 ? $fileSize : $this->findLineBoundary($inputPath, (int)($fileSize * ($i + 1) / $numProcs));
            $chunks[] = [$start, $end];
        }

        $pids = [];
        $shmIds = [];

        // Fork workers
        for ($i = 0; $i < $numProcs; $i++) {
            // Generate unique shared memory ID for each child
            $shmId = ftok(__FILE__, chr(65 + $i)); // 'A', 'B', 'C', 'D'
            $shmIds[] = $shmId;

            $pid = pcntl_fork();
            if ($pid === 0) {
                // Child process
                $data = $this->aggregate($inputPath, $chunks[$i][0], $chunks[$i][1]);
                $this->writeToShm($shmId, $data);
                exit(0);
            }
            $pids[] = $pid;
        }

        // Wait for all children and merge
        $data = [];
        foreach ($pids as $i => $pid) {
            pcntl_waitpid($pid, $status);
            $childData = $this->readFromShm($shmIds[$i]);
            $this->mergeInto($data, $childData);
        }

        // Pre-sort data once after merge to avoid sorting during write
        foreach ($data as &$dates) {
            ksort($dates);
        }
        unset($dates);

        $this->writeJson($outputPath, $data);
    }

    /**
     * Seek to $offset and advance to the next newline so we never split mid-row.
     */
    private function findLineBoundary(string $path, int $offset): int
    {
        $fh = fopen($path, 'rb');
        fseek($fh, $offset);
        fgets($fh);                 // discard partial line
        $pos = ftell($fh);
        fclose($fh);
        return $pos;
    }

    /**
     * Read rows in [$start, $end) and build the aggregation map.
     * Returns: [ '/path' => [ 'YYYY-MM-DD' => count, ... ], ... ]
     */
    private function aggregate(string $path, int $start, int $end): array
    {
        $data = [];
        $fh = fopen($path, 'rb');
        stream_set_read_buffer($fh, 16 * 1024 * 1024); // Increase to 16MB

        if ($start > 0) fseek($fh, $start);

        $bytesRead = $start;
        while ($bytesRead < $end && ($line = fgets($fh)) !== false) {
            $len = strlen($line);
            $bytesRead += $len;

            // Manual scan: find '/' after position 8, then ','
            $slashPos = false;
            $comma = false;

            for ($i = 8; $i < $len; $i++) {
                if (!$slashPos && $line[$i] === '/') {
                    $slashPos = $i;
                } elseif ($slashPos && $line[$i] === ',') {
                    $comma = $i;
                    break;
                }
            }

            if (!$slashPos || !$comma) continue;

            // Direct extraction without substr
            $urlPath = substr($line, $slashPos, $comma - $slashPos);
            $date = substr($line, $comma + 1, 10);

            $data[$urlPath][$date] = ($data[$urlPath][$date] ?? 0) + 1;
        }

        fclose($fh);
        return $data;
    }

    /**
     * Merge $source counts into $target in-place.
     */
    private function mergeInto(array &$target, array $source): void
    {
        foreach ($source as $urlPath => $dates) {
            if (!isset($target[$urlPath])) {
                $target[$urlPath] = $dates;
            } else {
                foreach ($dates as $date => $count) {
                    $target[$urlPath][$date] = ($target[$urlPath][$date] ?? 0) + $count;
                }
            }
        }
    }

    /**
     * Stream-write the nested JSON to avoid encoding a giant string in memory.
     */
    private function writeJson(string $outputPath, array $data): void
    {
        $fh      = fopen($outputPath, 'wb');
        $total   = count($data);
        $pathIdx = 0;

        $buffer = "{\n";
        $bufferSize = 2;
        $maxBuffer = 65536; // 64KB buffer

        foreach ($data as $urlPath => $dates) {
            $isLastPath = (++$pathIdx === $total);

            $urlJson = json_encode($urlPath);
            $lineLen = 4 + strlen($urlJson) + 3; // "    " + urlJson + ": {\n"
            $buffer .= '    ' . $urlJson . ": {\n";
            $bufferSize += $lineLen;

            $dateTotal = count($dates);
            $dateIdx   = 0;

            foreach ($dates as $date => $count) {
                $isLastDate = (++$dateIdx === $dateTotal);
                $line = '        ' . json_encode($date) . ': ' . $count . ($isLastDate ? '' : ',') . "\n";
                $buffer .= $line;
                $bufferSize += strlen($line);
                
                if ($bufferSize >= $maxBuffer) {
                    fwrite($fh, $buffer);
                    $buffer = '';
                    $bufferSize = 0;
                }
            }

            $line = '    }' . ($isLastPath ? '' : ',') . "\n";
            $buffer .= $line;
            $bufferSize += strlen($line);
            
            if ($bufferSize >= $maxBuffer) {
                fwrite($fh, $buffer);
                $buffer = '';
                $bufferSize = 0;
            }
        }

        $buffer .= "}";
        fwrite($fh, $buffer);
        fclose($fh);
    }

    /**
     * Write to shared memory
     * @param int $shmId
     * @param array $data
     * @return void
     */
    private function writeToShm(int $shmId, array $data): void
    {
        $serialized = igbinary_serialize($data);
        $size = strlen($serialized);

        // Write size first (8 bytes), then data
        $shm = shmop_open($shmId, 'c', 0644, $size + 8);
        shmop_write($shm, pack('J', $size), 0);
        shmop_write($shm, $serialized, 8);
    }

    /**
     * Read from shared memory
     * @param int $shmId
     * @return array
     */
    private function readFromShm(int $shmId): array
    {
        $shm = shmop_open($shmId, 'a', 0, 0);
        $size = unpack('J', shmop_read($shm, 0, 8))[1];
        $serialized = shmop_read($shm, 8, $size);
        shmop_delete($shm);

        return igbinary_unserialize($serialized);
    }
}