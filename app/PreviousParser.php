<?php

namespace App;

final class PreviousParser
{
    private const SLUG_OFFSET = 25;
    private const CHUNK_TARGET_SIZE = 16 * 1024 * 1024;
    private const READ_BUFFER = 512 * 1024;
    private const WORKER_COUNT = 1;

    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);

        // Pre-calculate ALL chunk boundaries (aligned to newlines)
        $s = microtime(true);
        $boundaries = $this->calculateAllChunkBoundaries($inputPath, $fileSize);
        echo round(microtime(true) - $s, 6) . "s\n";
        $totalChunks = count($boundaries) - 1;

        // Shared counter: which chunk to process next
        $semKey = ftok($inputPath, 'S');
        $shmKey = ftok($inputPath, 'C');
        $sem = sem_get($semKey, 1);
        $shm = shm_attach($shmKey, 1024);
        shm_put_var($shm, 1, 0); // next chunk index = 0

        // Fork workers — each writes results to a temp file
        $tmpFiles = [];
        $pids = [];

        for ($w = 0; $w < self::WORKER_COUNT; $w++) {
            $tmpFile = tempnam(sys_get_temp_dir(), 'chunk_');
            $tmpFiles[$w] = $tmpFile;

            $pid = pcntl_fork();

            if (-1 === $pid) {
                // Fork failed — single-thread fallback
                $result = $this->processChunk($inputPath, 0, $fileSize);
                $this->writeOutput($outputPath, $result);
                return;
            }

            if (0 === $pid) {
                // Child: grab chunks until none left
                $localResult = [];

                while (true) {
                    // Atomically grab next chunk
                    sem_acquire($sem);
                    $chunkIdx = shm_get_var($shm, 1);
                    if ($totalChunks <= $chunkIdx) {
                        sem_release($sem);
                        break;
                    }
                    shm_put_var($shm, 1, $chunkIdx + 1);
                    sem_release($sem);

                    // Process this chunk
                    $start = $boundaries[$chunkIdx];
                    $end = $boundaries[$chunkIdx + 1];
                    $chunkResult = $this->processChunk($inputPath, $start, $end);

                    // Merge into local result
                    foreach ($chunkResult as $url => $dates) {
                        if (!isset($localResult[$url])) {
                            $localResult[$url] = $dates;
                        } else {
                            foreach ($dates as $date => $count) {
                                $localResult[$url][$date] = ($localResult[$url][$date] ?? 0) + $count;
                            }
                        }
                    }
                }

                // Write local result to temp file
                file_put_contents($tmpFile, serialize($localResult));
                exit(0);
            }

            $pids[$w] = $pid;
        }

        // Parent: wait for all children
        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        // Cleanup shared memory
        shm_remove($shm);
        sem_remove($sem);

        // Merge results from all workers
        $result = [];
        foreach ($tmpFiles as $tmpFile) {
            $partialResult = unserialize(file_get_contents($tmpFile));
            foreach ($partialResult as $url => $dates) {
                if (!isset($result[$url])) {
                    $result[$url] = $dates;
                } else {
                    foreach ($dates as $date => $count) {
                        $result[$url][$date] = ($result[$url][$date] ?? 0) + $count;
                    }
                }
            }
            unlink($tmpFile);
        }

        foreach ($result as $url => &$dates) {
            ksort($dates);
        }

        $this->writeOutput($outputPath, $result);
    }

    /**
     * @return array<string, array<string, int>>
     */
    private function processChunk(string $inputPath, int $start, int $end): array
    {
        $result = [];
        $handle = fopen($inputPath, 'r');
        fseek($handle, $start);

        $remaining = '';
        $bytesLeft = $end - $start;

        while (0 < $bytesLeft) {
            $readSize = min(self::READ_BUFFER, $bytesLeft);
            $buffer = fread($handle, $readSize);

            if (false === $buffer || '' === $buffer) {
                break;
            }

            $bytesLeft -= strlen($buffer);
            $buffer = $remaining . $buffer;

            $lastNewline = strrpos($buffer, "\n");
            if (false === $lastNewline) {
                $remaining = $buffer;
                continue;
            }

            if (strlen($buffer) - 1 > $lastNewline) {
                $remaining = substr($buffer, $lastNewline + 1);
                $buffer = substr($buffer, 0, $lastNewline + 1);
            } else {
                $remaining = '';
            }

            $offset = 0;
            $bufLen = strlen($buffer);

            while ($bufLen > $offset) {
                $commaPos = strpos($buffer, ',', $offset);
                if (false === $commaPos) {
                    break;
                }

                // Extract just the slug: skip 'https://stitcher.io/blog/'
                $slug = substr($buffer, $offset + self::SLUG_OFFSET, $commaPos - $offset - self::SLUG_OFFSET);
                $date = substr($buffer, $commaPos + 1, 10);

                if (isset($result[$slug][$date])) {
                    $result[$slug][$date]++;
                } elseif (isset($result[$slug])) {
                    $result[$slug][$date] = 1;
                } else {
                    $result[$slug] = [$date => 1];
                }

                $newlinePos = strpos($buffer, "\n", $commaPos);
                if (false === $newlinePos) {
                    break;
                }
                $offset = $newlinePos + 1;
            }
        }

        if ('' !== $remaining) {
            $commaPos = strpos($remaining, ',');
            if (false !== $commaPos) {
                $slug = substr($remaining, self::SLUG_OFFSET, $commaPos - self::SLUG_OFFSET);
                $date = substr($remaining, $commaPos + 1, 10);

                if (isset($result[$slug][$date])) {
                    $result[$slug][$date]++;
                } elseif (isset($result[$slug])) {
                    $result[$slug][$date] = 1;
                } else {
                    $result[$slug] = [$date => 1];
                }
            }
        }

        fclose($handle);
        return $result;
    }

    /**
     * @return int[]
     */
    private function calculateAllChunkBoundaries(string $inputPath, int $fileSize): array
    {
        $handle = fopen($inputPath, 'r');
        $boundaries = [0];
        $pos = 0;

        while ($fileSize > $pos) {
            $nextPos = min($pos + self::CHUNK_TARGET_SIZE, $fileSize);

            if ($fileSize <= $nextPos) {
                $boundaries[] = $fileSize;
                break;
            }

            fseek($handle, $nextPos);
            $line = fgets($handle);
            if (false === $line) {
                $boundaries[] = $fileSize;
                break;
            }

            $boundaries[] = ftell($handle);
            $pos = ftell($handle);
        }

        fclose($handle);
        return $boundaries;
    }

    private function writeOutput(string $outputPath, array $result): void
    {
        $handle = fopen($outputPath, 'w');
        stream_set_write_buffer($handle, 1024 * 1024);

        fwrite($handle, "{\n");

        $firstUrl = true;
        foreach ($result as $slug => $dates) {
            if (!$firstUrl) {
                fwrite($handle, ",\n");
            }
            $firstUrl = false;
            fwrite($handle, "    \"\\/blog\\/{$slug}\": {\n");
            $firstDate = true;
            foreach ($dates as $date => $count) {
                if (!$firstDate) {
                    fwrite($handle, ",\n");
                }
                $firstDate = false;
                fwrite($handle, "        \"{$date}\": {$count}");
            }
            fwrite($handle, "\n    }");
        }

        fwrite($handle, "\n}");
        fclose($handle);
    }
}

