<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $fileSize = filesize($inputPath);

        // Single-thread for small files
        if ($fileSize < 1_000_000) {
            $data = $this->processChunk($inputPath, 0, $fileSize);
            $this->writeOutput($data, $outputPath);
            return;
        }

        // --- Multi-process parallel parsing ---
        $workerCount = 8;
        $boundaries = $this->chunkFile($inputPath, $fileSize, $workerCount);
        $childCount = count($boundaries);
        $tmpDir = sys_get_temp_dir();
        $useIgbinary = function_exists('igbinary_serialize');
        $pids = [];

        // Fork children for chunks 1..N-1
        for ($i = 1; $i < $childCount; $i++) {
            $pid = pcntl_fork();
            if ($pid === 0) {
                $data = $this->processChunk($inputPath, $boundaries[$i][0], $boundaries[$i][1]);
                $serialized = $useIgbinary ? igbinary_serialize($data) : serialize($data);
                file_put_contents("$tmpDir/p100m_$i.tmp", $serialized);
                exit(0);
            }
            $pids[] = $pid;
        }

        // Parent processes chunk 0 concurrently with children
        $merged = $this->processChunk($inputPath, $boundaries[0][0], $boundaries[0][1]);

        // Wait for all children
        while (pcntl_wait($status) > 0);

        // Merge children results in chunk order (preserves first-appearance key order)
        for ($i = 1; $i < $childCount; $i++) {
            $tmpFile = "$tmpDir/p100m_$i.tmp";
            $raw = file_get_contents($tmpFile);
            unlink($tmpFile);
            $data = $useIgbinary ? igbinary_unserialize($raw) : unserialize($raw);

            foreach ($data as $path => $dates) {
                if (!isset($merged[$path])) {
                    $merged[$path] = $dates;
                    continue;
                }
                $ref = &$merged[$path];
                foreach ($dates as $dateInt => $cnt) {
                    if (isset($ref[$dateInt])) {
                        $ref[$dateInt] += $cnt;
                    } else {
                        $ref[$dateInt] = $cnt;
                    }
                }
                unset($ref);
            }
        }

        $this->writeOutput($merged, $outputPath);
    }

    /**
     * Sort integer date keys, convert back to YYYY-MM-DD strings, and write JSON.
     */
    private function writeOutput(array &$data, string $outputPath): void
    {
        foreach ($data as &$dates) {
            ksort($dates);
            $stringDates = [];
            foreach ($dates as $dateInt => $cnt) {
                $d = (string)$dateInt;
                $stringDates[$d[0] . $d[1] . $d[2] . $d[3] . '-' . $d[4] . $d[5] . '-' . $d[6] . $d[7]] = $cnt;
            }
            $dates = $stringDates;
        }
        unset($dates);
        file_put_contents($outputPath, json_encode($data, JSON_PRETTY_PRINT));
    }

    /**
     * Split file into chunks aligned at newline boundaries.
     * @return array<int, array{0: int, 1: int}> Array of [start, end] byte offsets
     */
    private function chunkFile(string $filePath, int $fileSize, int $workerCount): array
    {
        $chunkSize = intdiv($fileSize, $workerCount);
        $boundaries = [];
        $handle = fopen($filePath, 'r');

        $start = 0;
        for ($i = 0; $i < $workerCount - 1; $i++) {
            $end = $start + $chunkSize;
            fseek($handle, $end);
            $buf = fread($handle, 4096);
            if ($buf !== false && ($nl = strpos($buf, "\n")) !== false) {
                $end += $nl + 1;
            }
            $boundaries[] = [$start, $end];
            $start = $end;
        }
        $boundaries[] = [$start, $fileSize];
        fclose($handle);

        return $boundaries;
    }

    /**
     * Process a file chunk. Returns path -> dateInt -> count.
     * Date keys are integers in YYYYMMDD format for faster hash lookups.
     * @return array<string, array<int, int>>
     */
    private function processChunk(string $filePath, int $start, int $end): array
    {
        $data = [];
        $handle = fopen($filePath, 'r');
        fseek($handle, $start);

        $remaining = $end - $start;
        $leftover = '';

        while ($remaining > 0) {
            $chunk = fread($handle, min(8_388_608, $remaining));
            if ($chunk === false || $chunk === '') {
                break;
            }
            $remaining -= strlen($chunk);

            $startPos = 0;

            // Complete leftover line without copying the entire buffer
            if ($leftover !== '') {
                $firstNl = strpos($chunk, "\n");
                if ($firstNl === false) {
                    $leftover .= $chunk;
                    continue;
                }
                $line = $leftover . substr($chunk, 0, $firstNl);
                $len = strlen($line);
                if ($len > 45) {
                    $path = substr($line, 19, $len - 45);
                    $ds = substr($line, $len - 25, 10);
                    $dateInt = (int)($ds[0] . $ds[1] . $ds[2] . $ds[3] . $ds[5] . $ds[6] . $ds[8] . $ds[9]);
                    if (isset($data[$path][$dateInt])) {
                        $data[$path][$dateInt]++;
                    } elseif (isset($data[$path])) {
                        $data[$path][$dateInt] = 1;
                    } else {
                        $data[$path] = [$dateInt => 1];
                    }
                }
                $startPos = $firstNl + 1;
                $leftover = '';
            }

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false || $lastNl < $startPos) {
                $leftover = ($startPos > 0) ? substr($chunk, $startPos) : $chunk;
                continue;
            }
            if ($lastNl < strlen($chunk) - 1) {
                $leftover = substr($chunk, $lastNl + 1);
            } else {
                $leftover = '';
            }

            // Hot parsing loop — 2x unrolled, integer date keys
            $pos = $startPos;
            while ($pos < $lastNl) {
                $nlPos = strpos($chunk, "\n", $pos);
                if ($nlPos === false) {
                    break;
                }
                $path = substr($chunk, $pos + 19, $nlPos - $pos - 45);
                $ds = substr($chunk, $nlPos - 25, 10);
                $dateInt = (int)($ds[0] . $ds[1] . $ds[2] . $ds[3] . $ds[5] . $ds[6] . $ds[8] . $ds[9]);
                if (isset($data[$path][$dateInt])) {
                    $data[$path][$dateInt]++;
                } elseif (isset($data[$path])) {
                    $data[$path][$dateInt] = 1;
                } else {
                    $data[$path] = [$dateInt => 1];
                }
                $pos = $nlPos + 1;
                if ($pos >= $lastNl) {
                    break;
                }

                $nlPos = strpos($chunk, "\n", $pos);
                if ($nlPos === false) {
                    break;
                }
                $path = substr($chunk, $pos + 19, $nlPos - $pos - 45);
                $ds = substr($chunk, $nlPos - 25, 10);
                $dateInt = (int)($ds[0] . $ds[1] . $ds[2] . $ds[3] . $ds[5] . $ds[6] . $ds[8] . $ds[9]);
                if (isset($data[$path][$dateInt])) {
                    $data[$path][$dateInt]++;
                } elseif (isset($data[$path])) {
                    $data[$path][$dateInt] = 1;
                } else {
                    $data[$path] = [$dateInt => 1];
                }
                $pos = $nlPos + 1;
            }
        }

        // Handle final leftover (last line without trailing newline)
        if ($leftover !== '') {
            $len = strlen($leftover);
            if ($len > 45) {
                $path = substr($leftover, 19, $len - 45);
                $ds = substr($leftover, $len - 25, 10);
                $dateInt = (int)($ds[0] . $ds[1] . $ds[2] . $ds[3] . $ds[5] . $ds[6] . $ds[8] . $ds[9]);
                if (isset($data[$path][$dateInt])) {
                    $data[$path][$dateInt]++;
                } elseif (isset($data[$path])) {
                    $data[$path][$dateInt] = 1;
                } else {
                    $data[$path] = [$dateInt => 1];
                }
            }
        }

        fclose($handle);
        return $data;
    }
}
