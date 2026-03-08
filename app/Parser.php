<?php

namespace App;

final class Parser
{
    private const int NUM_WORKERS = 16;

    private function numWorkers(): int
    {
        $env = getenv('NUM_WORKERS');
        return ($env !== false && $env > 0) ? (int)$env : self::NUM_WORKERS;
    }
    private const int DATE_COUNT = 2557; // Days from 2020-01-01 to 2026-12-31

    public function parse(string $inputPath, string $outputPath): void
    {
        $t0 = hrtime(true);

        gc_disable();

        $fileSize = filesize($inputPath);
        $numWorkers = $this->numWorkers();

        // Pre-compute date IDs: "YY-MM-DD" => integer ID
        $dateIds = $this->buildDateIds();

        // Discover paths from first 4MB of file
        // Returns pathIds (path => id), pathBases (path => id*DATE_COUNT), and paths (id => path)
        [$pathIds, $pathBases, $paths] = $this->discoverPaths($inputPath, min($fileSize, 4 * 1024 * 1024));
        $pathCount = count($paths);

        // Calculate chunk boundaries (aligned to newlines)
        $chunks = $this->calculateChunks($inputPath, $fileSize, $numWorkers);

        $chunkSizeMB = round(($chunks[0]['end'] - $chunks[0]['start']) / 1024 / 1024, 1);
        echo "Forking {$numWorkers} workers (flat keys, chunk: {$chunkSizeMB}MB, paths: $pathCount)...\n";

        // Use /dev/shm if available (Linux tmpfs), otherwise system temp
        $tmpDir = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();

        // Fork workers
        $tempFiles = [];
        $pids = [];

        for ($i = 0; $i < $numWorkers; $i++) {
            $tempFile = $tmpDir . '/parser_' . getmypid() . '_' . $i;
            $tempFiles[$i] = $tempFile;

            $pid = pcntl_fork();

            if ($pid === -1) {
                die("Failed to fork");
            } elseif ($pid === 0) {
                // === CHILD PROCESS ===
                $counts = $this->processChunk(
                    $inputPath,
                    $chunks[$i]['start'],
                    $chunks[$i]['end'],
                    $pathBases,
                    $dateIds,
                    $pathCount
                );

                // Write as packed integers
                file_put_contents($tempFile, pack('V*', ...$counts));

                posix_kill(posix_getpid(), SIGKILL);
            } else {
                $pids[$i] = $pid;
            }
        }

        // Wait for all children
        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $t1 = hrtime(true);
        echo "Workers done: " . number_format(($t1 - $t0) / 1e9, 4) . "s\n";

        // Merge results — start with first worker's data (1-indexed from unpack),
        // then add remaining workers with a for($i=1) loop.
        $totalSize = $pathCount * self::DATE_COUNT;

        $firstData = file_get_contents($tempFiles[0]);
        unlink($tempFiles[0]);
        $counts = unpack('V*', $firstData);
        unset($firstData);

        for ($w = 1; $w < $numWorkers; $w++) {
            $data = file_get_contents($tempFiles[$w]);
            unlink($tempFiles[$w]);
            $workerCounts = unpack('V*', $data);
            unset($data);
            for ($i = 1; $i <= $totalSize; $i++) {
                $counts[$i] += $workerCounts[$i];
            }
            unset($workerCounts);
        }

        $t2 = hrtime(true);

        // Build output JSON
        $this->writeJson($outputPath, $counts, $paths, $pathCount);

        $t3 = hrtime(true);

        echo "========================================\n";
        echo "   RESULTS ({$numWorkers} workers, flat keys)\n";
        echo "========================================\n";
        echo "Fork + Process: " . number_format(($t1 - $t0) / 1e9, 4) . "s\n";
        echo "Merge:          " . number_format(($t2 - $t1) / 1e9, 4) . "s\n";
        echo "JSON + Write:   " . number_format(($t3 - $t2) / 1e9, 4) . "s\n";
        echo "----------------------------------------\n";
        echo "TOTAL:          " . number_format(($t3 - $t0) / 1e9, 4) . "s\n";
        echo "========================================\n";
    }

    private function buildDateIds(): array
    {
        $dateIds = [];
        $id = 0;

        for ($y = 2020; $y <= 2026; $y++) {
            $yy = substr((string)$y, 2, 2);
            for ($m = 1; $m <= 12; $m++) {
                $mm = str_pad((string)$m, 2, '0', STR_PAD_LEFT);
                $maxDay = match ($m) {
                    2 => ($y % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                for ($d = 1; $d <= $maxDay; $d++) {
                    $dd = str_pad((string)$d, 2, '0', STR_PAD_LEFT);
                    $dateIds["$yy-$mm-$dd"] = $id++;
                }
            }
        }

        return $dateIds;
    }

    private function discoverPaths(string $inputPath, int $sampleSize): array
    {
        $handle = fopen($inputPath, 'rb');
        $raw = fread($handle, $sampleSize);
        fclose($handle);

        $pathIds   = [];
        $pathBases = [];
        $paths     = [];
        $pathCount = 0;

        $pos    = 0;
        $lastNl = strrpos($raw, "\n");

        while ($pos < $lastNl) {
            $nl = strpos($raw, "\n", $pos);
            if ($nl === false) break;

            // Use $nl-based offsets — consistent with processChunk's hot loop
            $path = substr($raw, $pos + 19, $nl - $pos - 45);

            if (!isset($pathIds[$path])) {
                $pathIds[$path]   = $pathCount;
                $pathBases[$path] = $pathCount * self::DATE_COUNT;
                $paths[$pathCount] = $path;
                $pathCount++;
            }

            $pos = $nl + 1;
        }

        unset($raw);
        return [$pathIds, $pathBases, $paths];
    }

    private function calculateChunks(string $inputPath, int $fileSize, int $numWorkers): array
    {
        $chunkSize = (int) ceil($fileSize / $numWorkers);
        $chunks = [];

        $handle = fopen($inputPath, 'r');
        for ($i = 0; $i < $numWorkers; $i++) {
            $start = $i * $chunkSize;

            if ($i > 0) {
                fseek($handle, $start);
                fgets($handle);
                $start = ftell($handle);
            }

            $end = min(($i + 1) * $chunkSize, $fileSize);
            if ($i < $numWorkers - 1) {
                fseek($handle, $end);
                fgets($handle);
                $end = ftell($handle);
            }

            $chunks[] = ['start' => $start, 'end' => $end];
        }
        fclose($handle);

        return $chunks;
    }

    private function processChunk(
        string $inputPath,
        int $start,
        int $end,
        array $pathBases,
        array $dateIds,
        int $pathCount
    ): array {
        $counts = array_fill(0, $pathCount * self::DATE_COUNT, 0);

        // Read entire chunk into memory — 1 syscall vs millions of fgets calls
        $chunk = file_get_contents($inputPath, false, null, $start, $end - $start);

        // strpos walk: operates directly on the chunk string without a per-line copy.
        // strtok() creates a new string for every line; strpos() just returns an offset.
        // With 6.25M lines per worker that's 6.25M fewer string allocations.
        //
        // Key offsets derived from $nl (newline position) directly — avoids
        // storing $lineLen as a local variable, saving one subtraction per line:
        //   path  = substr($chunk, $pos + 19, $nl - $pos - 45)
        //             ^prefix 19^              ^fixed tail 26 + prefix 19 = 45^
        //   date  = substr($chunk, $nl - 23, 8)
        //             ^timestamp tail is 25 chars; comma+20-prefix = 3; 26-3=23 from nl^
        //
        // $pathBases stores pathId * DATE_COUNT pre-multiplied, eliminating one
        // integer multiplication from the hot path per matching line.
        $pos = 0;
        while (($nl = strpos($chunk, "\n", $pos)) !== false) {
            $base = $pathBases[substr($chunk, $pos + 19, $nl - $pos - 45)] ?? -1;
            if ($base >= 0) {
                ++$counts[$base + $dateIds[substr($chunk, $nl - 23, 8)]];
            }
            $pos = $nl + 1;
        }

        unset($chunk);
        return $counts;
    }

    private function writeJson(string $outputPath, array $counts, array $paths, int $pathCount): void
    {
        // Pre-build the full ISO date strings once (YYYY-MM-DD format for JSON output)
        $dates = [];
        $id = 0;
        for ($y = 2020; $y <= 2026; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxDay = match ($m) {
                    2 => ($y % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                for ($d = 1; $d <= $maxDay; $d++) {
                    $dates[$id++] = sprintf("%d-%02d-%02d", $y, $m, $d);
                }
            }
        }

        // Pre-escape path strings and pre-build date prefix strings once,
        // so the hot loop does zero string escaping or formatting.
        // JSON requires forward slashes to be escaped as "\/" in some encoders,
        // but RFC 8259 only requires it optionally — however json_encode does NOT
        // escape slashes by default, so we match that: no escaping needed.
        $escapedPaths = [];
        for ($p = 0; $p < $pathCount; $p++) {
            // PHP's json_encode escapes forward slashes as "\/" by default.
            // We replicate that here so our manual JSON output matches exactly.
            $escapedPaths[$p] = '    "' . str_replace('/', '\/', $paths[$p]) . '": {';
        }

        // Pre-build date key prefixes: the inner JSON lines look like:
        //   "        \"YYYY-MM-DD\": N"
        $datePrefixes = [];
        for ($d = 0; $d < self::DATE_COUNT; $d++) {
            $datePrefixes[$d] = '        "' . $dates[$d] . '": ';
        }

        // Build the full JSON string in memory, then write in one shot.
        // Benchmarks show this is faster than streaming fwrite() calls because
        // PHP's string concatenation reuses the buffer, whereas many small fwrite()
        // calls add syscall overhead even with userspace buffering.
        $buf = '{';
        $firstPath = true;

        // $counts is 1-indexed (from unpack('V*')), so offset by 1
        $baseOffset = 1;

        for ($p = 0; $p < $pathCount; $p++) {
            $base    = $baseOffset + $p * self::DATE_COUNT;
            $entries = '';
            $hasEntry = false;

            for ($d = 0; $d < self::DATE_COUNT; $d++) {
                $count = $counts[$base + $d];
                if ($count > 0) {
                    if ($hasEntry) {
                        $entries .= ",\n" . $datePrefixes[$d] . $count;
                    } else {
                        $entries  = $datePrefixes[$d] . $count;
                        $hasEntry = true;
                    }
                }
            }

            if (!$hasEntry) continue;

            if ($firstPath) {
                $buf .= "\n" . $escapedPaths[$p] . "\n" . $entries . "\n    }";
                $firstPath = false;
            } else {
                $buf .= ",\n" . $escapedPaths[$p] . "\n" . $entries . "\n    }";
            }
        }

        $buf .= "\n}";

        file_put_contents($outputPath, $buf);
    }
}