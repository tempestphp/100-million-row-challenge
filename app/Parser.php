<?php

namespace App;

final class Parser
{
    private const READ_CHUNK_SIZE = 2_097_152;  // 2MB
    private const DISCOVERY_SIZE = 8_388_608;   // 8MB
    private const URI_PREFIX_LEN = 19;          // strlen("https://stitcher.io")
    private const TIMESTAMP_LEN = 25;           // strlen("YYYY-MM-DDTHH:MM:SS+00:00")

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $workerCount = (int) (getenv('WORKER_COUNT') ?: 4);
        $fileSize = filesize($inputPath);

        // ─── Pre-generate date dictionary (truncated 8-char keys for hot loop) ───

        $dateIds = [];
        $dateStrings = [];
        $dateCount = 0;

        for ($year = 2020; $year <= 2026; $year++) {
            $isLeap = ($year % 4 === 0 && ($year % 100 !== 0 || $year % 400 === 0));
            $daysInYear = $isLeap ? 366 : 365;
            $ts = mktime(0, 0, 0, 1, 1, $year);

            for ($day = 0; $day < $daysInYear; $day++) {
                $full = date('Y-m-d', $ts + $day * 86400);
                $dateIds[substr($full, 2)] = $dateCount;
                $dateStrings[$dateCount] = $full;
                $dateCount++;
            }
        }

        // ─── Discovery: read first 8MB, build path map in first-seen order ───

        $pathOffsets = [];
        $pathStrings = [];
        $pathCount = 0;

        $discoveryBytes = min(self::DISCOVERY_SIZE, $fileSize);
        $fh = fopen($inputPath, 'r');
        stream_set_read_buffer($fh, 0);
        $disc = fread($fh, $discoveryBytes);
        fclose($fh);

        $pos = 0;
        $len = strlen($disc);

        while ($pos < $len) {
            $nlPos = strpos($disc, "\n", $pos);
            if ($nlPos === false) {
                break;
            }

            $path = substr($disc, $pos + self::URI_PREFIX_LEN, $nlPos - $pos - 45);

            if (!isset($pathOffsets[$path])) {
                $pathOffsets[$path] = $pathCount * $dateCount;
                $pathStrings[$pathCount] = $path;
                $pathCount++;
            }

            $pos = $nlPos + 1;
        }

        unset($disc);

        $totalSlots = $pathCount * $dateCount;

        // ─── Compute file chunk boundaries (newline-aligned) ───

        $chunkSize = (int) ceil($fileSize / $workerCount);
        $boundaries = [0];

        $fh = fopen($inputPath, 'r');
        for ($i = 1; $i < $workerCount; $i++) {
            $target = $chunkSize * $i;
            if ($target >= $fileSize) {
                break;
            }
            fseek($fh, $target);
            $buf = fread($fh, 8192);
            $nl = strpos($buf, "\n");
            if ($nl !== false) {
                $boundaries[] = $target + $nl + 1;
            }
        }
        $boundaries[] = $fileSize;
        fclose($fh);

        $actualWorkers = count($boundaries) - 1;

        // ─── Fork workers ───

        $tmpDir = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
        $ppid = getmypid();
        $tmpFiles = [];
        $childPids = [];
        $myId = 0;

        for ($w = 1; $w < $actualWorkers; $w++) {
            $tmpFiles[$w] = $tmpDir . '/100m_' . $ppid . '_' . $w . '.bin';
            $pid = pcntl_fork();

            if ($pid === 0) {
                $myId = $w;
                break;
            } elseif ($pid > 0) {
                $childPids[] = $pid;
            }
        }

        // ─── Parse assigned chunk ───

        $counts = array_fill(0, $totalSlots, 0);
        $overflow = []; // Catches paths not seen in discovery phase
        $overflowFirstSeen = [];

        $fh = fopen($inputPath, 'r');
        stream_set_read_buffer($fh, 0);
        fseek($fh, $boundaries[$myId]);

        $rangeStart = $boundaries[$myId];
        $remaining = $boundaries[$myId + 1] - $rangeStart;
        $leftover = '';
        $filePos = $rangeStart;

        while ($remaining > 0) {
            $toRead = min(self::READ_CHUNK_SIZE, $remaining);
            $chunkBase = $filePos - strlen($leftover);
            $chunk = $leftover . fread($fh, $toRead);
            $filePos += $toRead;
            $remaining -= $toRead;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) {
                $leftover = $chunk;
                continue;
            }

            $leftover = ($lastNl < strlen($chunk) - 1) ? substr($chunk, $lastNl + 1) : '';

            $pos = 0;
            while ($pos < $lastNl) {
                $nlPos = strpos($chunk, "\n", $pos);
                if ($nlPos === false || $nlPos > $lastNl) {
                    break;
                }

                $path = substr($chunk, $pos + self::URI_PREFIX_LEN, $nlPos - $pos - 45);

                if (isset($pathOffsets[$path])) {
                    $counts[$pathOffsets[$path] + $dateIds[substr($chunk, $nlPos - 23, 8)]]++;
                } else {
                    $date = substr($chunk, $nlPos - 25, 10);
                    $overflow[$path][$date] = ($overflow[$path][$date] ?? 0) + 1;

                    if (!isset($overflowFirstSeen[$path])) {
                        $overflowFirstSeen[$path] = $chunkBase + $pos;
                    }
                }

                $pos = $nlPos + 1;
            }
        }

        fclose($fh);

        // ─── IPC: children serialize, parent merges ───

        if ($myId > 0) {
            $packed = pack('V*', ...$counts);
            if ($overflow !== [] || $overflowFirstSeen !== []) {
                // Append serialized overflow payload after flat array (fixed-size boundary)
                $packed .= serialize([
                    'overflow' => $overflow,
                    'firstSeen' => $overflowFirstSeen,
                ]);
            }
            file_put_contents($tmpFiles[$myId], $packed);
            exit(0);
        }

        foreach ($childPids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $flatSize = $totalSlots * 4; // 4 bytes per uint32

        for ($w = 1; $w < $actualWorkers; $w++) {
            $data = file_get_contents($tmpFiles[$w]);
            $child = array_values(unpack('V*', substr($data, 0, $flatSize)));

            for ($i = 0; $i < $totalSlots; $i++) {
                $counts[$i] += $child[$i];
            }

            // Merge child overflow payload if present
            if (strlen($data) > $flatSize) {
                $tail = unserialize(substr($data, $flatSize));

                if (is_array($tail)) {
                    $childOverflow = $tail['overflow'] ?? [];
                    $childFirstSeen = $tail['firstSeen'] ?? [];

                    foreach ($childOverflow as $path => $dates) {
                        foreach ($dates as $date => $count) {
                            $overflow[$path][$date] = ($overflow[$path][$date] ?? 0) + $count;
                        }
                    }

                    foreach ($childFirstSeen as $path => $offset) {
                        if (!isset($overflowFirstSeen[$path]) || $offset < $overflowFirstSeen[$path]) {
                            $overflowFirstSeen[$path] = $offset;
                        }
                    }
                }
            }

            unlink($tmpFiles[$w]);
        }

        // ─── Build and write JSON output ───

        $out = fopen($outputPath, 'w');
        stream_set_write_buffer($out, 1_048_576);
        $firstPath = true;

        fwrite($out, "{\n");

        for ($p = 0; $p < $pathCount; $p++) {
            $base = $p * $dateCount;
            $parts = [];

            for ($d = 0; $d < $dateCount; $d++) {
                $c = $counts[$base + $d];
                if ($c > 0) {
                    $parts[] = '        "' . $dateStrings[$d] . '": ' . $c;
                }
            }

            if ($parts !== []) {
                if (!$firstPath) {
                    fwrite($out, ",\n");
                }
                $escapedPath = str_replace('/', '\\/', $pathStrings[$p]);
                fwrite($out, '    "' . $escapedPath . "\": {\n" . implode(",\n", $parts) . "\n    }");
                $firstPath = false;
            }
        }

        // Append overflow paths (discovered after first 8MB) in first-seen order.
        uksort($overflow, static function (string $a, string $b) use ($overflowFirstSeen): int {
            $oa = $overflowFirstSeen[$a] ?? PHP_INT_MAX;
            $ob = $overflowFirstSeen[$b] ?? PHP_INT_MAX;

            if ($oa === $ob) {
                return $a <=> $b;
            }

            return $oa <=> $ob;
        });

        foreach ($overflow as $path => $dates) {
            ksort($dates);
            $parts = [];
            foreach ($dates as $date => $count) {
                $parts[] = '        "' . $date . '": ' . $count;
            }
            if (!$firstPath) {
                fwrite($out, ",\n");
            }
            $escapedPath = str_replace('/', '\\/', $path);
            fwrite($out, '    "' . $escapedPath . "\": {\n" . implode(",\n", $parts) . "\n    }");
            $firstPath = false;
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
