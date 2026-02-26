<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        \ini_set('memory_limit', '4G');
        \gc_disable();

        $fileSize = \filesize($inputPath);

        $handle = \fopen($inputPath, 'rb');
        $discoverSize = \min($fileSize, 16_777_216);
        $discoverChunk = \fread($handle, $discoverSize);
        \fclose($handle);

        $pathIds = [];
        $paths = [];
        $pathCount = 0;
        $dateSet = [];

        $lastNl = \strrpos($discoverChunk, "\n");
        $p = 25;
        while ($p < $lastNl) {
            $sep = \strpos($discoverChunk, ',', $p);
            if ($sep === false || $sep >= $lastNl) break;

            $slug = \substr($discoverChunk, $p, $sep - $p);
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug] = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }

            $dateSet[\substr($discoverChunk, $sep + 3, 8)] = true;

            $p = $sep + 52;
        }
        unset($discoverChunk);

        \ksort($dateSet);
        $dateIds = [];
        $dates = [];
        $dateCount = 0;
        foreach ($dateSet as $date => $_) {
            $dateIds[$date] = $dateCount;
            $dates[$dateCount] = $date;
            $dateCount++;
        }
        unset($dateSet);

        $totalCells = $pathCount * $dateCount;
        foreach ($pathIds as $slug => $id) {
            $pathIds[$slug] = $id * $dateCount;
        }

        if (PHP_OS_FAMILY === 'Darwin') {
            $numWorkers = (int) \trim(\shell_exec('sysctl -n hw.ncpu'));
        } else {
            $numWorkers = (int) (\trim(\shell_exec('nproc 2>/dev/null') ?: '8'));
        }

        $handle = \fopen($inputPath, 'rb');
        $splits = [0];
        for ($w = 1; $w < $numWorkers; $w++) {
            \fseek($handle, (int) ($fileSize * $w / $numWorkers));
            \fgets($handle);
            $splits[] = \ftell($handle);
        }
        $splits[] = $fileSize;
        \fclose($handle);

        $tmpDir = \is_dir('/dev/shm') ? '/dev/shm' : \sys_get_temp_dir();
        $tmpPrefix = $tmpDir . '/p_' . \getmypid() . '_';
        $chunkSize = 4_194_304;

        $childPids = [];
        for ($w = 1; $w < $numWorkers; $w++) {
            $pid = \pcntl_fork();
            if ($pid === -1) continue;
            if ($pid === 0) {
                $counts = \array_fill(0, $totalCells, 0);
                $handle = \fopen($inputPath, 'rb');
                \stream_set_read_buffer($handle, 0);
                \fseek($handle, $splits[$w]);
                $remaining = $splits[$w + 1] - $splits[$w];
                $leftover = '';

                while ($remaining > 0) {
                    $toRead = $remaining < $chunkSize ? $remaining : $chunkSize;
                    $chunk = \fread($handle, $toRead);
                    if ($chunk === false || $chunk === '') break;
                    $remaining -= \strlen($chunk);

                    if ($leftover !== '') {
                        $nlPos = \strpos($chunk, "\n");
                        if ($nlPos === false) {
                            $leftover .= $chunk;
                            continue;
                        }
                        $full = $leftover . \substr($chunk, 0, $nlPos);
                        $leftover = '';
                        $sep = \strpos($full, ',', 25);
                        if ($sep !== false) {
                            $counts[$pathIds[\substr($full, 25, $sep - 25)] + $dateIds[\substr($full, $sep + 3, 8)]]++;
                        }
                        $p = $nlPos + 26;
                    } else {
                        $p = 25;
                    }

                    $lastNl = \strrpos($chunk, "\n");
                    if ($lastNl === false) {
                        $leftover = \substr($chunk, $p - 25);
                        continue;
                    }

                    $fence = $lastNl - 600;
                    while ($p < $fence) {
                        $sep = \strpos($chunk, ',', $p);
                        $counts[$pathIds[\substr($chunk, $p, $sep - $p)] + $dateIds[\substr($chunk, $sep + 3, 8)]]++;
                        $p = $sep + 52;

                        $sep = \strpos($chunk, ',', $p);
                        $counts[$pathIds[\substr($chunk, $p, $sep - $p)] + $dateIds[\substr($chunk, $sep + 3, 8)]]++;
                        $p = $sep + 52;

                        $sep = \strpos($chunk, ',', $p);
                        $counts[$pathIds[\substr($chunk, $p, $sep - $p)] + $dateIds[\substr($chunk, $sep + 3, 8)]]++;
                        $p = $sep + 52;

                        $sep = \strpos($chunk, ',', $p);
                        $counts[$pathIds[\substr($chunk, $p, $sep - $p)] + $dateIds[\substr($chunk, $sep + 3, 8)]]++;
                        $p = $sep + 52;

                        $sep = \strpos($chunk, ',', $p);
                        $counts[$pathIds[\substr($chunk, $p, $sep - $p)] + $dateIds[\substr($chunk, $sep + 3, 8)]]++;
                        $p = $sep + 52;

                        $sep = \strpos($chunk, ',', $p);
                        $counts[$pathIds[\substr($chunk, $p, $sep - $p)] + $dateIds[\substr($chunk, $sep + 3, 8)]]++;
                        $p = $sep + 52;
                    }
                    while ($p < $lastNl) {
                        $sep = \strpos($chunk, ',', $p);
                        if ($sep === false || $sep >= $lastNl) break;
                        $counts[$pathIds[\substr($chunk, $p, $sep - $p)] + $dateIds[\substr($chunk, $sep + 3, 8)]]++;
                        $p = $sep + 52;
                    }

                    if ($lastNl + 1 < \strlen($chunk)) {
                        $leftover = \substr($chunk, $lastNl + 1);
                    }
                }

                if ($leftover !== '') {
                    $sep = \strpos($leftover, ',', 25);
                    if ($sep !== false && isset($pathIds[\substr($leftover, 25, $sep - 25)]) && isset($dateIds[\substr($leftover, $sep + 3, 8)])) {
                        $counts[$pathIds[\substr($leftover, 25, $sep - 25)] + $dateIds[\substr($leftover, $sep + 3, 8)]]++;
                    }
                }

                \fclose($handle);
                \file_put_contents($tmpPrefix . $w, \pack('V*', ...$counts));
                exit(0);
            }
            $childPids[$w] = $pid;
        }

        $counts = \array_fill(0, $totalCells, 0);
        $handle = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($handle, 0);
        $remaining = $splits[1];
        $leftover = '';

        while ($remaining > 0) {
            $toRead = $remaining < $chunkSize ? $remaining : $chunkSize;
            $chunk = \fread($handle, $toRead);
            if ($chunk === false || $chunk === '') break;
            $remaining -= \strlen($chunk);

            if ($leftover !== '') {
                $nlPos = \strpos($chunk, "\n");
                if ($nlPos === false) {
                    $leftover .= $chunk;
                    continue;
                }
                $full = $leftover . \substr($chunk, 0, $nlPos);
                $leftover = '';
                $sep = \strpos($full, ',', 25);
                if ($sep !== false) {
                    $counts[$pathIds[\substr($full, 25, $sep - 25)] + $dateIds[\substr($full, $sep + 3, 8)]]++;
                }
                $p = $nlPos + 26;
            } else {
                $p = 25;
            }

            $lastNl = \strrpos($chunk, "\n");
            if ($lastNl === false) {
                $leftover = \substr($chunk, $p - 25);
                continue;
            }

            $fence = $lastNl - 600;
            while ($p < $fence) {
                $sep = \strpos($chunk, ',', $p);
                $counts[$pathIds[\substr($chunk, $p, $sep - $p)] + $dateIds[\substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;

                $sep = \strpos($chunk, ',', $p);
                $counts[$pathIds[\substr($chunk, $p, $sep - $p)] + $dateIds[\substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;

                $sep = \strpos($chunk, ',', $p);
                $counts[$pathIds[\substr($chunk, $p, $sep - $p)] + $dateIds[\substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;

                $sep = \strpos($chunk, ',', $p);
                $counts[$pathIds[\substr($chunk, $p, $sep - $p)] + $dateIds[\substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;

                $sep = \strpos($chunk, ',', $p);
                $counts[$pathIds[\substr($chunk, $p, $sep - $p)] + $dateIds[\substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;

                $sep = \strpos($chunk, ',', $p);
                $counts[$pathIds[\substr($chunk, $p, $sep - $p)] + $dateIds[\substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;
            }
            while ($p < $lastNl) {
                $sep = \strpos($chunk, ',', $p);
                if ($sep === false || $sep >= $lastNl) break;
                $counts[$pathIds[\substr($chunk, $p, $sep - $p)] + $dateIds[\substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;
            }

            if ($lastNl + 1 < \strlen($chunk)) {
                $leftover = \substr($chunk, $lastNl + 1);
            }
        }

        if ($leftover !== '') {
            $sep = \strpos($leftover, ',', 25);
            if ($sep !== false && isset($pathIds[\substr($leftover, 25, $sep - 25)]) && isset($dateIds[\substr($leftover, $sep + 3, 8)])) {
                $counts[$pathIds[\substr($leftover, 25, $sep - 25)] + $dateIds[\substr($leftover, $sep + 3, 8)]]++;
            }
        }
        \fclose($handle);

        $pending = $childPids;
        while (!empty($pending)) {
            $merged = false;
            foreach ($pending as $w => $pid) {
                $ret = \pcntl_waitpid($pid, $status, WNOHANG);
                if ($ret > 0) {
                    $raw = \file_get_contents($tmpPrefix . $w);
                    @\unlink($tmpPrefix . $w);
                    $childCounts = \unpack('V*', $raw);
                    for ($i = 0; $i < $totalCells; $i++) {
                        $counts[$i] += $childCounts[$i + 1];
                    }
                    unset($pending[$w]);
                    $merged = true;
                    break;
                }
            }
            if (!$merged && !empty($pending)) {
                $w = \array_key_first($pending);
                \pcntl_waitpid($pending[$w], $status);
                $raw = \file_get_contents($tmpPrefix . $w);
                @\unlink($tmpPrefix . $w);
                $childCounts = \unpack('V*', $raw);
                for ($i = 0; $i < $totalCells; $i++) {
                    $counts[$i] += $childCounts[$i + 1];
                }
                unset($pending[$w]);
            }
        }

        $fp = \fopen($outputPath, 'wb');
        $buf = '{';
        $firstPath = true;
        for ($p = 0; $p < $pathCount; $p++) {
            $offset = $p * $dateCount;
            $hasAny = false;
            for ($d = 0; $d < $dateCount; $d++) {
                if ($counts[$offset + $d] > 0) {
                    $hasAny = true;
                    break;
                }
            }
            if (!$hasAny) continue;
            if (!$firstPath) $buf .= ',';
            $firstPath = false;
            $buf .= "\n    \"\\/blog\\/" . \str_replace('/', '\\/', $paths[$p]) . "\": {";
            $firstDate = true;
            for ($d = 0; $d < $dateCount; $d++) {
                $count = $counts[$offset + $d];
                if ($count === 0) continue;
                if (!$firstDate) $buf .= ',';
                $firstDate = false;
                $buf .= "\n        \"20" . $dates[$d] . "\": " . $count;
            }
            $buf .= "\n    }";
            if (\strlen($buf) > 65536) {
                \fwrite($fp, $buf);
                $buf = '';
            }
        }
        $buf .= "\n}";
        \fwrite($fp, $buf);
        \fclose($fp);
    }
}
