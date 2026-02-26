<?php

namespace App;

final class Parser
{
    public function parse($inputPath, $outputPath)
    {
        \ini_set('memory_limit', '4G');
        \gc_disable();

        $fileSize = \filesize($inputPath);

        $handle = \fopen($inputPath, 'rb');
        $discoverChunk = \fread($handle, \min($fileSize, 16777216));
        \fclose($handle);

        $pathIds = [];
        $pathList = [];
        $dateSet = [];
        $pathCount = 0;

        $pos = 0;
        while (($nlPos = \strpos($discoverChunk, "\n", $pos)) !== false) {
            $path = \substr($discoverChunk, $pos + 19, $nlPos - $pos - 45);
            if (!isset($pathIds[$path])) {
                $pathIds[$path] = $pathCount;
                $pathList[$pathCount] = $path;
                $pathCount++;
            }
            $date = \substr($discoverChunk, $nlPos - 25, 10);
            $dateSet[$date] = true;
            $pos = $nlPos + 1;
        }
        unset($discoverChunk);

        \ksort($dateSet);
        $dateIds = [];
        $dateList = [];
        $dateCount = 0;
        foreach ($dateSet as $date => $_) {
            $dateIds[$date] = $dateCount;
            $dateList[$dateCount] = $date;
            $dateCount++;
        }
        unset($dateSet);

        $stride = $dateCount;
        $totalCells = $pathCount * $stride;
        $chunkSize = 2097152;

        $dateIdChars = [];
        foreach ($dateIds as $date => $id) {
            $dateIdChars[$date] = \chr($id & 0xFF) . \chr($id >> 8);
        }

        $pathOffsets = [];
        foreach ($pathIds as $path => $id) {
            $pathOffsets[$path] = $id * $stride;
        }

        if ($fileSize >= 10485760) {
            if (PHP_OS_FAMILY === 'Darwin') {
                $numWorkers = (int)\trim(\shell_exec('sysctl -n hw.ncpu'));
            } else {
                $numWorkers = (int)(\trim(\shell_exec('nproc 2>/dev/null') ?: '8'));
            }

            $handle = \fopen($inputPath, 'rb');
            $splits = [0];
            for ($w = 1; $w < $numWorkers; $w++) {
                \fseek($handle, (int)($fileSize * $w / $numWorkers));
                \fgets($handle);
                $splits[] = \ftell($handle);
            }
            $splits[] = $fileSize;
            \fclose($handle);

            $tmpDir = \is_dir('/dev/shm') ? '/dev/shm' : \sys_get_temp_dir();
            $tmpPrefix = $tmpDir . '/p_' . \getmypid() . '_';

            $childPids = [];
            for ($w = 1; $w < $numWorkers; $w++) {
                $pid = \pcntl_fork();
                if ($pid === -1) continue;
                if ($pid === 0) {
                    $handle = \fopen($inputPath, 'rb');
                    \stream_set_read_buffer($handle, 0);
                    \fseek($handle, $splits[$w]);
                    $buckets = \array_fill(0, $pathCount, '');
                    $leftover = '';
                    $remaining = $splits[$w + 1] - $splits[$w];

                    while ($remaining > 0) {
                        $toRead = $remaining < $chunkSize ? $remaining : $chunkSize;
                        $chunk = \fread($handle, $toRead);
                        if ($chunk === false || $chunk === '') break;
                        $remaining -= \strlen($chunk);
                        $pos = 0;
                        if ($leftover !== '') {
                            $nlPos = \strpos($chunk, "\n");
                            if ($nlPos === false) { $leftover .= $chunk; continue; }
                            $fullLine = $leftover . \substr($chunk, 0, $nlPos);
                            $lineLen = \strlen($fullLine);
                            $path = \substr($fullLine, 19, $lineLen - 45);
                            $date = \substr($fullLine, $lineLen - 25, 10);
                            if (isset($pathIds[$path]) && isset($dateIdChars[$date])) {
                                $buckets[$pathIds[$path]] .= $dateIdChars[$date];
                            }
                            $pos = $nlPos + 1;
                        }
                        while (true) {
                            $nlPos = \strpos($chunk, "\n", $pos);
                            if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                            $buckets[$pathIds[\substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[\substr($chunk, $nlPos - 25, 10)];
                            $pos = $nlPos + 1;
                            $nlPos = \strpos($chunk, "\n", $pos);
                            if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                            $buckets[$pathIds[\substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[\substr($chunk, $nlPos - 25, 10)];
                            $pos = $nlPos + 1;
                            $nlPos = \strpos($chunk, "\n", $pos);
                            if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                            $buckets[$pathIds[\substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[\substr($chunk, $nlPos - 25, 10)];
                            $pos = $nlPos + 1;
                            $nlPos = \strpos($chunk, "\n", $pos);
                            if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                            $buckets[$pathIds[\substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[\substr($chunk, $nlPos - 25, 10)];
                            $pos = $nlPos + 1;
                            $nlPos = \strpos($chunk, "\n", $pos);
                            if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                            $buckets[$pathIds[\substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[\substr($chunk, $nlPos - 25, 10)];
                            $pos = $nlPos + 1;
                            $nlPos = \strpos($chunk, "\n", $pos);
                            if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                            $buckets[$pathIds[\substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[\substr($chunk, $nlPos - 25, 10)];
                            $pos = $nlPos + 1;
                        }
                    }
                    if ($leftover !== '') {
                        $lineLen = \strlen($leftover);
                        if ($lineLen >= 46) {
                            $path = \substr($leftover, 19, $lineLen - 45);
                            $date = \substr($leftover, $lineLen - 25, 10);
                            if (isset($pathIds[$path]) && isset($dateIdChars[$date])) {
                                $buckets[$pathIds[$path]] .= $dateIdChars[$date];
                            }
                        }
                    }
                    \fclose($handle);
                    $counts = \array_fill(0, $totalCells, 0);
                    for ($p = 0; $p < $pathCount; $p++) {
                        if ($buckets[$p] === '') continue;
                        $offset = $p * $stride;
                        foreach (\unpack('v*', $buckets[$p]) as $did) {
                            $counts[$offset + $did]++;
                        }
                    }
                    \file_put_contents($tmpPrefix . $w, \pack('V*', ...$counts));
                    exit(0);
                }
                $childPids[$w] = $pid;
            }

            // Parent processes first segment
            $handle = \fopen($inputPath, 'rb');
            \stream_set_read_buffer($handle, 0);
            $buckets = \array_fill(0, $pathCount, '');
            $leftover = '';
            $remaining = $splits[1];

            while ($remaining > 0) {
                $toRead = $remaining < $chunkSize ? $remaining : $chunkSize;
                $chunk = \fread($handle, $toRead);
                if ($chunk === false || $chunk === '') break;
                $remaining -= \strlen($chunk);
                $pos = 0;
                if ($leftover !== '') {
                    $nlPos = \strpos($chunk, "\n");
                    if ($nlPos === false) { $leftover .= $chunk; continue; }
                    $fullLine = $leftover . \substr($chunk, 0, $nlPos);
                    $lineLen = \strlen($fullLine);
                    $path = \substr($fullLine, 19, $lineLen - 45);
                    $date = \substr($fullLine, $lineLen - 25, 10);
                    if (isset($pathIds[$path]) && isset($dateIdChars[$date])) {
                        $buckets[$pathIds[$path]] .= $dateIdChars[$date];
                    }
                    $pos = $nlPos + 1;
                }
                while (true) {
                    $nlPos = \strpos($chunk, "\n", $pos);
                    if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                    $buckets[$pathIds[\substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[\substr($chunk, $nlPos - 25, 10)];
                    $pos = $nlPos + 1;
                    $nlPos = \strpos($chunk, "\n", $pos);
                    if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                    $buckets[$pathIds[\substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[\substr($chunk, $nlPos - 25, 10)];
                    $pos = $nlPos + 1;
                    $nlPos = \strpos($chunk, "\n", $pos);
                    if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                    $buckets[$pathIds[\substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[\substr($chunk, $nlPos - 25, 10)];
                    $pos = $nlPos + 1;
                    $nlPos = \strpos($chunk, "\n", $pos);
                    if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                    $buckets[$pathIds[\substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[\substr($chunk, $nlPos - 25, 10)];
                    $pos = $nlPos + 1;
                    $nlPos = \strpos($chunk, "\n", $pos);
                    if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                    $buckets[$pathIds[\substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[\substr($chunk, $nlPos - 25, 10)];
                    $pos = $nlPos + 1;
                    $nlPos = \strpos($chunk, "\n", $pos);
                    if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                    $buckets[$pathIds[\substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[\substr($chunk, $nlPos - 25, 10)];
                    $pos = $nlPos + 1;
                }
            }
            if ($leftover !== '') {
                $lineLen = \strlen($leftover);
                if ($lineLen >= 46) {
                    $path = \substr($leftover, 19, $lineLen - 45);
                    $date = \substr($leftover, $lineLen - 25, 10);
                    if (isset($pathIds[$path]) && isset($dateIdChars[$date])) {
                        $buckets[$pathIds[$path]] .= $dateIdChars[$date];
                    }
                }
            }
            \fclose($handle);

            // Convert buckets to counts
            $counts = \array_fill(0, $totalCells, 0);
            for ($p = 0; $p < $pathCount; $p++) {
                if ($buckets[$p] === '') continue;
                $offset = $p * $stride;
                foreach (\unpack('v*', $buckets[$p]) as $did) {
                    $counts[$offset + $did]++;
                }
            }
            unset($buckets);

            foreach ($childPids as $w => $pid) {
                \pcntl_waitpid($pid, $status);
                $raw = \file_get_contents($tmpPrefix . $w);
                @\unlink($tmpPrefix . $w);
                $childCounts = \unpack('V*', $raw);
                for ($i = 0; $i < $totalCells; $i++) {
                    $counts[$i] += $childCounts[$i + 1];
                }
            }

            $fp = \fopen($outputPath, 'wb');
            $buf = '{';
            $firstPath = true;
            for ($p = 0; $p < $pathCount; $p++) {
                $offset = $p * $stride;
                $hasAny = false;
                for ($d = 0; $d < $dateCount; $d++) {
                    if ($counts[$offset + $d] > 0) { $hasAny = true; break; }
                }
                if (!$hasAny) continue;
                if (!$firstPath) $buf .= ',';
                $firstPath = false;
                $buf .= "\n    \"" . \str_replace('/', '\\/', $pathList[$p]) . "\": {";
                $firstDate = true;
                for ($d = 0; $d < $dateCount; $d++) {
                    $count = $counts[$offset + $d];
                    if ($count === 0) continue;
                    if (!$firstDate) $buf .= ',';
                    $firstDate = false;
                    $buf .= "\n        \"" . $dateList[$d] . "\": " . $count;
                }
                $buf .= "\n    }";
                if (\strlen($buf) > 65536) { \fwrite($fp, $buf); $buf = ''; }
            }
            $buf .= "\n}";
            \fwrite($fp, $buf);
            \fclose($fp);
            return;
        }

        // Single process (small file)
        $handle = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($handle, 0);
        $counts = \array_fill(0, $totalCells, 0);
        $leftover = '';
        $remaining = $fileSize;

        while ($remaining > 0) {
            $toRead = $remaining < $chunkSize ? $remaining : $chunkSize;
            $chunk = \fread($handle, $toRead);
            if ($chunk === false || $chunk === '') break;
            $remaining -= \strlen($chunk);
            $pos = 0;
            if ($leftover !== '') {
                $nlPos = \strpos($chunk, "\n");
                if ($nlPos === false) { $leftover .= $chunk; continue; }
                $fullLine = $leftover . \substr($chunk, 0, $nlPos);
                $lineLen = \strlen($fullLine);
                $path = \substr($fullLine, 19, $lineLen - 45);
                $date = \substr($fullLine, $lineLen - 25, 10);
                if (isset($pathOffsets[$path]) && isset($dateIds[$date])) {
                    $counts[$pathOffsets[$path] + $dateIds[$date]]++;
                }
                $pos = $nlPos + 1;
            }
            while (true) {
                $nlPos = \strpos($chunk, "\n", $pos);
                if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                $counts[$pathOffsets[\substr($chunk, $pos + 19, $nlPos - $pos - 45)] + $dateIds[\substr($chunk, $nlPos - 25, 10)]]++;
                $pos = $nlPos + 1;
                $nlPos = \strpos($chunk, "\n", $pos);
                if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                $counts[$pathOffsets[\substr($chunk, $pos + 19, $nlPos - $pos - 45)] + $dateIds[\substr($chunk, $nlPos - 25, 10)]]++;
                $pos = $nlPos + 1;
                $nlPos = \strpos($chunk, "\n", $pos);
                if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                $counts[$pathOffsets[\substr($chunk, $pos + 19, $nlPos - $pos - 45)] + $dateIds[\substr($chunk, $nlPos - 25, 10)]]++;
                $pos = $nlPos + 1;
                $nlPos = \strpos($chunk, "\n", $pos);
                if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                $counts[$pathOffsets[\substr($chunk, $pos + 19, $nlPos - $pos - 45)] + $dateIds[\substr($chunk, $nlPos - 25, 10)]]++;
                $pos = $nlPos + 1;
                $nlPos = \strpos($chunk, "\n", $pos);
                if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                $counts[$pathOffsets[\substr($chunk, $pos + 19, $nlPos - $pos - 45)] + $dateIds[\substr($chunk, $nlPos - 25, 10)]]++;
                $pos = $nlPos + 1;
                $nlPos = \strpos($chunk, "\n", $pos);
                if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                $counts[$pathOffsets[\substr($chunk, $pos + 19, $nlPos - $pos - 45)] + $dateIds[\substr($chunk, $nlPos - 25, 10)]]++;
                $pos = $nlPos + 1;
            }
        }
        if ($leftover !== '') {
            $lineLen = \strlen($leftover);
            if ($lineLen >= 46) {
                $path = \substr($leftover, 19, $lineLen - 45);
                $date = \substr($leftover, $lineLen - 25, 10);
                if (isset($pathOffsets[$path]) && isset($dateIds[$date])) {
                    $counts[$pathOffsets[$path] + $dateIds[$date]]++;
                }
            }
        }
        \fclose($handle);

        $fp = \fopen($outputPath, 'wb');
        $buf = '{';
        $firstPath = true;
        for ($p = 0; $p < $pathCount; $p++) {
            $offset = $p * $stride;
            $hasAny = false;
            for ($d = 0; $d < $dateCount; $d++) {
                if ($counts[$offset + $d] > 0) { $hasAny = true; break; }
            }
            if (!$hasAny) continue;
            if (!$firstPath) $buf .= ',';
            $firstPath = false;
            $buf .= "\n    \"" . \str_replace('/', '\\/', $pathList[$p]) . "\": {";
            $firstDate = true;
            for ($d = 0; $d < $dateCount; $d++) {
                $count = $counts[$offset + $d];
                if ($count === 0) continue;
                if (!$firstDate) $buf .= ',';
                $firstDate = false;
                $buf .= "\n        \"" . $dateList[$d] . "\": " . $count;
            }
            $buf .= "\n    }";
            if (\strlen($buf) > 65536) { \fwrite($fp, $buf); $buf = ''; }
        }
        $buf .= "\n}";
        \fwrite($fp, $buf);
        \fclose($fp);
    }
}
