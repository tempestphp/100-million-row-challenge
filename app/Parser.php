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
        $discoverChunk = \fread($handle, \min($fileSize, 4194304));
        \fclose($handle);

        $pathIds = [];
        $pathList = [];
        $pathCount = 0;

        $pos = 0;
        while (($nlPos = \strpos($discoverChunk, "\n", $pos)) !== false) {
            $path = \substr($discoverChunk, $pos + 19, $nlPos - $pos - 45);
            if (!isset($pathIds[$path])) {
                $pathIds[$path] = $pathCount;
                $pathList[$pathCount] = $path;
                $pathCount++;
            }
            $pos = $nlPos + 1;
        }
        unset($discoverChunk);

        foreach (Commands\Visit::all() as $visit) {
            $path = \substr($visit->uri, 19);
            if (!isset($pathIds[$path])) {
                $pathIds[$path] = $pathCount;
                $pathList[$pathCount] = $path;
                $pathCount++;
            }
        }

        $dateIds = [];
        $dateList = [];
        $dateCount = 0;
        $dateIdChars = [];
        for ($year = 20; $year <= 26; $year++) {
            for ($month = 1; $month <= 12; $month++) {
                $maxDay = ($month === 2) ? (($year % 4 === 0) ? 29 : 28) : (($month === 4 || $month === 6 || $month === 9 || $month === 11) ? 30 : 31);
                for ($day = 1; $day <= $maxDay; $day++) {
                    $date8 = \sprintf('%02d-%02d-%02d', $year, $month, $day);
                    $dateIds[$date8] = $dateCount;
                    $dateList[$dateCount] = '20' . $date8;
                    $dateIdChars[$date8] = \chr($dateCount & 0xFF) . \chr($dateCount >> 8);
                    $dateCount++;
                }
            }
        }

        $stride = $dateCount;
        $totalCells = $pathCount * $stride;
        $chunkSize = 1048576;

        if ($fileSize >= 10485760) {
            $ncpu = PHP_OS_FAMILY === 'Darwin'
                ? (int)\trim(\shell_exec('sysctl -n hw.ncpu'))
                : (int)(\trim(\shell_exec('nproc 2>/dev/null') ?: '8'));
            $numWorkers = $ncpu + 2;

            $handle = \fopen($inputPath, 'rb');
            $splits = [0];
            for ($s = 1; $s < $numWorkers; $s++) {
                \fseek($handle, (int)($fileSize * $s / $numWorkers));
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
                    $buckets = \array_fill(0, $pathCount, '');

                    \fseek($handle, $splits[$w]);
                    $remaining = $splits[$w + 1] - $splits[$w];

                    while ($remaining > 0) {
                            $toRead = $remaining < $chunkSize ? $remaining : $chunkSize;
                            $chunk = \fread($handle, $toRead);
                            if ($chunk === false || $chunk === '') break;
                            $chunkLen = \strlen($chunk);
                            $remaining -= $chunkLen;

                            $lastNl = \strrpos($chunk, "\n");
                            if ($lastNl === false) continue;

                            $tail = $chunkLen - $lastNl - 1;
                            if ($tail > 0) {
                                \fseek($handle, -$tail, SEEK_CUR);
                                $remaining += $tail;
                            }

                            $fence = $lastNl - 720;
                            $pos = 0;
                            while ($pos < $fence) {
                                $nlPos = \strpos($chunk, "\n", $pos + 54);
                                $buckets[$pathIds[\substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                                $pos = $nlPos + 1;
                                $nlPos = \strpos($chunk, "\n", $pos + 54);
                                $buckets[$pathIds[\substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                                $pos = $nlPos + 1;
                                $nlPos = \strpos($chunk, "\n", $pos + 54);
                                $buckets[$pathIds[\substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                                $pos = $nlPos + 1;
                                $nlPos = \strpos($chunk, "\n", $pos + 54);
                                $buckets[$pathIds[\substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                                $pos = $nlPos + 1;
                                $nlPos = \strpos($chunk, "\n", $pos + 54);
                                $buckets[$pathIds[\substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                                $pos = $nlPos + 1;
                                $nlPos = \strpos($chunk, "\n", $pos + 54);
                                $buckets[$pathIds[\substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                                $pos = $nlPos + 1;
                            }
                            while ($pos < $lastNl) {
                                $nlPos = \strpos($chunk, "\n", $pos + 54);
                                if ($nlPos === false || $nlPos > $lastNl) break;
                                $buckets[$pathIds[\substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                                $pos = $nlPos + 1;
                            }
                        }
                    \fclose($handle);

                    $counts = \array_fill(0, $totalCells, 0);
                    for ($p = 0; $p < $pathCount; $p++) {
                        if ($buckets[$p] === '') continue;
                        $offset = $p * $stride;
                        foreach (\array_count_values(\unpack('v*', $buckets[$p])) as $did => $cnt) {
                            $counts[$offset + $did] += $cnt;
                        }
                    }
                    \file_put_contents($tmpPrefix . $w, \pack('V*', ...$counts));
                    exit(0);
                }
                $childPids[$w] = $pid;
            }

            // Parent is worker 0: contiguous segment
            $handle = \fopen($inputPath, 'rb');
            \stream_set_read_buffer($handle, 0);
            $buckets = \array_fill(0, $pathCount, '');
            $remaining = $splits[1] - $splits[0];

            while ($remaining > 0) {
                $toRead = $remaining < $chunkSize ? $remaining : $chunkSize;
                $chunk = \fread($handle, $toRead);
                if ($chunk === false || $chunk === '') break;
                $chunkLen = \strlen($chunk);
                $remaining -= $chunkLen;

                $lastNl = \strrpos($chunk, "\n");
                if ($lastNl === false) continue;

                $tail = $chunkLen - $lastNl - 1;
                if ($tail > 0) {
                    \fseek($handle, -$tail, SEEK_CUR);
                    $remaining += $tail;
                }

                $fence = $lastNl - 720;
                $pos = 0;
                while ($pos < $fence) {
                    $nlPos = \strpos($chunk, "\n", $pos + 54);
                    $buckets[$pathIds[\substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                    $pos = $nlPos + 1;
                    $nlPos = \strpos($chunk, "\n", $pos + 54);
                    $buckets[$pathIds[\substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                    $pos = $nlPos + 1;
                    $nlPos = \strpos($chunk, "\n", $pos + 54);
                    $buckets[$pathIds[\substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                    $pos = $nlPos + 1;
                    $nlPos = \strpos($chunk, "\n", $pos + 54);
                    $buckets[$pathIds[\substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                    $pos = $nlPos + 1;
                    $nlPos = \strpos($chunk, "\n", $pos + 54);
                    $buckets[$pathIds[\substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                    $pos = $nlPos + 1;
                    $nlPos = \strpos($chunk, "\n", $pos + 54);
                    $buckets[$pathIds[\substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                    $pos = $nlPos + 1;
                }
                while ($pos < $lastNl) {
                    $nlPos = \strpos($chunk, "\n", $pos + 54);
                    if ($nlPos === false || $nlPos > $lastNl) break;
                    $buckets[$pathIds[\substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                    $pos = $nlPos + 1;
                }
            }
            \fclose($handle);

            $counts = \array_fill(0, $totalCells, 0);
            for ($p = 0; $p < $pathCount; $p++) {
                if ($buckets[$p] === '') continue;
                $offset = $p * $stride;
                foreach (\array_count_values(\unpack('v*', $buckets[$p])) as $did => $cnt) {
                    $counts[$offset + $did] += $cnt;
                }
            }
            unset($buckets);

            $escapedPaths = [];
            $datePrefixes = [];
            for ($p = 0; $p < $pathCount; $p++) {
                $escapedPaths[$p] = "\n    \"" . \str_replace('/', '\\/', $pathList[$p]) . "\": {";
            }
            for ($d = 0; $d < $dateCount; $d++) {
                $datePrefixes[$d] = "\n        \"" . $dateList[$d] . "\": ";
            }

            $remaining = \count($childPids);
            while ($remaining > 0) {
                $pid = \pcntl_wait($status);
                $w = \array_search($pid, $childPids);
                if ($w === false) continue;
                $raw = \file_get_contents($tmpPrefix . $w);
                @\unlink($tmpPrefix . $w);
                $j = 0;
                $len = \strlen($raw);
                for ($off = 0; $off < $len; $off += 65536) {
                    foreach (\unpack('V*', \substr($raw, $off, 65536)) as $v) {
                        $counts[$j] += $v;
                        $j++;
                    }
                }
                $remaining--;
            }

            $fp = \fopen($outputPath, 'wb');
            \stream_set_write_buffer($fp, 1048576);
            $buf = '{';
            $firstPath = true;
            for ($p = 0; $p < $pathCount; $p++) {
                $offset = $p * $stride;
                $firstD = -1;
                for ($d = 0; $d < $dateCount; $d++) {
                    if ($counts[$offset + $d] > 0) { $firstD = $d; break; }
                }
                if ($firstD === -1) continue;
                if (!$firstPath) $buf .= ',';
                $firstPath = false;
                $buf .= $escapedPaths[$p];
                $buf .= $datePrefixes[$firstD] . $counts[$offset + $firstD];
                for ($d = $firstD + 1; $d < $dateCount; $d++) {
                    if ($counts[$offset + $d] === 0) continue;
                    $buf .= ',' . $datePrefixes[$d] . $counts[$offset + $d];
                }
                $buf .= "\n    }";
                if (\strlen($buf) > 65536) { \fwrite($fp, $buf); $buf = ''; }
            }
            $buf .= "\n}";
            \fwrite($fp, $buf);
            \fclose($fp);
            return;
        }

        $pathOffsets = [];
        foreach ($pathIds as $path => $id) {
            $pathOffsets[$path] = $id * $stride;
        }

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
                $date = \substr($fullLine, $lineLen - 23, 8);
                if (isset($pathOffsets[$path]) && isset($dateIds[$date])) {
                    $counts[$pathOffsets[$path] + $dateIds[$date]]++;
                }
                $pos = $nlPos + 1;
            }
            while (true) {
                $nlPos = \strpos($chunk, "\n", $pos);
                if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                $counts[$pathOffsets[\substr($chunk, $pos + 19, $nlPos - $pos - 45)] + $dateIds[\substr($chunk, $nlPos - 23, 8)]]++;
                $pos = $nlPos + 1;
                $nlPos = \strpos($chunk, "\n", $pos);
                if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                $counts[$pathOffsets[\substr($chunk, $pos + 19, $nlPos - $pos - 45)] + $dateIds[\substr($chunk, $nlPos - 23, 8)]]++;
                $pos = $nlPos + 1;
                $nlPos = \strpos($chunk, "\n", $pos);
                if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                $counts[$pathOffsets[\substr($chunk, $pos + 19, $nlPos - $pos - 45)] + $dateIds[\substr($chunk, $nlPos - 23, 8)]]++;
                $pos = $nlPos + 1;
                $nlPos = \strpos($chunk, "\n", $pos);
                if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                $counts[$pathOffsets[\substr($chunk, $pos + 19, $nlPos - $pos - 45)] + $dateIds[\substr($chunk, $nlPos - 23, 8)]]++;
                $pos = $nlPos + 1;
                $nlPos = \strpos($chunk, "\n", $pos);
                if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                $counts[$pathOffsets[\substr($chunk, $pos + 19, $nlPos - $pos - 45)] + $dateIds[\substr($chunk, $nlPos - 23, 8)]]++;
                $pos = $nlPos + 1;
                $nlPos = \strpos($chunk, "\n", $pos);
                if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                $counts[$pathOffsets[\substr($chunk, $pos + 19, $nlPos - $pos - 45)] + $dateIds[\substr($chunk, $nlPos - 23, 8)]]++;
                $pos = $nlPos + 1;
            }
        }
        if ($leftover !== '') {
            $lineLen = \strlen($leftover);
            if ($lineLen >= 46) {
                $path = \substr($leftover, 19, $lineLen - 45);
                $date = \substr($leftover, $lineLen - 23, 8);
                if (isset($pathOffsets[$path]) && isset($dateIds[$date])) {
                    $counts[$pathOffsets[$path] + $dateIds[$date]]++;
                }
            }
        }
        \fclose($handle);

        $escapedPaths = [];
        $datePrefixes = [];
        for ($p = 0; $p < $pathCount; $p++) {
            $escapedPaths[$p] = "\n    \"" . \str_replace('/', '\\/', $pathList[$p]) . "\": {";
        }
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = "\n        \"" . $dateList[$d] . "\": ";
        }
        $fp = \fopen($outputPath, 'wb');
        \stream_set_write_buffer($fp, 1048576);
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
            $buf .= $escapedPaths[$p];
            $firstDate = true;
            for ($d = 0; $d < $dateCount; $d++) {
                $count = $counts[$offset + $d];
                if ($count === 0) continue;
                if (!$firstDate) $buf .= ',';
                $firstDate = false;
                $buf .= $datePrefixes[$d] . $count;
            }
            $buf .= "\n    }";
            if (\strlen($buf) > 65536) { \fwrite($fp, $buf); $buf = ''; }
        }
        $buf .= "\n}";
        \fwrite($fp, $buf);
        \fclose($fp);
    }
}
