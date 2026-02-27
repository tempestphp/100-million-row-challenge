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
            $dateSet[\substr($discoverChunk, $nlPos - 23, 8)] = true;
            $pos = $nlPos + 1;
        }
        unset($discoverChunk);

        \ksort($dateSet);
        $dateIds = [];
        $dateList = [];
        $dateCount = 0;
        foreach ($dateSet as $date8 => $_) {
            $dateIds[$date8] = $dateCount;
            $dateList[$dateCount] = '20' . $date8;
            $dateCount++;
        }
        unset($dateSet);

        $stride = $dateCount;
        $totalCells = $pathCount * $stride;
        $chunkSize = 262144;

        $dateIdChars = [];
        foreach ($dateIds as $date => $id) {
            $dateIdChars[$date] = \chr($id & 0xFF) . \chr($id >> 8);
        }

        $byLen = [];
        foreach ($pathIds as $path => $id) {
            $byLen[\strlen($path)][] = ['p' => $path, 'id' => $id];
        }
        $dp1 = \array_fill(0, 100, 19);
        $dp2 = \array_fill(0, 100, 20);
        $fastP = [];
        foreach ($byLen as $len => $group) {
            $gc = \count($group);
            if ($gc === 1) {
                $dp1[$len] = 19; $dp2[$len] = 20;
                $g = $group[0];
                $fastP[$len][$g['p'][0]][$g['p'][1]] = $g['id'];
                continue;
            }
            $best1 = 0; $best2 = 1; $bestScore = 0;
            for ($i = 0; $i < $len && $bestScore < $gc; $i++) {
                for ($j = $i + 1; $j < $len; $j++) {
                    $seen = [];
                    foreach ($group as $g) {
                        $k = $g['p'][$i] . $g['p'][$j];
                        $seen[$k] = ($seen[$k] ?? 0) + 1;
                    }
                    $u = 0;
                    foreach ($seen as $c) if ($c === 1) $u++;
                    if ($u > $bestScore) { $bestScore = $u; $best1 = $i; $best2 = $j; }
                    if ($u === $gc) break 2;
                }
            }
            $dp1[$len] = 19 + $best1;
            $dp2[$len] = 19 + $best2;
            foreach ($group as $g) {
                $b1 = $g['p'][$best1]; $b2 = $g['p'][$best2];
                $unique = true;
                foreach ($group as $g2) {
                    if ($g2['id'] !== $g['id'] && $g2['p'][$best1] === $b1 && $g2['p'][$best2] === $b2) {
                        $unique = false; break;
                    }
                }
                if ($unique) $fastP[$len][$b1][$b2] = $g['id'];
            }
        }
        unset($byLen);

        if ($fileSize >= 10485760) {
            if (($envW = \getenv('WORKERS')) !== false) {
                $numWorkers = (int)$envW;
            } elseif (PHP_OS_FAMILY === 'Darwin') {
                $numWorkers = (int)\trim(\shell_exec('sysctl -n hw.ncpu')) + 2;
            } else {
                $numWorkers = (int)(\trim(\shell_exec('nproc 2>/dev/null') ?: '8'));
            }

            $numSegments = $numWorkers * 5;
            $handle = \fopen($inputPath, 'rb');
            $splits = [0];
            for ($s = 1; $s < $numSegments; $s++) {
                \fseek($handle, (int)($fileSize * $s / $numSegments));
                \fgets($handle);
                $splits[] = \ftell($handle);
            }
            $splits[] = $fileSize;
            \fclose($handle);

            $tmpDir = \is_dir('/dev/shm') ? '/dev/shm' : \sys_get_temp_dir();
            $tmpPrefix = $tmpDir . '/p_' . \getmypid() . '_';
            $counterFile = $tmpPrefix . 'ctr';
            \file_put_contents($counterFile, \pack('V', 0));

            $childPids = [];
            for ($w = 1; $w < $numWorkers; $w++) {
                $pid = \pcntl_fork();
                if ($pid === -1) continue;
                if ($pid === 0) {
                    $handle = \fopen($inputPath, 'rb');
                    \stream_set_read_buffer($handle, 0);
                    $buckets = \array_fill(0, $pathCount, '');
                    $lockFp = \fopen($counterFile, 'r+b');

                    while (true) {
                        \flock($lockFp, LOCK_EX);
                        \fseek($lockFp, 0);
                        $segId = \unpack('V', \fread($lockFp, 4))[1];
                        if ($segId >= $numSegments) { \flock($lockFp, LOCK_UN); break; }
                        \fseek($lockFp, 0);
                        \fwrite($lockFp, \pack('V', $segId + 1));
                        \fflush($lockFp);
                        \flock($lockFp, LOCK_UN);

                        \fseek($handle, $splits[$segId]);
                        $leftover = '';
                        $remaining = $splits[$segId + 1] - $splits[$segId];
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
                                if (isset($pathIds[$path]) && isset($dateIdChars[$date])) {
                                    $buckets[$pathIds[$path]] .= $dateIdChars[$date];
                                }
                                $pos = $nlPos + 1;
                            }
                            while (true) {
                                $nlPos = \strpos($chunk, "\n", $pos);
                                if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                                $pL = $nlPos - $pos - 45;
                                $buckets[$fastP[$pL][$chunk[$pos + $dp1[$pL]]][$chunk[$pos + $dp2[$pL]]] ?? $pathIds[\substr($chunk, $pos + 19, $pL)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                                $pos = $nlPos + 1;
                                $nlPos = \strpos($chunk, "\n", $pos);
                                if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                                $pL = $nlPos - $pos - 45;
                                $buckets[$fastP[$pL][$chunk[$pos + $dp1[$pL]]][$chunk[$pos + $dp2[$pL]]] ?? $pathIds[\substr($chunk, $pos + 19, $pL)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                                $pos = $nlPos + 1;
                                $nlPos = \strpos($chunk, "\n", $pos);
                                if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                                $pL = $nlPos - $pos - 45;
                                $buckets[$fastP[$pL][$chunk[$pos + $dp1[$pL]]][$chunk[$pos + $dp2[$pL]]] ?? $pathIds[\substr($chunk, $pos + 19, $pL)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                                $pos = $nlPos + 1;
                                $nlPos = \strpos($chunk, "\n", $pos);
                                if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                                $pL = $nlPos - $pos - 45;
                                $buckets[$fastP[$pL][$chunk[$pos + $dp1[$pL]]][$chunk[$pos + $dp2[$pL]]] ?? $pathIds[\substr($chunk, $pos + 19, $pL)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                                $pos = $nlPos + 1;
                                $nlPos = \strpos($chunk, "\n", $pos);
                                if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                                $pL = $nlPos - $pos - 45;
                                $buckets[$fastP[$pL][$chunk[$pos + $dp1[$pL]]][$chunk[$pos + $dp2[$pL]]] ?? $pathIds[\substr($chunk, $pos + 19, $pL)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                                $pos = $nlPos + 1;
                                $nlPos = \strpos($chunk, "\n", $pos);
                                if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                                $pL = $nlPos - $pos - 45;
                                $buckets[$fastP[$pL][$chunk[$pos + $dp1[$pL]]][$chunk[$pos + $dp2[$pL]]] ?? $pathIds[\substr($chunk, $pos + 19, $pL)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                                $pos = $nlPos + 1;
                            }
                        }
                    }
                    if ($leftover !== '') {
                        $lineLen = \strlen($leftover);
                        if ($lineLen >= 46) {
                            $path = \substr($leftover, 19, $lineLen - 45);
                            $date = \substr($leftover, $lineLen - 23, 8);
                            if (isset($pathIds[$path]) && isset($dateIdChars[$date])) {
                                $buckets[$pathIds[$path]] .= $dateIdChars[$date];
                            }
                        }
                    }
                    \fclose($lockFp);
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

            $handle = \fopen($inputPath, 'rb');
            \stream_set_read_buffer($handle, 0);
            $buckets = \array_fill(0, $pathCount, '');
            $leftover = '';
            $lockFp = \fopen($counterFile, 'r+b');

            while (true) {
                \flock($lockFp, LOCK_EX);
                \fseek($lockFp, 0);
                $segId = \unpack('V', \fread($lockFp, 4))[1];
                if ($segId >= $numSegments) { \flock($lockFp, LOCK_UN); break; }
                \fseek($lockFp, 0);
                \fwrite($lockFp, \pack('V', $segId + 1));
                \fflush($lockFp);
                \flock($lockFp, LOCK_UN);

                \fseek($handle, $splits[$segId]);
                $leftover = '';
                $remaining = $splits[$segId + 1] - $splits[$segId];
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
                        if (isset($pathIds[$path]) && isset($dateIdChars[$date])) {
                            $buckets[$pathIds[$path]] .= $dateIdChars[$date];
                        }
                        $pos = $nlPos + 1;
                    }
                    while (true) {
                        $nlPos = \strpos($chunk, "\n", $pos);
                        if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                        $pL = $nlPos - $pos - 45;
                        $buckets[$fastP[$pL][$chunk[$pos + $dp1[$pL]]][$chunk[$pos + $dp2[$pL]]] ?? $pathIds[\substr($chunk, $pos + 19, $pL)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                        $pos = $nlPos + 1;
                        $nlPos = \strpos($chunk, "\n", $pos);
                        if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                        $pL = $nlPos - $pos - 45;
                        $buckets[$fastP[$pL][$chunk[$pos + $dp1[$pL]]][$chunk[$pos + $dp2[$pL]]] ?? $pathIds[\substr($chunk, $pos + 19, $pL)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                        $pos = $nlPos + 1;
                        $nlPos = \strpos($chunk, "\n", $pos);
                        if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                        $pL = $nlPos - $pos - 45;
                        $buckets[$fastP[$pL][$chunk[$pos + $dp1[$pL]]][$chunk[$pos + $dp2[$pL]]] ?? $pathIds[\substr($chunk, $pos + 19, $pL)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                        $pos = $nlPos + 1;
                        $nlPos = \strpos($chunk, "\n", $pos);
                        if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                        $pL = $nlPos - $pos - 45;
                        $buckets[$fastP[$pL][$chunk[$pos + $dp1[$pL]]][$chunk[$pos + $dp2[$pL]]] ?? $pathIds[\substr($chunk, $pos + 19, $pL)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                        $pos = $nlPos + 1;
                        $nlPos = \strpos($chunk, "\n", $pos);
                        if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                        $pL = $nlPos - $pos - 45;
                        $buckets[$fastP[$pL][$chunk[$pos + $dp1[$pL]]][$chunk[$pos + $dp2[$pL]]] ?? $pathIds[\substr($chunk, $pos + 19, $pL)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                        $pos = $nlPos + 1;
                        $nlPos = \strpos($chunk, "\n", $pos);
                        if ($nlPos === false) { $leftover = \substr($chunk, $pos); break; }
                        $pL = $nlPos - $pos - 45;
                        $buckets[$fastP[$pL][$chunk[$pos + $dp1[$pL]]][$chunk[$pos + $dp2[$pL]]] ?? $pathIds[\substr($chunk, $pos + 19, $pL)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                        $pos = $nlPos + 1;
                    }
                }
            }
            if ($leftover !== '') {
                $lineLen = \strlen($leftover);
                if ($lineLen >= 46) {
                    $path = \substr($leftover, 19, $lineLen - 45);
                    $date = \substr($leftover, $lineLen - 23, 8);
                    if (isset($pathIds[$path]) && isset($dateIdChars[$date])) {
                        $buckets[$pathIds[$path]] .= $dateIdChars[$date];
                    }
                }
            }
            \fclose($lockFp);
            \fclose($handle);
            @\unlink($counterFile);

            $counts = \array_fill(0, $totalCells, 0);
            for ($p = 0; $p < $pathCount; $p++) {
                if ($buckets[$p] === '') continue;
                $offset = $p * $stride;
                foreach (\array_count_values(\unpack('v*', $buckets[$p])) as $did => $cnt) {
                    $counts[$offset + $did] += $cnt;
                }
            }
            unset($buckets);

            $remaining = \count($childPids);
            while ($remaining > 0) {
                $pid = \pcntl_wait($status);
                $w = \array_search($pid, $childPids);
                if ($w === false) continue;
                $raw = \file_get_contents($tmpPrefix . $w);
                @\unlink($tmpPrefix . $w);
                $j = 0;
                foreach (\unpack('V*', $raw) as $v) {
                    $counts[$j] += $v;
                    $j++;
                }
                $remaining--;
            }

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
