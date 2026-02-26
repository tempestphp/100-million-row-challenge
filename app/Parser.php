<?php

declare(strict_types=1);

namespace App;

use App\Commands\Visit;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        \ini_set('memory_limit', '4G');
        \gc_disable();

        $fileSize = \filesize($inputPath);

        $handle = \fopen($inputPath, 'rb');
        $discoverChunk = \fread($handle, \min($fileSize, 16_777_216));
        \fclose($handle);

        $pathIds = [];
        $paths = [];
        $pathCount = 0;
        $dateSet = [];

        $lastNl = \strrpos($discoverChunk, "\n");
        if ($lastNl !== false) {
            $pos = 0;
            while ($pos < $lastNl) {
                $nlPos = \strpos($discoverChunk, "\n", $pos + 52);
                if ($nlPos === false) break;

                $slug = \substr($discoverChunk, $pos + 25, $nlPos - $pos - 51);
                if (!isset($pathIds[$slug])) {
                    $pathIds[$slug] = $pathCount;
                    $paths[$pathCount] = $slug;
                    $pathCount++;
                }

                $dateSet[\substr($discoverChunk, $nlPos - 23, 8)] = true;
                $pos = $nlPos + 1;
            }
        }
        unset($discoverChunk);

        foreach (Visit::all() as $visit) {
            $slug = \substr($visit->uri, 25);
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug] = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }
        }

        \ksort($dateSet);
        $dateIds = [];
        $dates = [];
        $dateCount = 0;
        $dateIdChars = [];
        foreach ($dateSet as $date => $_) {
            $dateIds[$date] = $dateCount;
            $dates[$dateCount] = $date;
            $dateIdChars[$date] = \chr($dateCount & 0xFF) . \chr($dateCount >> 8);
            $dateCount++;
        }
        unset($dateSet);

        $numWorkers = 10;
        $chunkSize = 1_048_576;

        $handle = \fopen($inputPath, 'rb');
        $splits = [0];
        for ($w = 1; $w < $numWorkers; $w++) {
            \fseek($handle, (int) ($fileSize * $w / $numWorkers));
            \fgets($handle);
            $splits[] = \ftell($handle);
        }
        $splits[] = $fileSize;
        \fclose($handle);

        $tmpDir = \sys_get_temp_dir();
        $tmpPrefix = $tmpDir . '/p_' . \getmypid() . '_';

        $childPids = [];
        for ($w = 1; $w < $numWorkers; $w++) {
            $pid = \pcntl_fork();
            if ($pid === -1) continue;
            if ($pid === 0) {
                $buckets = \array_fill(0, $pathCount, '');
                $handle = \fopen($inputPath, 'rb');
                \stream_set_read_buffer($handle, 0);
                \fseek($handle, $splits[$w]);
                $remaining = $splits[$w + 1] - $splits[$w];

                while ($remaining > 0) {
                    $chunk = \fread($handle, $remaining > $chunkSize ? $chunkSize : $remaining);
                    $chunkLen = \strlen($chunk);
                    if ($chunkLen === 0) break;
                    $remaining -= $chunkLen;

                    $lastNl = \strrpos($chunk, "\n");
                    if ($lastNl === false) break;

                    $tail = $chunkLen - $lastNl - 1;
                    if ($tail > 0) {
                        \fseek($handle, -$tail, SEEK_CUR);
                        $remaining += $tail;
                    }

                    $pos = 0;
                    $fence = $lastNl - 720;

                    while ($pos < $fence) {
                        $nlPos = \strpos($chunk, "\n", $pos + 52);
                        $buckets[$pathIds[\substr($chunk, $pos + 25, $nlPos - $pos - 51)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                        $pos = $nlPos + 1;

                        $nlPos = \strpos($chunk, "\n", $pos + 52);
                        $buckets[$pathIds[\substr($chunk, $pos + 25, $nlPos - $pos - 51)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                        $pos = $nlPos + 1;

                        $nlPos = \strpos($chunk, "\n", $pos + 52);
                        $buckets[$pathIds[\substr($chunk, $pos + 25, $nlPos - $pos - 51)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                        $pos = $nlPos + 1;

                        $nlPos = \strpos($chunk, "\n", $pos + 52);
                        $buckets[$pathIds[\substr($chunk, $pos + 25, $nlPos - $pos - 51)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                        $pos = $nlPos + 1;

                        $nlPos = \strpos($chunk, "\n", $pos + 52);
                        $buckets[$pathIds[\substr($chunk, $pos + 25, $nlPos - $pos - 51)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                        $pos = $nlPos + 1;

                        $nlPos = \strpos($chunk, "\n", $pos + 52);
                        $buckets[$pathIds[\substr($chunk, $pos + 25, $nlPos - $pos - 51)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                        $pos = $nlPos + 1;
                    }
                    while ($pos < $lastNl) {
                        $nlPos = \strpos($chunk, "\n", $pos + 52);
                        if ($nlPos === false) break;
                        $buckets[$pathIds[\substr($chunk, $pos + 25, $nlPos - $pos - 51)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                        $pos = $nlPos + 1;
                    }
                }

                \fclose($handle);

                $totalCells = $pathCount * $dateCount;
                $counts = \array_fill(0, $totalCells, 0);
                for ($p = 0; $p < $pathCount; $p++) {
                    if ($buckets[$p] === '') continue;
                    $offset = $p * $dateCount;
                    foreach (\array_count_values(\unpack('v*', $buckets[$p])) as $did => $cnt) {
                        $counts[$offset + $did] += $cnt;
                    }
                }

                \file_put_contents($tmpPrefix . $w, \pack('V*', ...$counts));
                exit(0);
            }
            $childPids[$w] = $pid;
        }

        $buckets = \array_fill(0, $pathCount, '');
        $handle = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($handle, 0);
        $remaining = $splits[1];

        while ($remaining > 0) {
            $chunk = \fread($handle, $remaining > $chunkSize ? $chunkSize : $remaining);
            $chunkLen = \strlen($chunk);
            if ($chunkLen === 0) break;
            $remaining -= $chunkLen;

            $lastNl = \strrpos($chunk, "\n");
            if ($lastNl === false) break;

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                \fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $pos = 0;
            $fence = $lastNl - 720;

            while ($pos < $fence) {
                $nlPos = \strpos($chunk, "\n", $pos + 52);
                $buckets[$pathIds[\substr($chunk, $pos + 25, $nlPos - $pos - 51)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                $pos = $nlPos + 1;

                $nlPos = \strpos($chunk, "\n", $pos + 52);
                $buckets[$pathIds[\substr($chunk, $pos + 25, $nlPos - $pos - 51)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                $pos = $nlPos + 1;

                $nlPos = \strpos($chunk, "\n", $pos + 52);
                $buckets[$pathIds[\substr($chunk, $pos + 25, $nlPos - $pos - 51)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                $pos = $nlPos + 1;

                $nlPos = \strpos($chunk, "\n", $pos + 52);
                $buckets[$pathIds[\substr($chunk, $pos + 25, $nlPos - $pos - 51)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                $pos = $nlPos + 1;

                $nlPos = \strpos($chunk, "\n", $pos + 52);
                $buckets[$pathIds[\substr($chunk, $pos + 25, $nlPos - $pos - 51)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                $pos = $nlPos + 1;

                $nlPos = \strpos($chunk, "\n", $pos + 52);
                $buckets[$pathIds[\substr($chunk, $pos + 25, $nlPos - $pos - 51)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                $pos = $nlPos + 1;
            }
            while ($pos < $lastNl) {
                $nlPos = \strpos($chunk, "\n", $pos + 52);
                if ($nlPos === false) break;
                $buckets[$pathIds[\substr($chunk, $pos + 25, $nlPos - $pos - 51)]] .= $dateIdChars[\substr($chunk, $nlPos - 23, 8)];
                $pos = $nlPos + 1;
            }
        }
        \fclose($handle);

        $totalCells = $pathCount * $dateCount;
        $counts = \array_fill(0, $totalCells, 0);
        for ($p = 0; $p < $pathCount; $p++) {
            if ($buckets[$p] === '') continue;
            $offset = $p * $dateCount;
            foreach (\array_count_values(\unpack('v*', $buckets[$p])) as $did => $cnt) {
                $counts[$offset + $did] += $cnt;
            }
        }
        unset($buckets);

        $pidToWorker = \array_flip($childPids);
        $remainingW = \count($childPids);
        while ($remainingW > 0) {
            $pid = \pcntl_wait($status);
            $w = $pidToWorker[$pid];
            $raw = \file_get_contents($tmpPrefix . $w);
            @\unlink($tmpPrefix . $w);
            $j = 0;
            foreach (\unpack('V*', $raw) as $v) {
                $counts[$j++] += $v;
            }
            $remainingW--;
        }

        $datePrefixes = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "20' . $dates[$d] . '": ';
        }

        $escapedPaths = [];
        for ($p = 0; $p < $pathCount; $p++) {
            $escapedPaths[$p] = "\"\\/blog\\/" . \str_replace('/', '\\/', $paths[$p]) . "\"";
        }

        $fp = \fopen($outputPath, 'wb');
        \stream_set_write_buffer($fp, 1_048_576);
        \fwrite($fp, '{');
        $firstPath = true;

        foreach ($escapedPaths as $p => $escapedPath) {
            $base = $p * $dateCount;
            $buf = '';
            $sep = '';

            foreach ($datePrefixes as $d => $prefix) {
                $count = $counts[$base + $d];
                if ($count === 0) continue;
                $buf .= $sep . $prefix . $count;
                $sep = ",\n";
            }

            if ($buf === '') continue;

            \fwrite($fp, ($firstPath ? '' : ',') . "\n    " . $escapedPath . ": {\n" . $buf . "\n    }");
            $firstPath = false;
        }

        \fwrite($fp, "\n}");
        \fclose($fp);
    }
}
