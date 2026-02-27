<?php

namespace App;

use function array_count_values;
use function array_fill;
use function chr;
use function count;
use function fclose;
use function fgets;
use function file_get_contents;
use function file_put_contents;
use function filesize;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function getmypid;
use function ini_set;
use function ksort;
use function pack;
use function pcntl_fork;
use function pcntl_wait;
use function str_replace;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function sys_get_temp_dir;
use function unlink;
use function unpack;

use const SEEK_CUR;
use const WNOHANG;


final class Parser
{
    public function parse($inputPath, $outputPath)
    {
        gc_disable();
        ini_set('memory_limit', '-1');

        $fileSize = filesize($inputPath);

        $sampleSize = $fileSize > 16_777_216 ? 16_777_216 : $fileSize;
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $sample = fread($handle, $sampleSize);
        fclose($handle);

        $pathIds = [];
        $paths = [];
        $pathCount = 0;
        $discoveredDates = [];

        $lastNl = strrpos($sample, "\n");
        if ($lastNl !== false) {
            $pos = 0;
            while ($pos < $lastNl) {
                $nl = strpos($sample, "\n", $pos + 52);
                if ($nl === false) break;
                $slug = substr($sample, $pos + 25, $nl - $pos - 51);
                if (!isset($pathIds[$slug])) {
                    $pathIds[$slug] = $pathCount;
                    $paths[$pathCount] = $slug;
                    $pathCount++;
                }
                $discoveredDates[substr($sample, $nl - 23, 8)] = true;
                $pos = $nl + 1;
            }
        }
        unset($sample);

        ksort($discoveredDates);

        $dateChars = [];
        $dates = [];
        $dateCount = 0;
        foreach ($discoveredDates as $key => $_) {
            $dateChars[$key] = chr($dateCount & 0xFF) . chr($dateCount >> 8);
            $dates[$dateCount] = $key;
            $dateCount++;
        }
        unset($discoveredDates);

        $numWorkers = 12;

        $splits = [0];
        $handle = fopen($inputPath, 'rb');
        for ($w = 1; $w < $numWorkers; $w++) {
            fseek($handle, (int) ($fileSize * $w / $numWorkers));
            fgets($handle);
            $splits[] = ftell($handle);
        }
        $splits[] = $fileSize;
        fclose($handle);

        $tmpDir = sys_get_temp_dir();
        $myPid = getmypid();
        $totalCells = $pathCount * $dateCount;

        $childMap = [];
        for ($w = 0; $w < $numWorkers; $w++) {
            $tmpFile = $tmpDir . '/p_' . $myPid . '_' . $w;
            $pid = pcntl_fork();
            if ($pid === -1) continue;
            if ($pid === 0) {
                gc_disable();
                ini_set('memory_limit', '-1');
                $wCounts = $this->parseRange(
                    $inputPath, $splits[$w], $splits[$w + 1],
                    $pathIds, $dateChars, $pathCount, $dateCount,
                );
                file_put_contents($tmpFile, pack('v*', ...$wCounts));
                exit(0);
            }
            $childMap[$pid] = $tmpFile;
        }

        $counts = array_fill(0, $totalCells, 0);
        $pending = count($childMap);
        while ($pending > 0) {
            $pid = pcntl_wait($status, WNOHANG);
            if ($pid <= 0) {
                $pid = pcntl_wait($status);
            }
            if (!isset($childMap[$pid])) continue;
            $tmpFile = $childMap[$pid];
            $wCounts = unpack('v*', file_get_contents($tmpFile));
            unlink($tmpFile);
            $j = 0;
            foreach ($wCounts as $v) {
                $counts[$j++] += $v;
            }
            $pending--;
        }

        $datePrefixes = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "20' . $dates[$d] . '": ';
        }

        $escapedPaths = [];
        for ($p = 0; $p < $pathCount; $p++) {
            $escapedPaths[$p] = '"\\/blog\\/' . str_replace('/', '\\/', $paths[$p]) . '"';
        }

        $fp = fopen($outputPath, 'wb');
        stream_set_write_buffer($fp, 1_048_576);
        fwrite($fp, '{');
        $firstPath = true;

        for ($p = 0; $p < $pathCount; $p++) {
            $base = $p * $dateCount;
            $body = '';
            $sep = '';

            for ($d = 0; $d < $dateCount; $d++) {
                $n = $counts[$base + $d];
                if ($n === 0) continue;
                $body .= $sep . $datePrefixes[$d] . $n;
                $sep = ",\n";
            }

            if ($body === '') continue;

            fwrite($fp, ($firstPath ? '' : ',') . "\n    " . $escapedPaths[$p] . ": {\n" . $body . "\n    }");
            $firstPath = false;
        }

        fwrite($fp, "\n}");
        fclose($fp);
    }

    private function parseRange(
        $inputPath, $start, $end,
        $pathIds, $dateChars, $pathCount, $dateCount,
    ) {
        $chunkSize = 163_840;
        $buckets = array_fill(0, $pathCount, '');
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $chunk = fread($handle, $remaining > $chunkSize ? $chunkSize : $remaining);
            $chunkLen = strlen($chunk);
            if ($chunkLen === 0) break;
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) break;

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $p = 0;
            $fence = $lastNl - 600;

            while ($p < $fence) {
                $nl = strpos($chunk, "\n", $p + 52);
                $buckets[$pathIds[substr($chunk, $p + 25, $nl - $p - 51)]] .= $dateChars[substr($chunk, $nl - 23, 8)];
                $p = $nl + 1;

                $nl = strpos($chunk, "\n", $p + 52);
                $buckets[$pathIds[substr($chunk, $p + 25, $nl - $p - 51)]] .= $dateChars[substr($chunk, $nl - 23, 8)];
                $p = $nl + 1;

                $nl = strpos($chunk, "\n", $p + 52);
                $buckets[$pathIds[substr($chunk, $p + 25, $nl - $p - 51)]] .= $dateChars[substr($chunk, $nl - 23, 8)];
                $p = $nl + 1;

                $nl = strpos($chunk, "\n", $p + 52);
                $buckets[$pathIds[substr($chunk, $p + 25, $nl - $p - 51)]] .= $dateChars[substr($chunk, $nl - 23, 8)];
                $p = $nl + 1;

                $nl = strpos($chunk, "\n", $p + 52);
                $buckets[$pathIds[substr($chunk, $p + 25, $nl - $p - 51)]] .= $dateChars[substr($chunk, $nl - 23, 8)];
                $p = $nl + 1;

                $nl = strpos($chunk, "\n", $p + 52);
                $buckets[$pathIds[substr($chunk, $p + 25, $nl - $p - 51)]] .= $dateChars[substr($chunk, $nl - 23, 8)];
                $p = $nl + 1;
            }
            while ($p < $lastNl) {
                $nl = strpos($chunk, "\n", $p + 52);
                if ($nl === false) break;
                $buckets[$pathIds[substr($chunk, $p + 25, $nl - $p - 51)]] .= $dateChars[substr($chunk, $nl - 23, 8)];
                $p = $nl + 1;
            }
        }

        fclose($handle);

        $counts = array_fill(0, $pathCount * $dateCount, 0);
        for ($p = 0; $p < $pathCount; $p++) {
            if ($buckets[$p] === '') continue;
            $offset = $p * $dateCount;
            foreach (array_count_values(unpack('v*', $buckets[$p])) as $did => $cnt) {
                $counts[$offset + $did] += $cnt;
            }
        }

        return $counts;
    }
}
