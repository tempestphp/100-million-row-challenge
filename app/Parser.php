<?php

namespace App;

\gc_disable();

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
use function getmypid;
use function implode;
use function ini_set;
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
        ini_set('memory_limit', '-1');

        $fileSize = filesize($inputPath);

        $dateChars = [];
        $dates = [];
        $dateCount = 0;
        for ($y = 21; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => $y === 24 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $mStr = $m < 10 ? "0{$m}" : (string) $m;
                $ymStr = "{$y}-{$mStr}-";
                for ($d = 1; $d <= $maxD; $d++) {
                    $key = $ymStr . ($d < 10 ? "0{$d}" : (string) $d);
                    $dateChars[$key] = chr($dateCount & 0xFF) . chr($dateCount >> 8);
                    $dates[$dateCount] = $key;
                    $dateCount++;
                }
            }
        }

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $sample = fread($handle, 131_072);
        fclose($handle);

        $pathIds = [];
        $paths = [];
        $pathCount = 0;

        $lastNl = strrpos($sample, "\n");
        if ($lastNl !== false) {
            $p = 25;
            while ($p < $lastNl) {
                $sep = strpos($sample, ',', $p);
                if ($sep === false) break;
                $slug = substr($sample, $p, $sep - $p);
                if (!isset($pathIds[$slug])) {
                    $pathIds[$slug] = $pathCount;
                    $paths[$pathCount] = $slug;
                    $pathCount++;
                }
                $p = $sep + 52;
            }
        }
        unset($sample);

        $numWorkers = 10;

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

        $childMap = [];
        for ($w = 0; $w < $numWorkers - 1; $w++) {
            $tmpFile = $tmpDir . '/p_' . $myPid . '_' . $w;
            $pid = pcntl_fork();
            if ($pid === -1) continue;
            if ($pid === 0) {
                $wCounts = self::parseRange(
                    $inputPath, $splits[$w], $splits[$w + 1],
                    $pathIds, $dateChars, $pathCount, $dateCount,
                );
                file_put_contents($tmpFile, pack('v*', ...$wCounts));
                exit(0);
            }
            $childMap[$pid] = $tmpFile;
        }

        $counts = self::parseRange(
            $inputPath, $splits[$numWorkers - 1], $splits[$numWorkers],
            $pathIds, $dateChars, $pathCount, $dateCount,
        );

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

        self::writeJson($outputPath, $counts, $paths, $pathCount, $dates, $dateCount);
    }

    private static function parseRange(
        $inputPath, $start, $end,
        $pathIds, $dateChars, $pathCount, $dateCount,
    ) {
        $buckets = array_fill(0, $pathCount, '');
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $chunk = fread($handle, $remaining > 163_840 ? 163_840 : $remaining);
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

            $p = 25;
            $fence = $lastNl - 606;

            while ($p < $fence) {
                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;
            }

            while ($p < $lastNl) {
                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;
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

    private static function writeJson($outputPath, $counts, $paths, $pathCount, $dates, $dateCount)
    {
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
            $dateEntries = [];

            for ($d = 0; $d < $dateCount; $d++) {
                $n = $counts[$base + $d];
                if ($n === 0) continue;
                $dateEntries[] = $datePrefixes[$d] . $n;
            }

            if ($dateEntries === []) continue;

            $buf = $firstPath ? "\n    " : ",\n    ";
            $firstPath = false;
            $buf .= $escapedPaths[$p] . ": {\n" . implode(",\n", $dateEntries) . "\n    }";
            fwrite($fp, $buf);
        }

        fwrite($fp, "\n}");
        fclose($fp);
    }
}
