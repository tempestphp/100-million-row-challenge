<?php

namespace App;

use App\Commands\Visit;

use function strpos;
use function strrpos;
use function substr;
use function strlen;
use function fopen;
use function fclose;
use function fread;
use function fseek;
use function ftell;
use function fgets;
use function fwrite;
use function filesize;
use function gc_disable;
use function proc_nice;
use function pcntl_fork;
use function pcntl_wait;
use function pack;
use function unpack;
use function chr;
use function array_fill;
use function array_count_values;
use function count;
use function str_replace;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function sys_get_temp_dir;
use function file_get_contents;
use function file_put_contents;
use function getmypid;
use function unlink;
use const SEEK_CUR;
use const WNOHANG;

final class Parser
{
    private const WORKERS = 10;
    private const READ_CHUNK = 163_840;

    public static function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $fileSize = filesize($inputPath);

        // Pre-enumerate all possible dates 2021-2026 (no discovery needed)
        $dateIdChars = [];
        $dates = [];
        $dateCount = 0;
        for ($y = 21; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => $y === 24 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $mStr = ($m < 10 ? '0' : '') . $m;
                $ymStr = "{$y}-{$mStr}-";
                for ($d = 1; $d <= $maxD; $d++) {
                    $key = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $dateIdChars[$key] = chr($dateCount & 0xFF) . chr($dateCount >> 8);
                    $dates[$dateCount] = $key;
                    $dateCount++;
                }
            }
        }

        // Discover slugs from 2MB sample
        $pathIds = [];
        $paths = [];
        $pathCount = 0;

        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);
        $sample = fread($fh, 2_097_152);
        fclose($fh);

        $lastNl = strrpos($sample, "\n");
        $pos = 0;
        while ($pos < $lastNl) {
            $nlPos = strpos($sample, "\n", $pos + 52);
            if ($nlPos === false) break;
            $slug = substr($sample, $pos + 25, $nlPos - $pos - 51);
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug] = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }
            $pos = $nlPos + 1;
        }
        unset($sample);

        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, 25);
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug] = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }
        }

        // Split file into worker chunks
        $boundaries = [0];
        $fh = fopen($inputPath, 'rb');
        for ($i = 1; $i < self::WORKERS; $i++) {
            fseek($fh, (int)($fileSize * $i / self::WORKERS));
            fgets($fh);
            $boundaries[] = ftell($fh);
        }
        fclose($fh);
        $boundaries[] = $fileSize;

        // Pre-warm file cache at chunk boundaries
        $fh = fopen($inputPath, 'rb');
        for ($i = 0; $i <= self::WORKERS; $i++) {
            fseek($fh, $boundaries[$i]);
            fread($fh, 4096);
        }
        fclose($fh);

        $oldHandler = set_error_handler(function() { return true; });
        proc_nice(-20);
        set_error_handler($oldHandler);

        // Fork WORKERS-1 children for chunks 0..WORKERS-2
        $tmpDir = sys_get_temp_dir();
        $myPid = getmypid();
        $childMap = [];

        for ($w = 0; $w < self::WORKERS - 1; $w++) {
            $tmpFile = "{$tmpDir}/p100m_{$myPid}_{$w}";
            $pid = pcntl_fork();
            if ($pid === 0) {
                $wCounts = self::parseRange(
                    $inputPath, $boundaries[$w], $boundaries[$w + 1],
                    $pathIds, $dateIdChars, $pathCount, $dateCount,
                );
                file_put_contents($tmpFile, pack('v*', ...$wCounts));
                exit(0);
            }
            $childMap[$pid] = $tmpFile;
        }

        // Parent parses last chunk directly
        $counts = self::parseRange(
            $inputPath, $boundaries[self::WORKERS - 1], $boundaries[self::WORKERS],
            $pathIds, $dateIdChars, $pathCount, $dateCount,
        );

        // Merge children
        $pending = count($childMap);
        while ($pending > 0) {
            $pid = pcntl_wait($status, WNOHANG);
            if ($pid <= 0) {
                $pid = pcntl_wait($status);
            }
            $tmpFile = $childMap[$pid];
            $wCounts = unpack('v*', file_get_contents($tmpFile));
            unlink($tmpFile);
            $j = 0;
            foreach ($wCounts as $v) {
                $counts[$j] += $v;
                $j++;
            }
            $pending--;
        }

        self::writeJson($outputPath, $counts, $paths, $dates, $dateCount);
    }

    private static function parseRange(
        $inputPath, $start, $end,
        $pathIds, $dateIdChars,
        $pathCount, $dateCount,
    ) {
        $buckets = array_fill(0, $pathCount, '');
        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);
        fseek($fh, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $chunk = fread($fh, $remaining > self::READ_CHUNK ? self::READ_CHUNK : $remaining);
            $cLen = strlen($chunk);
            $remaining -= $cLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) break;

            $tail = $cLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($fh, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $p = 25;
            $fence = $lastNl - 600;

            while ($p < $fence) {
                $sep = strpos($chunk, ',', $p + 4);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p + 4);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p + 4);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p + 4);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p + 4);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;
            }

            while ($p < $lastNl) {
                $sep = strpos($chunk, ',', $p + 4);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;
            }
        }

        fclose($fh);

        // Convert buckets to flat counts
        $counts = array_fill(0, $pathCount * $dateCount, 0);
        for ($s = 0; $s < $pathCount; $s++) {
            if ($buckets[$s] === '') continue;
            $base = $s * $dateCount;
            foreach (array_count_values(unpack('v*', $buckets[$s])) as $did => $n) {
                $counts[$base + $did] += $n;
            }
        }

        return $counts;
    }

    private static function writeJson(
        $outputPath, $counts,
        $paths, $dates, $dateCount,
    ) {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);
        fwrite($out, '{');

        $datePrefixes = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "20' . $dates[$d] . '": ';
        }

        $pathCount = count($paths);
        $escapedPaths = [];
        for ($p = 0; $p < $pathCount; $p++) {
            $escapedPaths[$p] = '"\\/blog\\/' . str_replace('/', '\\/', $paths[$p]) . '"';
        }

        $first = true;

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

            fwrite($out, ($first ? "\n    " : ",\n    ") . $escapedPaths[$p] . ": {\n" . $body . "\n    }");
            $first = false;
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
