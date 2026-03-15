<?php

namespace App;

use function array_fill;
use function fclose;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use const SEEK_CUR;
use const SEEK_END;

final class Parser
{
    public static function parse($inputPath, $outputPath)
    {
        gc_disable();

        $dateIds = [];
        $datePrefixes = [];
        $dateCount = 0;
        for ($y = 1; $y <= 6; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => $y === 4 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $mStr = ($m < 10 ? '0' : '') . $m;
                $ymStr = "{$y}-{$mStr}-";
                for ($d = 1; $d <= $maxD; $d++) {
                    $key = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $dateIds[$key] = $dateCount;
                    $datePrefixes[$dateCount] = '        "202' . $key . '": ';
                    $dateCount++;
                }
            }
        }

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $raw = fread($handle, 142000);

        $paths = [];
        $slugBaseMap = [];
        $slugTotal = 0;
        $pos = 0;
        while ($slugTotal < 268) {
            $nl = strpos($raw, "\n", $pos + 52);
            if ($nl === false) {
                break;
            }

            $slug = substr($raw, $pos + 25, $nl - $pos - 51);
            if (! isset($slugBaseMap[$slug])) {
                $paths[$slugTotal] = $slug;
                $slugBaseMap[$slug] = $slugTotal * $dateCount;
                $slugTotal++;
            }

            $pos = $nl + 1;
        }

        $prefix = 'https://stitcher.io/blog/';
        $slugBaseMap = [];
        for ($p = 0; $p < $slugTotal; $p++) {
            $stride = strlen($paths[$p]) + 52;
            $slugBaseMap[substr($prefix . $paths[$p], -22)] = ($stride << 20) | ($p * $dateCount);
        }

        $outputSize = $slugTotal * $dateCount;
        $counts = array_fill(0, $outputSize, 0);

        fseek($handle, 0, SEEK_END);
        $left = ftell($handle);
        fseek($handle, 0);

        while ($left > 0) {
            $chunk = fread($handle, $left > 131_072 ? 131_072 : $left);
            $chunkLen = strlen($chunk);
            if ($chunkLen === 0) {
                break;
            }

            $left -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) {
                break;
            }

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
                $left += $tail;
            }

            $p = $lastNl;

            while ($p > 1248) {
                $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20;
                $counts[$idx]++;

                $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20;
                $counts[$idx]++;

                $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20;
                $counts[$idx]++;

                $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20;
                $counts[$idx]++;

                $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20;
                $counts[$idx]++;

                $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20;
                $counts[$idx]++;

                $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20;
                $counts[$idx]++;

                $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20;
                $counts[$idx]++;

                $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20;
                $counts[$idx]++;

                $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20;
                $counts[$idx]++;

                $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20;
                $counts[$idx]++;

                $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20;
                $counts[$idx]++;
            }

            while ($p >= 48) {
                $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20;
                $counts[$idx]++;
            }
        }

        fclose($handle);

        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 2097152);
        fwrite($out, '{');

        $escapedPaths = [];
        for ($p = 0; $p < $slugTotal; $p++) {
            $escapedPaths[$p] = '"\/blog\/' . $paths[$p] . '": {';
        }

        $sep = "\n    ";
        $base = 0;

        for ($p = 0; $p < $slugTotal; $p++) {
            $start = $base;
            $limit = $base + $dateCount;

            while ($base < $limit && $counts[$base] === 0) {
                $base++;
            }

            if ($base === $limit) {
                continue;
            }

            $buf = $sep . $escapedPaths[$p] . "\n" . $datePrefixes[$base - $start] . $counts[$base];
            $sep = ",\n    ";

            for ($base++; $base < $limit; $base++) {
                $count = $counts[$base];
                if ($count !== 0) {
                    $buf .= ",\n" . $datePrefixes[$base - $start] . $count;
                }
            }

            $buf .= "\n    }";
            fwrite($out, $buf);
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
