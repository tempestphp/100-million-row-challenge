<?php

namespace App;

use App\Commands\Visit;

use function array_fill;
use function chr;
use function fclose;
use function feof;
use function fopen;
use function fread;
use function fseek;
use function fwrite;
use function gc_disable;
use function pcntl_fork;
use function str_repeat;
use function str_replace;
use function stream_select;
use function stream_set_chunk_size;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function stream_socket_pair;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function unpack;
use const SEEK_CUR;
use const SEEK_END;
use const STREAM_IPPROTO_IP;
use const STREAM_PF_UNIX;
use const STREAM_SOCK_STREAM;

final class Parser
{
    public static function parse($inputPath, $outputPath)
    {
        self::parseSingleThread($inputPath, $outputPath);
    }

    public static function parseSingleThread($inputPath, $outputPath)
    {
        gc_disable();

        $dateIds = [];
        $dates = [];
        $di = 0;
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
                    $dateIds[$key] = $di;
                    $dates[$di] = $key;
                    $di++;
                }
            }
        }

        $slugBaseMap = [];
        $escapedPaths = [];
        $slugTotal = \count(Visit::SLUGS);
        for ($s = $slugTotal; $s-- > 0;) {
            $slug = Visit::SLUGS[$s];
            $base = $s * $di;
            $slugBaseMap[$slug] = $base;
            $escapedPaths[$base] = '"\/blog\/' . str_replace('/', '\/', $slug) . '": {';
        }

        $paths = [];
        $seenPaths = [];
        $pathTotal = 0;

        $outputSize = $slugTotal * $di;

        $counts = self::parseSingleThreadRange(
            $inputPath,
            $slugBaseMap,
            $dateIds,
            $outputSize,
            $paths,
            $seenPaths,
            $pathTotal,
        );

        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);
        fwrite($out, '{');

        $datePrefixes = [];
        for ($d = $di; $d-- > 0;) {
            $datePrefixes[$d] = '        "20' . $dates[$d] . '": ';
        }

        $firstPath = true;

        for ($p = 0; $p < $pathTotal; $p++) {
            $base = $paths[$p];
            $firstDate = -1;
            for ($d = 0; $d < $di; $d++) {
                if ($counts[$base + $d] !== 0) {
                    $firstDate = $d;
                    break;
                }
            }

            if ($firstDate === -1) {
                continue;
            }

            $buf = $firstPath ? "\n    " : ",\n    ";
            $firstPath = false;
            $buf .= $escapedPaths[$base] . "\n" . $datePrefixes[$firstDate] . $counts[$base + $firstDate];

            for ($d = $firstDate + 1; $d < $di; $d++) {
                $count = $counts[$base + $d];
                if ($count === 0) {
                    continue;
                }
                $buf .= ",\n" . $datePrefixes[$d] . $count;
            }

            $buf .= "\n    }";
            fwrite($out, $buf);
        }

        fwrite($out, "\n}");
        fclose($out);
    }

    private static function parseRange(
        $inputPath, $start, $end,
        $slugBaseMap, $dateIds, $next, $outputSize,
    ) {
        $output = str_repeat("\0", $outputSize);
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $toRead = $remaining > 163_840 ? 163_840 : $remaining;
            $chunk = fread($handle, $toRead);
            $chunkLen = strlen($chunk);
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) {
                break;
            }

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $p = 25;
            $fence = $lastNl - 5000;

            while ($p < $fence) {
                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
            }

            while ($p < $lastNl) {
                $sep = strpos($chunk, ',', $p);
                if ($sep === false || $sep >= $lastNl) {
                    break;
                }
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
            }
        }

        fclose($handle);

        return $output;
    }

    private static function parseSingleThreadRange(
        $inputPath,
        $slugBaseMap, $dateIds, $outputSize,
        &$paths, &$seenPaths, &$pathTotal,
    ) {
        $counts = array_fill(0, $outputSize, 0);
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, 0, SEEK_END);
        $remaining = ftell($handle);
        fseek($handle, 0);

        while ($remaining > 0) {
            $toRead = $remaining > 1_048_576 ? 1_048_576 : $remaining;
            $chunk = fread($handle, $toRead);
            $chunkLen = strlen($chunk);
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) {
                break;
            }

            $tail = $chunkLen - $lastNl - 1;
            fseek($handle, -$tail, SEEK_CUR);
            $remaining += $tail;

            $p = 25;
            $fence = $lastNl - 1010;

            while ($p < $fence) {
                $sep = strpos($chunk, ',', $p);
                $slug = substr($chunk, $p, $sep - $p);
                $base = $slugBaseMap[$slug];
                if (!isset($seenPaths[$base])) {
                    $seenPaths[$base] = 1;
                    $paths[$pathTotal] = $base;
                    $pathTotal++;
                }
                $counts[$base + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $slug = substr($chunk, $p, $sep - $p);
                $base = $slugBaseMap[$slug];
                if (!isset($seenPaths[$base])) {
                    $seenPaths[$base] = 1;
                    $paths[$pathTotal] = $base;
                    $pathTotal++;
                }
                $counts[$base + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $slug = substr($chunk, $p, $sep - $p);
                $base = $slugBaseMap[$slug];
                if (!isset($seenPaths[$base])) {
                    $seenPaths[$base] = 1;
                    $paths[$pathTotal] = $base;
                    $pathTotal++;
                }
                $counts[$base + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $slug = substr($chunk, $p, $sep - $p);
                $base = $slugBaseMap[$slug];
                if (!isset($seenPaths[$base])) {
                    $seenPaths[$base] = 1;
                    $paths[$pathTotal] = $base;
                    $pathTotal++;
                }
                $counts[$base + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $slug = substr($chunk, $p, $sep - $p);
                $base = $slugBaseMap[$slug];
                if (!isset($seenPaths[$base])) {
                    $seenPaths[$base] = 1;
                    $paths[$pathTotal] = $base;
                    $pathTotal++;
                }
                $counts[$base + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $slug = substr($chunk, $p, $sep - $p);
                $base = $slugBaseMap[$slug];
                if (!isset($seenPaths[$base])) {
                    $seenPaths[$base] = 1;
                    $paths[$pathTotal] = $base;
                    $pathTotal++;
                }
                $counts[$base + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $slug = substr($chunk, $p, $sep - $p);
                $base = $slugBaseMap[$slug];
                if (!isset($seenPaths[$base])) {
                    $seenPaths[$base] = 1;
                    $paths[$pathTotal] = $base;
                    $pathTotal++;
                }
                $counts[$base + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $slug = substr($chunk, $p, $sep - $p);
                $base = $slugBaseMap[$slug];
                if (!isset($seenPaths[$base])) {
                    $seenPaths[$base] = 1;
                    $paths[$pathTotal] = $base;
                    $pathTotal++;
                }
                $counts[$base + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $slug = substr($chunk, $p, $sep - $p);
                $base = $slugBaseMap[$slug];
                if (!isset($seenPaths[$base])) {
                    $seenPaths[$base] = 1;
                    $paths[$pathTotal] = $base;
                    $pathTotal++;
                }
                $counts[$base + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $slug = substr($chunk, $p, $sep - $p);
                $base = $slugBaseMap[$slug];
                if (!isset($seenPaths[$base])) {
                    $seenPaths[$base] = 1;
                    $paths[$pathTotal] = $base;
                    $pathTotal++;
                }
                $counts[$base + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;
            }

            while ($p < $lastNl) {
                $sep = strpos($chunk, ',', $p);
                $slug = substr($chunk, $p, $sep - $p);
                $base = $slugBaseMap[$slug];
                if (!isset($seenPaths[$base])) {
                    $seenPaths[$base] = 1;
                    $paths[$pathTotal] = $base;
                    $pathTotal++;
                }
                $counts[$base + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;
            }
        }

        fclose($handle);

        return $counts;
    }

    private static function writeJson(
        $outputPath, $counts, $paths, $dates, $dateCount, $slugCount,
    ) {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);
        fwrite($out, '{');

        $datePrefixes = [];
        $d = $dateCount;
        while ($d-- > 0) {
            $datePrefixes[$d] = '        "' . $dates[$d] . '": ';
        }

        $escapedPaths = [];
        $p = $slugCount;
        while ($p-- > 0) {
            $escapedPaths[$p] = '"\/blog\/' . str_replace('/', '\/', $paths[$p]) . '": {';
        }

        $firstPath = true;
        $base =0;

        for ($p = 0; $p < $slugCount; $p++) {
            $firstDate = -1;
            $idx = $base;
            for ($d = 0; $d < $dateCount; $d++) {
                if (($counts[$idx] ?? 0) !== 0) {
                    $firstDate = $d;
                    break;
                }
                $idx++;
            }

            if ($firstDate === -1) {
                $base += $dateCount;
                continue;
            }

            $buf = $firstPath ? "\n    " : ",\n    ";
            $firstPath = false;
            $buf .= $escapedPaths[$p] . "\n" . $datePrefixes[$firstDate] . ($counts[$idx] ?? 0);

            for ($d = $firstDate + 1; $d < $dateCount; $d++) {
                $idx++;
                $count = $counts[$idx] ?? 0;
                if ($count === 0) {
                    continue;
                }
                $buf .= ",\n" . $datePrefixes[$d] . $count;
            }

            $buf .= "\n    }";
            fwrite($out, $buf);
            $base += $dateCount;
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
