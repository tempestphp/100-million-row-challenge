<?php

namespace App;

use function array_fill;
use function chr;
use function fclose;
use function fgets;
use function file_exists;
use function file_get_contents;
use function file_put_contents;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function getmypid;
use function intdiv;
use function is_dir;
use function pack;
use function pcntl_fork;
use function pcntl_wait;
use function posix_getpid;
use function posix_kill;
use function str_repeat;
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
use const SEEK_END;
use const SIGKILL;
use const WNOHANG;

final class Parser
{
    private const int WORKERS = 8;

    public static function parse($inputPath, $outputPath): void
    {
        gc_disable();
        $dateIds = [];
        $dates   = [];
        $di      = 0;
        for ($y = 21; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2       => ($y === 24) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $mStr  = ($m < 10 ? '0' : '') . $m;
                $ymStr = ($y % 10) . '-' . $mStr . '-';
                for ($d = 1; $d <= $maxD; $d++) {
                    $dStr           = ($d < 10 ? '0' : '') . $d;
                    $dateIds[$ymStr . $dStr] = $di;
                    $dates[$di]             = "20{$y}-{$mStr}-{$dStr}";
                    $di++;
                }
            }
        }
        $next = [];
        $i    = 255;
        while ($i-- > 0) {
            $next[chr($i)] = chr($i + 1);
        }

        $bh  = fopen($inputPath, 'rb');
        $raw = fread($bh, 2_097_152);

        $slugBaseMap = [];
        $paths       = [];
        $slugTotal   = 0;
        $pos         = 0;
        $lastNl      = strrpos($raw, "\n") ?: 0;
        $noNew       = 0;

        while ($pos < $lastNl) {
            $nl = strpos($raw, "\n", $pos + 52);
            if ($nl === false) break;
            $slug = substr($raw, $pos + 25, $nl - $pos - 51);
            if (!isset($slugBaseMap[$slug])) {
                $paths[$slugTotal]    = $slug;
                $slugBaseMap[$slug]   = $slugTotal * $di;
                $slugTotal++;
                $noNew = 0;
            } elseif (++$noNew > 5000) {
                break;
            }
            $pos = $nl + 1;
        }
        unset($raw);

        $outputSize = $slugTotal * $di;

        fseek($bh, 0, SEEK_END);
        $fileSize   = ftell($bh);
        $step       = intdiv($fileSize, self::WORKERS);
        $boundaries = [0];
        for ($i = 1; $i < self::WORKERS; $i++) {
            fseek($bh, $step * $i);
            fgets($bh);
            $boundaries[] = ftell($bh);
        }
        fclose($bh);
        $boundaries[] = $fileSize;

        $myPid     = getmypid();
        $tmpDir    = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
        $tmpPrefix = $tmpDir . '/p100m_' . $myPid;
        $childMap  = [];

        for ($w = 0; $w < self::WORKERS - 1; $w++) {
            $outFile = $tmpPrefix . '_' . $w;
            $pid     = pcntl_fork();
            if ($pid === 0) {
                $fh = fopen($inputPath, 'rb');
                stream_set_read_buffer($fh, 0);
                $output = self::parseRange(
                    $inputPath, $boundaries[$w], $boundaries[$w + 1],
                    $slugBaseMap, $dateIds, $next, $outputSize,
                );
                fclose($fh);
                file_put_contents($outFile, $output);
                posix_kill(posix_getpid(), SIGKILL);
            }
            $childMap[$pid] = $outFile;
        }

        $output  = self::parseRange(
            $inputPath, $boundaries[self::WORKERS - 1], $boundaries[self::WORKERS],
            $slugBaseMap, $dateIds, $next, $outputSize,
        );

        $counts = array_fill(0, $outputSize, 0);
        $j      = 0;
        foreach (unpack('C*', $output) as $v) {
            $counts[$j++] = $v;
        }

        $pending = self::WORKERS - 1;
        while ($pending > 0) {
            $pid = pcntl_wait($status, WNOHANG);
            if ($pid <= 0) $pid = pcntl_wait($status);
            $outFile = $childMap[$pid];
            if (file_exists($outFile)) {
                $j = 0;
                foreach (unpack('C*', file_get_contents($outFile)) as $v) {
                    $counts[$j++] += $v;
                }
                unlink($outFile);
            }
            unset($childMap[$pid]);
            $pending--;
        }

        self::writeJson($outputPath, $counts, $paths, $dates, $di, $slugTotal);
    }

    private static function parseRange(
        string $inputPath,
        int    $start,
        int    $end,
        array  $slugBaseMap,
        array  $dateIds,
        array  $next,
        int    $outputSize,
    ): string {
        $output = str_repeat("\0", $outputSize);
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $toRead   = $remaining > 163_840 ? 163_840 : $remaining;
            $chunk    = fread($handle, $toRead);
            $chunkLen = strlen($chunk);
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) break;

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $p     = 25;
            $fence = $lastNl - 1010;

            while ($p < $fence) {
                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
            }

            while ($p < $lastNl) {
                $sep = strpos($chunk, ',', $p);
                if ($sep === false || $sep >= $lastNl) break;
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
            }
        }

        fclose($handle);
        return $output;
    }

    private static function writeJson(
        string $outputPath,
        array  $counts,
        array  $paths,
        array  $dates,
        int    $dateCount,
        int    $slugCount,
    ): void {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);
        fwrite($out, '{');

        $firstDatePrefixes = [];
        $datePrefixes      = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $firstDatePrefixes[$d] = "\n        \"" . $dates[$d] . '": ';
            $datePrefixes[$d]      = ",\n        \"" . $dates[$d] . '": ';
        }

        $escapedPaths = [];
        for ($p = 0; $p < $slugCount; $p++) {
            $escapedPaths[$p] = "\"\/blog\/" . $paths[$p] . '": {';
        }

        $sep  = "\n    ";
        $base = 0;

        for ($p = 0; $p < $slugCount; $p++) {
            $firstDate = -1;
            $idx       = $base;
            for ($d = 0; $d < $dateCount; $d++, $idx++) {
                if ($counts[$idx] !== 0) {
                    $firstDate = $d;
                    break;
                }
            }

            if ($firstDate === -1) {
                $base += $dateCount;
                continue;
            }

            $buf = $sep . $escapedPaths[$p] . $firstDatePrefixes[$firstDate] . $counts[$idx];
            $sep = ",\n    ";

            for ($d = $firstDate + 1; $d < $dateCount; $d++) {
                $idx++;
                $count = $counts[$idx];
                if ($count === 0) continue;
                $buf .= $datePrefixes[$d] . $count;
            }

            $buf .= "\n    }";
            fwrite($out, $buf);
            $base += $dateCount;
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}