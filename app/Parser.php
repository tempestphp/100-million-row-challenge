<?php

namespace App;

use function chr;
use function count;
use function fclose;
use function fgets;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function implode;
use function pcntl_fork;
use function str_replace;
use function str_repeat;
use function stream_select;
use function stream_set_chunk_size;
use function stream_get_contents;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function stream_socket_pair;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function unpack;

use const SEEK_CUR;
use const STREAM_IPPROTO_IP;
use const STREAM_PF_UNIX;
use const STREAM_SOCK_STREAM;

final class Parser
{
    private const int K0 = 163_840;
    private const int K1   = 2_097_152;
    private const int K2  = 25;
    private const int K3     = 10;

    public function parse($inputPath, $outputPath)
    {
        gc_disable();

        $inputBytes   = 7_509_674_827;
        $workerTotal = self::K3;

        $dayIdByKey   = [];
        $dayKeyById     = [];
        $dateCount = 0;

        for ($y = 21; $y <= 26; $y++) {
            $yStr = (string)$y;
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2           => $y === 24 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default     => 31,
                };
                $mStr = ($m < 10 ? '0' : '') . $m;
                $ym   = $yStr . '-' . $mStr . '-';
                for ($d = 1; $d <= $maxD; $d++) {
                    $dd                    = ($d < 10 ? '0' : '') . $d;
                    $dayIdByKey[$ym . $dd] = $dateCount;
                    $dayKeyById[$dateCount] = $ym . $dd;
                    $dateCount++;
                }
            }
        }

        $next = [];
        for ($i = 0; $i < 255; $i++) {
            $next[chr($i)] = chr($i + 1);
        }

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $raw = fread($handle, self::K1);
        fclose($handle);

        $slugIdByKey   = [];
        $slugKeyById     = [];
        $slugTotal = 0;
        $pos       = 0;
        $lastNl    = strrpos($raw, "\n") ?: 0;

        while ($pos < $lastNl) {
            $nl = strpos($raw, "\n", $pos + 52);
            if ($nl === false) break;

            $slug = substr($raw, $pos + self::K2, $nl - $pos - 51);

            if (!isset($slugIdByKey[$slug])) {
                $slugIdByKey[$slug]    = $slugTotal;
                $slugKeyById[$slugTotal] = $slug;
                $slugTotal++;
            }

            $pos = $nl + 1;
        }
        unset($raw);

        $slugBaseMap = [];
        foreach ($slugIdByKey as $slug => $id) {
            $slugBaseMap[$slug] = $id * $dateCount;
        }

        $outputSize = $slugTotal * $dateCount;

        $boundaries = [0];
        $bh = fopen($inputPath, 'rb');
        for ($i = 1; $i < $workerTotal; $i++) {
            fseek($bh, (int)($inputBytes * $i / $workerTotal));
            fgets($bh);
            $boundaries[] = ftell($bh);
        }
        fclose($bh);
        $boundaries[] = $inputBytes;

        $sockets = [];

        for ($w = 0; $w < $workerTotal - 1; $w++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_chunk_size($pair[0], $outputSize);
            stream_set_chunk_size($pair[1], $outputSize);
            $pid = pcntl_fork();

            if ($pid === 0) {
                fclose($pair[0]);
                $output = str_repeat(chr(0), $outputSize);
                $fh     = fopen($inputPath, 'rb');
                stream_set_read_buffer($fh, 0);

                self::q2($fh, $boundaries[$w], $boundaries[$w + 1], $slugBaseMap, $dayIdByKey, $next, $output);

                fclose($fh);
                fwrite($pair[1], $output);
                fclose($pair[1]);
                exit(0);
            }

            fclose($pair[1]);
            $sockets[(int)$pair[0]] = $pair[0];
        }

        $output = str_repeat(chr(0), $outputSize);
        $fh     = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);
        self::q2($fh, $boundaries[$workerTotal - 1], $boundaries[$workerTotal], $slugBaseMap, $dayIdByKey, $next, $output);

        fclose($fh);

        $counts = unpack('C*', $output);
        unset($output);

        while ($sockets !== []) {
            $read = $sockets;
            $write = [];
            $except = [];
            stream_select($read, $write, $except, 5);
            foreach ($read as $socket) {
                $data = stream_get_contents($socket);
                fclose($socket);
                unset($sockets[(int)$socket]);
                foreach (unpack('C*', $data) as $k => $v) {
                    $counts[$k] += $v;
                }
            }
        }

        self::q4($outputPath, $counts, $slugKeyById, $dayKeyById, $dateCount);
    }

    private static function q2($handle, $start, $end, $slugBaseMap, $dayIdByKey, $next, &$output)
    {
        fseek($handle, $start);

        $remaining = $end - $start;
        $bufSize   = self::K0;
        $prefixLen = self::K2;

        while ($remaining > 0) {
            $toRead = $remaining > $bufSize ? $bufSize : $remaining;
            $chunk  = fread($handle, $toRead);
            if (!$chunk) break;

            $chunkLen   = strlen($chunk);
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) continue;

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $p     = $prefixLen;
            $fence = $lastNl - 792;

            while ($p < $fence) {
                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dayIdByKey[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dayIdByKey[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dayIdByKey[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dayIdByKey[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dayIdByKey[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dayIdByKey[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dayIdByKey[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dayIdByKey[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

            }

            while ($p < $lastNl) {
                $sep = strpos($chunk, ',', $p);
                if ($sep === false || $sep >= $lastNl) break;
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dayIdByKey[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
            }
        }
    }

    private static function q4($outputPath, $counts, $slugKeyById, $dayKeyById, $dateCount)
    {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);

        $slugTotal    = count($slugKeyById);
        $datePrefixes = [];
        $escapedPaths = [];

        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "20' . $dayKeyById[$d] . '": ';
        }

        for ($p = 0; $p < $slugTotal; $p++) {
            $escapedPaths[$p] = "\"\\/blog\\/" . str_replace('/', '\\/', $slugKeyById[$p]) . '"';
        }

        fwrite($out, '{');
        $firstPath = true;
        $base      = 1;

        for ($p = 0; $p < $slugTotal; $p++) {
            $dateEntries = [];

            for ($d = 0; $d < $dateCount; $d++) {
                $count = $counts[$base + $d];
                if ($count !== 0) {
                    $dateEntries[] = $datePrefixes[$d] . $count;
                }
            }

            if (empty($dateEntries)) {
                $base += $dateCount;
                continue;
            }

            $sep2      = $firstPath ? '' : ',';
            $firstPath = false;

            fwrite($out,
                $sep2 .
                "\n    " . $escapedPaths[$p] . ": {\n" .
                implode(",\n", $dateEntries) .
                "\n    }"
            );

            $base += $dateCount;
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
