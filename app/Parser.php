<?php

namespace App;

use function array_fill;
use function chr;
use function chunk_split;
use function count;
use function fclose;
use function feof;
use function fgets;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function sodium_add;
use function str_repeat;
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
        gc_disable();

        $dateIds = [];
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

        $next = [];
        for ($i = 0; $i < 255; $i++) {
            $next[chr($i)] = chr($i + 1);
        }

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $raw = fread($handle, 181_000);

        $prefix = 'https://stitcher.io/blog/';
        $paths = [];
        $slugBaseMap = [];
        $slugTotal = 0;
        $pos = 0;
        $lastNl = strrpos($raw, "\n") ?: 0;

        while ($pos < $lastNl && $slugTotal < 268) {
            $nl = strpos($raw, "\n", $pos + 52);
            if ($nl === false) break;
            $slug = substr($raw, $pos + 25, $nl - $pos - 51);
            if (!isset($slugBaseMap[$slug])) {
                $paths[$slugTotal] = $slug;
                $slugBaseMap[$slug] = $slugTotal * $dateCount;
                $slugTotal++;
            }
            $pos = $nl + 1;
        }
        unset($raw);

        $tailLength = 1;
        while (true) {
            $slugBaseMap = [];
            for ($p = 0; $p < $slugTotal; $p++) {
                $tail = substr($prefix . $paths[$p], -$tailLength);
                if (isset($slugBaseMap[$tail])) {
                    $tailLength++;
                    continue 2;
                }
                $slugBaseMap[$tail] = true;
            }
            break;
        }

        $shift = 20;
        $mask = (1 << $shift) - 1;
        $maxStride = 0;
        $slugBaseMap = [];
        for ($p = 0; $p < $slugTotal; $p++) {
            $stride = strlen($paths[$p]) + 52;
            if ($stride > $maxStride) $maxStride = $stride;
            $slugBaseMap[substr($prefix . $paths[$p], -$tailLength)] = ($stride << $shift) | ($p * $dateCount);
        }
        $tailOffset = 26 + $tailLength;
        $dateOffset = 22;
        $dateLength = 7;
        $fence = ($maxStride * 12) + $tailOffset;

        $outputSize = $slugTotal * $dateCount;

        fseek($handle, 0, SEEK_END);
        $fileSize = ftell($handle);
        fclose($handle);

        $grain = 1 << 25;
        $chunks = [];
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $lo = 0;
        while ($lo < $fileSize) {
            $hi = $lo + $grain;
            if ($hi > $fileSize) $hi = $fileSize;
            $from = 0;
            if ($lo > 0) { fseek($handle, $lo); fgets($handle); $from = ftell($handle); }
            $to = $fileSize;
            if ($hi < $fileSize) { fseek($handle, $hi); fgets($handle); $to = ftell($handle); }
            $chunks[] = [$from, $to];
            $lo = $hi;
        }
        fclose($handle);
        $chunkCount = count($chunks);

        $workers = 8;
        $sockets = [];

        for ($w = 0; $w < $workers; $w++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_chunk_size($pair[0], $outputSize << 1);
            stream_set_chunk_size($pair[1], $outputSize << 1);
            if (pcntl_fork() === 0) {
                fclose($pair[0]);
                $output = str_repeat("\0", $outputSize);
                $reader = fopen($inputPath, 'rb');
                stream_set_read_buffer($reader, 0);

                for ($ci = $w; $ci < $chunkCount; $ci += $workers) {
                    [$from, $to] = $chunks[$ci];
                    fseek($reader, $from);
                    $remaining = $to - $from;

                    while ($remaining > 0) {
                        $chunk = fread($reader, $remaining > 131_072 ? 131_072 : $remaining);
                        $chunkLen = strlen($chunk);
                        $remaining -= $chunkLen;

                        $lastNl = strrpos($chunk, "\n");
                        if ($lastNl === false) break;

                        $tail = $chunkLen - $lastNl - 1;
                        if ($tail > 0) {
                            fseek($reader, -$tail, SEEK_CUR);
                            $remaining += $tail;
                        }

                        $p = $lastNl;

                        while ($p > $fence) {
                            $packed = $slugBaseMap[substr($chunk, $p - $tailOffset, $tailLength)]; $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - $dateOffset, $dateLength)]; $p -= $packed >> $shift; $output[$idx] = $next[$output[$idx]];
                            $packed = $slugBaseMap[substr($chunk, $p - $tailOffset, $tailLength)]; $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - $dateOffset, $dateLength)]; $p -= $packed >> $shift; $output[$idx] = $next[$output[$idx]];
                            $packed = $slugBaseMap[substr($chunk, $p - $tailOffset, $tailLength)]; $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - $dateOffset, $dateLength)]; $p -= $packed >> $shift; $output[$idx] = $next[$output[$idx]];
                            $packed = $slugBaseMap[substr($chunk, $p - $tailOffset, $tailLength)]; $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - $dateOffset, $dateLength)]; $p -= $packed >> $shift; $output[$idx] = $next[$output[$idx]];
                            $packed = $slugBaseMap[substr($chunk, $p - $tailOffset, $tailLength)]; $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - $dateOffset, $dateLength)]; $p -= $packed >> $shift; $output[$idx] = $next[$output[$idx]];
                            $packed = $slugBaseMap[substr($chunk, $p - $tailOffset, $tailLength)]; $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - $dateOffset, $dateLength)]; $p -= $packed >> $shift; $output[$idx] = $next[$output[$idx]];
                            $packed = $slugBaseMap[substr($chunk, $p - $tailOffset, $tailLength)]; $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - $dateOffset, $dateLength)]; $p -= $packed >> $shift; $output[$idx] = $next[$output[$idx]];
                            $packed = $slugBaseMap[substr($chunk, $p - $tailOffset, $tailLength)]; $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - $dateOffset, $dateLength)]; $p -= $packed >> $shift; $output[$idx] = $next[$output[$idx]];
                            $packed = $slugBaseMap[substr($chunk, $p - $tailOffset, $tailLength)]; $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - $dateOffset, $dateLength)]; $p -= $packed >> $shift; $output[$idx] = $next[$output[$idx]];
                            $packed = $slugBaseMap[substr($chunk, $p - $tailOffset, $tailLength)]; $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - $dateOffset, $dateLength)]; $p -= $packed >> $shift; $output[$idx] = $next[$output[$idx]];
                            $packed = $slugBaseMap[substr($chunk, $p - $tailOffset, $tailLength)]; $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - $dateOffset, $dateLength)]; $p -= $packed >> $shift; $output[$idx] = $next[$output[$idx]];
                            $packed = $slugBaseMap[substr($chunk, $p - $tailOffset, $tailLength)]; $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - $dateOffset, $dateLength)]; $p -= $packed >> $shift; $output[$idx] = $next[$output[$idx]];
                        }

                        while ($p >= $tailOffset) {
                            $packed = $slugBaseMap[substr($chunk, $p - $tailOffset, $tailLength)]; $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - $dateOffset, $dateLength)]; $p -= $packed >> $shift; $output[$idx] = $next[$output[$idx]];
                        }
                    }
                }

                fclose($reader);
                fwrite($pair[1], chunk_split($output, 1, "\0"));
                fclose($pair[1]);
                exit(0);
            }
            fclose($pair[1]);
            $sockets[$w] = $pair[0];
        }

        $buffers = array_fill(0, $workers, '');
        while ($sockets !== []) {
            $read = $sockets; $write = []; $except = [];
            stream_select($read, $write, $except, null);
            foreach ($read as $key => $socket) {
                $data = fread($socket, 8_388_608);
                if ($data !== '' && $data !== false) {
                    $buffers[$key] .= $data;
                }
                if (feof($socket)) {
                    fclose($socket);
                    unset($sockets[$key]);
                }
            }
        }

        $merged = $buffers[0];
        for ($w = 1; $w < $workers; $w++) {
            sodium_add($merged, $buffers[$w]);
        }
        $counts = unpack('v*', $merged);

        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 4_194_304);
        fwrite($out, '{');

        $escapedPaths = [];
        for ($p = 0; $p < $slugTotal; $p++) {
            $escapedPaths[$p] = '"\/blog\/' . $paths[$p] . '": {';
        }

        $sep = "\n    ";
        $base = 1;

        for ($p = 0; $p < $slugTotal; $p++) {
            $limit = $base + $dateCount;

            while ($base < $limit && $counts[$base] === 0) {
                $base++;
            }

            if ($base === $limit) continue;

            $dOff = $base - ($limit - $dateCount);
            $buf = $sep . $escapedPaths[$p] . "\n" . $datePrefixes[$dOff] . $counts[$base];
            $sep = ",\n    ";

            for ($base++; $base < $limit; $base++) {
                $count = $counts[$base];
                if ($count === 0) continue;
                $buf .= ",\n" . $datePrefixes[$base - ($limit - $dateCount)] . $count;
            }

            $buf .= "\n    }";
            fwrite($out, $buf);
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
