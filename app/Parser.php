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
use function gc_disable;
use function pcntl_fork;
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

        $workers = 8;

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
        $raw = fread($handle, 142000);

        $paths = [];
        $slugBaseMap = [];
        $slugTotal = 0;
        $pos = 0;

        while ($slugTotal < 268) {
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

        $prefix = 'https://stitcher.io/blog/';
        $slugBaseMap = [];
        for ($p = 0; $p < $slugTotal; $p++) {
            $stride = strlen($paths[$p]) + 52;
            $slugBaseMap[substr($prefix . $paths[$p], -22)] = ($stride << 20) | ($p * $dateCount);
        }

        $outputSize = $slugTotal * $dateCount;

        fseek($handle, 0, SEEK_END);
        $fileSize = ftell($handle);

        $chunks = [];
        $lo = 0;
        stream_set_read_buffer($handle, 0);
        while ($lo < $fileSize) {
            $hi = $lo + 33554432;
            if ($hi > $fileSize) {
                $hi = $fileSize;
            }

            $from = 0;
            if ($lo > 0) {
                fseek($handle, $lo);
                fgets($handle);
                $from = ftell($handle);
            }

            $to = $fileSize;
            if ($hi < $fileSize) {
                fseek($handle, $hi);
                fgets($handle);
                $to = ftell($handle);
            }

            $chunks[] = [$from, $to];
            $lo = $hi;
        }

        $chunkCount = count($chunks);

        $sockets = [];

        for ($w = 0; $w < $workers - 1; $w++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_chunk_size($pair[0], $outputSize * 2);
            stream_set_chunk_size($pair[1], $outputSize * 2);
            if (pcntl_fork() === 0) {
                $output = str_repeat("\0", $outputSize);
                $reader = fopen($inputPath, 'rb');
                stream_set_read_buffer($reader, 0);

                for ($chunkIndex = $w; $chunkIndex < $chunkCount; $chunkIndex += $workers) {
                    [$from, $to] = $chunks[$chunkIndex];
                    fseek($reader, $from);
                    $left = $to - $from;

                    while ($left > 0) {
                        $chunk = fread($reader, $left > 131_072 ? 131_072 : $left);
                        $chunkLen = strlen($chunk);
                        $left -= $chunkLen;

                        $lastNl = strrpos($chunk, "\n");
                        if ($lastNl === false) break;

                        $tail = $chunkLen - $lastNl - 1;
                        if ($tail > 0) {
                            fseek($reader, -$tail, SEEK_CUR);
                            $left += $tail;
                        }

                        $p = $lastNl;

                        while ($p > 1248) {
                            $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                            $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                            $p -= $packed >> 20;
                            $output[$idx] = $next[$output[$idx]];

                            $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                            $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                            $p -= $packed >> 20;
                            $output[$idx] = $next[$output[$idx]];

                            $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                            $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                            $p -= $packed >> 20;
                            $output[$idx] = $next[$output[$idx]];

                            $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                            $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                            $p -= $packed >> 20;
                            $output[$idx] = $next[$output[$idx]];

                            $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                            $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                            $p -= $packed >> 20;
                            $output[$idx] = $next[$output[$idx]];

                            $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                            $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                            $p -= $packed >> 20;
                            $output[$idx] = $next[$output[$idx]];

                            $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                            $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                            $p -= $packed >> 20;
                            $output[$idx] = $next[$output[$idx]];

                            $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                            $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                            $p -= $packed >> 20;
                            $output[$idx] = $next[$output[$idx]];

                            $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                            $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                            $p -= $packed >> 20;
                            $output[$idx] = $next[$output[$idx]];

                            $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                            $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                            $p -= $packed >> 20;
                            $output[$idx] = $next[$output[$idx]];

                            $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                            $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                            $p -= $packed >> 20;
                            $output[$idx] = $next[$output[$idx]];

                            $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                            $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                            $p -= $packed >> 20;
                            $output[$idx] = $next[$output[$idx]];
                        }

                        while ($p >= 48) {
                            $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                            $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                            $p -= $packed >> 20;
                            $output[$idx] = $next[$output[$idx]];
                        }
                    }
                }

                fclose($reader);
                fwrite($pair[1], chunk_split($output, 1, "\0"));
                exit(0);
            }
            fclose($pair[1]);
            $sockets[$w] = $pair[0];
        }

        $mainOutput = str_repeat("\0", $outputSize);
        stream_set_read_buffer($handle, 0);

        for ($chunkIndex = $workers - 1; $chunkIndex < $chunkCount; $chunkIndex += $workers) {
            [$from, $to] = $chunks[$chunkIndex];
            fseek($handle, $from);
            $left = $to - $from;

            while ($left > 0) {
                $chunk = fread($handle, $left > 131_072 ? 131_072 : $left);
                $chunkLen = strlen($chunk);
                $left -= $chunkLen;

                $lastNl = strrpos($chunk, "\n");
                if ($lastNl === false) break;

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
                    $mainOutput[$idx] = $next[$mainOutput[$idx]];

                    $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                    $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                    $p -= $packed >> 20;
                    $mainOutput[$idx] = $next[$mainOutput[$idx]];

                    $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                    $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                    $p -= $packed >> 20;
                    $mainOutput[$idx] = $next[$mainOutput[$idx]];

                    $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                    $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                    $p -= $packed >> 20;
                    $mainOutput[$idx] = $next[$mainOutput[$idx]];

                    $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                    $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                    $p -= $packed >> 20;
                    $mainOutput[$idx] = $next[$mainOutput[$idx]];

                    $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                    $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                    $p -= $packed >> 20;
                    $mainOutput[$idx] = $next[$mainOutput[$idx]];

                    $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                    $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                    $p -= $packed >> 20;
                    $mainOutput[$idx] = $next[$mainOutput[$idx]];

                    $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                    $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                    $p -= $packed >> 20;
                    $mainOutput[$idx] = $next[$mainOutput[$idx]];

                    $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                    $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                    $p -= $packed >> 20;
                    $mainOutput[$idx] = $next[$mainOutput[$idx]];

                    $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                    $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                    $p -= $packed >> 20;
                    $mainOutput[$idx] = $next[$mainOutput[$idx]];

                    $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                    $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                    $p -= $packed >> 20;
                    $mainOutput[$idx] = $next[$mainOutput[$idx]];

                    $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                    $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                    $p -= $packed >> 20;
                    $mainOutput[$idx] = $next[$mainOutput[$idx]];
                }

                while ($p >= 48) {
                    $packed = $slugBaseMap[substr($chunk, $p - 48, 22)];
                    $idx = ($packed & 1048575) + $dateIds[substr($chunk, $p - 22, 7)];
                    $p -= $packed >> 20;
                    $mainOutput[$idx] = $next[$mainOutput[$idx]];
                }
            }
        }

        fclose($handle);
        $mainMerged = chunk_split($mainOutput, 1, "\0");

        $buffers = array_fill(0, $workers - 1, '');

        $write = [];
        $except = [];
        while ($sockets !== []) {
            $read = $sockets;
            stream_select($read, $write, $except, null);
            foreach ($read as $key => $socket) {
                $data = fread($socket, $outputSize * 1.5);
                if ($data !== '' && $data !== false) {
                    $buffers[$key] .= $data;
                }
                if (feof($socket)) {
                    fclose($socket);
                    unset($sockets[$key]);
                }
            }
        }

        $merged = $mainMerged;
        for ($w = 0; $w < $workers - 1; $w++) {
            sodium_add($merged, $buffers[$w]);
        }
        $counts = unpack('v*', $merged);

        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 2097152);
        fwrite($out, '{');


        $escapedPaths = [];
        for ($p = 0; $p < $slugTotal; $p++) {
            $escapedPaths[$p] = '"\/blog\/' . $paths[$p] . '": {';
        }

        $sep = "\n    ";
        $base = 1;

        for ($p = 0; $p < $slugTotal; $p++) {
            $start = $base;
            $limit = $base + $dateCount;

            while ($base < $limit && $counts[$base] === 0) {
                $base++;
            }

            if ($base === $limit) continue;

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
