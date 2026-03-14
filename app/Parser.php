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
        $dates = [];
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
                    $dates[$dateCount] = '202' . $key;
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
        $raw = fread($handle, 181000);

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

        $prefix = 'https://stitcher.io/blog/';
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
            if ($stride > $maxStride) {
                $maxStride = $stride;
            }
            $slugBaseMap[substr($prefix . $paths[$p], -$tailLength)] = ($stride << $shift) | ($p * $dateCount);
        }
        $tailOffset = 26 + $tailLength;
        $dateOffset = 22;
        $dateLength = 7;
        $fence = ($maxStride * 10) + $tailOffset;

        $outputSize = $slugTotal * $dateCount;

        fseek($handle, 0, SEEK_END);
        $fileSize = ftell($handle);
        fclose($handle);

        $grain = 1 << 23;
        $chunks = [];
        $lo = 0;
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        while ($lo < $fileSize) {
            $hi = $lo + $grain;
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
        fclose($handle);

        $chunkCount = count($chunks);

        $sockets = [];

        for ($w = 0; $w < $workers; $w++) {
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
                        $chunk = fread($reader, $left > 163_840 ? 163_840 : $left);
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

                        while ($p > $fence) {
                            $packed = $slugBaseMap[substr($chunk, $p - $tailOffset, $tailLength)];
                            $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - $dateOffset, $dateLength)];
                            $output[$idx] = $next[$output[$idx]];
                            $p -= $packed >> $shift;

                            $packed = $slugBaseMap[substr($chunk, $p - $tailOffset, $tailLength)];
                            $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - $dateOffset, $dateLength)];
                            $output[$idx] = $next[$output[$idx]];
                            $p -= $packed >> $shift;

                            $packed = $slugBaseMap[substr($chunk, $p - $tailOffset, $tailLength)];
                            $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - $dateOffset, $dateLength)];
                            $output[$idx] = $next[$output[$idx]];
                            $p -= $packed >> $shift;

                            $packed = $slugBaseMap[substr($chunk, $p - $tailOffset, $tailLength)];
                            $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - $dateOffset, $dateLength)];
                            $output[$idx] = $next[$output[$idx]];
                            $p -= $packed >> $shift;

                            $packed = $slugBaseMap[substr($chunk, $p - $tailOffset, $tailLength)];
                            $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - $dateOffset, $dateLength)];
                            $output[$idx] = $next[$output[$idx]];
                            $p -= $packed >> $shift;

                            $packed = $slugBaseMap[substr($chunk, $p - $tailOffset, $tailLength)];
                            $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - $dateOffset, $dateLength)];
                            $output[$idx] = $next[$output[$idx]];
                            $p -= $packed >> $shift;

                            $packed = $slugBaseMap[substr($chunk, $p - $tailOffset, $tailLength)];
                            $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - $dateOffset, $dateLength)];
                            $output[$idx] = $next[$output[$idx]];
                            $p -= $packed >> $shift;

                            $packed = $slugBaseMap[substr($chunk, $p - $tailOffset, $tailLength)];
                            $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - $dateOffset, $dateLength)];
                            $output[$idx] = $next[$output[$idx]];
                            $p -= $packed >> $shift;

                            $packed = $slugBaseMap[substr($chunk, $p - $tailOffset, $tailLength)];
                            $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - $dateOffset, $dateLength)];
                            $output[$idx] = $next[$output[$idx]];
                            $p -= $packed >> $shift;

                            $packed = $slugBaseMap[substr($chunk, $p - $tailOffset, $tailLength)];
                            $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - $dateOffset, $dateLength)];
                            $output[$idx] = $next[$output[$idx]];
                            $p -= $packed >> $shift;
                        }

                        while ($p >= $tailOffset) {
                            $packed = $slugBaseMap[substr($chunk, $p - $tailOffset, $tailLength)];
                            $idx = ($packed & $mask) + $dateIds[substr($chunk, $p - $dateOffset, $dateLength)];
                            $output[$idx] = $next[$output[$idx]];
                            $p -= $packed >> $shift;
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

        $buffers = array_fill(0, $workers, '');

        $write = [];
        $except = [];
        while ($sockets !== []) {
            $read = $sockets;
            stream_select($read, $write, $except, 5);
            foreach ($read as $key => $socket) {
                $data = fread($socket, $outputSize * 2);
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
        stream_set_write_buffer($out, 1_048_576);
        fwrite($out, '{');

        $datePrefixes = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "' . $dates[$d] . '": ';
        }

        $escapedPaths = [];
        for ($p = 0; $p < $slugTotal; $p++) {
            $escapedPaths[$p] = '"\/blog\/' . str_replace('/', '\/', $paths[$p]) . '": {';
        }

        $sep = "\n    ";
        $base = 1;

        for ($p = 0; $p < $slugTotal; $p++) {
            $firstDate = -1;
            $idx = $base;
            for ($d = 0; $d < $dateCount; $d++) {
                if ($counts[$idx] !== 0) {
                    $firstDate = $d;
                    break;
                }
                $idx++;
            }

            if ($firstDate === -1) {
                $base += $dateCount;
                continue;
            }

            $buf = $sep . $escapedPaths[$p] . "\n" . $datePrefixes[$firstDate] . $counts[$idx];
            $sep = ",\n    ";

            for ($d = $firstDate + 1; $d < $dateCount; $d++) {
                $idx++;
                $count = $counts[$idx];
                if ($count === 0) continue;
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
