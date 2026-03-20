<?php

declare(strict_types=1);

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
use function implode;
use function intdiv;
use function pcntl_fork;
use function sodium_add;
use function str_repeat;
use function str_replace;
use function stream_select;
use function stream_set_chunk_size;
use function stream_set_read_buffer;
use function stream_socket_pair;
use function stream_set_write_buffer;
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
    public static function parse(string $inPath, string $outPath)
    {
        gc_disable();
        $threads = 8;
        $keys = [];
        $dates = [];
        $datesCount = 0;
        for ($y = 1; $y <= 6; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $daysInMonth = match($m) {
                    2 => $y === 4 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31
                };

                $month = $m < 10 ? "0{$m}" : (string)$m;
                $date = "{$y}-{$month}-";
                for ($d = 1; $d <= $daysInMonth; $d++) {
                    $key = $date . ($d < 10 ? "0{$d}" : (string)$d);
                    $keys[$key] = $datesCount;
                    $dates[$datesCount] = $key;
                    $datesCount++;
                }
            }
        }

        $chars = [];
        for ($i = 0; $i < 255; $i++) {
            $chars[chr($i)] = chr($i + 1);
        }
        
        $inFile = fopen($inPath, 'rb');
        stream_set_read_buffer($inFile, 0);
        $data = fread($inFile, 142_000);
        $outData = [];
        $uris = [];
        $urisCount = 0;
        $lineNo = 0;
        do {
            $currLine = strpos($data, "\n", $lineNo + 52);
            if ($currLine === false) {
                break;
            }

            $uri = substr($data, $lineNo + 25, $currLine - $lineNo - 51);
            if (!isset($outData[$uri])) {
                $uris[$urisCount] = $uri;
                $outData[$uri] = $urisCount * $datesCount;
                $urisCount++;
            }

            $lineNo = $currLine + 1;
        } while ($urisCount < 268);

        unset($data);
        $allUris = [];
        for ($currUri = 0; $currUri < 268; $currUri++) {
            $subUri = strlen($uris[$currUri]) + 52;
            $allUris[substr('https://stitcher.io/blog/' . $uris[$currUri], -22)] = ($subUri << 20) | ($currUri * $datesCount);
        }

        $allCount = $urisCount * $datesCount;
        $chunkLimit = $allCount * 2;
        fseek($inFile, 0, SEEK_END);
        $remaining = ftell($inFile);
        $part = 0;
        $parts = [];
        $partsLimit = $threads - 1;
        do {
            $curr = intdiv($remaining * $part, $threads);
            $next = intdiv($remaining * ($part + 1), $threads);
            if ($curr > 0) {
                fseek($inFile, $curr);
                fgets($inFile);
                $curr = ftell($inFile);
            }

            if ($part < $partsLimit) {
                fseek($inFile, $next);
                fgets($inFile);
                $next = ftell($inFile);
            } else {
                $next = $remaining;
            }

            $parts[$part] = [$curr, $next];
        } while (++$part < $threads);

        $sockets = [];
        for ($thread = 0; $thread < $partsLimit; $thread++) {
            $ssPair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_chunk_size($ssPair[0], $chunkLimit);
            stream_set_chunk_size($ssPair[1], $chunkLimit);
            if (pcntl_fork() === 0) {
                $all = str_repeat("\0", $allCount);
                $file = fopen($inPath, 'rb');
                stream_set_read_buffer($file, 0);
                [$first, $last] = $parts[$thread];
                fseek($file, $first);
                $remaining = $last - $first;
                do {
                    $chunk = fread($file, $remaining > 1_048_576 ? 1_048_576 : $remaining);
                    $chunkLen = strlen($chunk);
                    $remaining -= $chunkLen;
                    $lastRow = strrpos($chunk, "\n");
                    if ($lastRow === false) {
                        break;
                    }

                    $finish = $chunkLen - $lastRow - 1;
                    if ($finish > 0) {
                        fseek($file, -$finish, SEEK_CUR);
                        $remaining += $finish;
                    }

                    $limit = $lastRow;
                    do {
                        $segment = substr($chunk, $limit - 48, 33);
                        $done = $allUris[substr($segment, 0, 22)];
                        $position = ($done & 1_048_575) + $keys[substr($segment, -7)];
                        $limit -= $done >> 20;
                        $all[$position] = $chars[$all[$position]];
                        $segment = substr($chunk, $limit - 48, 33);
                        $done = $allUris[substr($segment, 0, 22)];
                        $position = ($done & 1_048_575) + $keys[substr($segment, -7)];
                        $limit -= $done >> 20;
                        $all[$position] = $chars[$all[$position]];
                        $segment = substr($chunk, $limit - 48, 33);
                        $done = $allUris[substr($segment, 0, 22)];
                        $position = ($done & 1_048_575) + $keys[substr($segment, -7)];
                        $limit -= $done >> 20;
                        $all[$position] = $chars[$all[$position]];
                        $segment = substr($chunk, $limit - 48, 33);
                        $done = $allUris[substr($segment, 0, 22)];
                        $position = ($done & 1_048_575) + $keys[substr($segment, -7)];
                        $limit -= $done >> 20;
                        $all[$position] = $chars[$all[$position]];
                        $segment = substr($chunk, $limit - 48, 33);
                        $done = $allUris[substr($segment, 0, 22)];
                        $position = ($done & 1_048_575) + $keys[substr($segment, -7)];
                        $limit -= $done >> 20;
                        $all[$position] = $chars[$all[$position]];
                        $segment = substr($chunk, $limit - 48, 33);
                        $done = $allUris[substr($segment, 0, 22)];
                        $position = ($done & 1_048_575) + $keys[substr($segment, -7)];
                        $limit -= $done >> 20;
                        $all[$position] = $chars[$all[$position]];
                        $segment = substr($chunk, $limit - 48, 33);
                        $done = $allUris[substr($segment, 0, 22)];
                        $position = ($done & 1_048_575) + $keys[substr($segment, -7)];
                        $limit -= $done >> 20;
                        $all[$position] = $chars[$all[$position]];
                        $segment = substr($chunk, $limit - 48, 33);
                        $done = $allUris[substr($segment, 0, 22)];
                        $position = ($done & 1_048_575) + $keys[substr($segment, -7)];
                        $limit -= $done >> 20;
                        $all[$position] = $chars[$all[$position]];
                        $segment = substr($chunk, $limit - 48, 33);
                        $done = $allUris[substr($segment, 0, 22)];
                        $position = ($done & 1_048_575) + $keys[substr($segment, -7)];
                        $limit -= $done >> 20;
                        $all[$position] = $chars[$all[$position]];
                        $segment = substr($chunk, $limit - 48, 33);
                        $done = $allUris[substr($segment, 0, 22)];
                        $position = ($done & 1_048_575) + $keys[substr($segment, -7)];
                        $limit -= $done >> 20;
                        $all[$position] = $chars[$all[$position]];
                        $segment = substr($chunk, $limit - 48, 33);
                        $done = $allUris[substr($segment, 0, 22)];
                        $position = ($done & 1_048_575) + $keys[substr($segment, -7)];
                        $limit -= $done >> 20;
                        $all[$position] = $chars[$all[$position]];
                        $segment = substr($chunk, $limit - 48, 33);
                        $done = $allUris[substr($segment, 0, 22)];
                        $position = ($done & 1_048_575) + $keys[substr($segment, -7)];
                        $limit -= $done >> 20;
                        $all[$position] = $chars[$all[$position]];
                    } while ($limit > 1248);

                    while ($limit >= 48) {
                        $segment = substr($chunk, $limit - 48, 33);
                        $done = $allUris[substr($segment, 0, 22)];
                        $position = ($done & 1_048_575) + $keys[substr($segment, -7)];
                        $limit -= $done >> 20;
                        $all[$position] = $chars[$all[$position]];
                    }
                } while ($remaining > 0);

                fclose($file);
                fwrite($ssPair[1], chunk_split($all, 1, "\0"));
                exit(0);
            }

            fclose($ssPair[1]);
            $sockets[$thread] = $ssPair[0];
        }

        $allInAll = str_repeat("\0", $allCount);
        stream_set_read_buffer($inFile, 0);
        [$first, $last] = $parts[$partsLimit];
        fseek($inFile, $first);
        $remaining = $last - $first;
        do {
            $chunk = fread($inFile, $remaining > 1_048_576 ? 1_048_576 : $remaining);
            $chunkLen = strlen($chunk);
            $remaining -= $chunkLen;
            $lastRow = strrpos($chunk, "\n");
            if ($lastRow === false) {
                break;
            }

            $finish = $chunkLen - $lastRow - 1;
            if ($finish > 0) {
                fseek($inFile, -$finish, SEEK_CUR);
                $remaining += $finish;
            }

            $limit = $lastRow;
            do {
                $segment = substr($chunk, $limit - 48, 33);
                $done = $allUris[substr($segment, 0, 22)];
                $position = ($done & 1_048_575) + $keys[substr($segment, -7)];
                $limit -= $done >> 20;
                $allInAll[$position] = $chars[$allInAll[$position]];
                $segment = substr($chunk, $limit - 48, 33);
                $done = $allUris[substr($segment, 0, 22)];
                $position = ($done & 1_048_575) + $keys[substr($segment, -7)];
                $limit -= $done >> 20;
                $allInAll[$position] = $chars[$allInAll[$position]];
                $segment = substr($chunk, $limit - 48, 33);
                $done = $allUris[substr($segment, 0, 22)];
                $position = ($done & 1_048_575) + $keys[substr($segment, -7)];
                $limit -= $done >> 20;
                $allInAll[$position] = $chars[$allInAll[$position]];
                $segment = substr($chunk, $limit - 48, 33);
                $done = $allUris[substr($segment, 0, 22)];
                $position = ($done & 1_048_575) + $keys[substr($segment, -7)];
                $limit -= $done >> 20;
                $allInAll[$position] = $chars[$allInAll[$position]];
                $segment = substr($chunk, $limit - 48, 33);
                $done = $allUris[substr($segment, 0, 22)];
                $position = ($done & 1_048_575) + $keys[substr($segment, -7)];
                $limit -= $done >> 20;
                $allInAll[$position] = $chars[$allInAll[$position]];
                $segment = substr($chunk, $limit - 48, 33);
                $done = $allUris[substr($segment, 0, 22)];
                $position = ($done & 1_048_575) + $keys[substr($segment, -7)];
                $limit -= $done >> 20;
                $allInAll[$position] = $chars[$allInAll[$position]];
                $segment = substr($chunk, $limit - 48, 33);
                $done = $allUris[substr($segment, 0, 22)];
                $position = ($done & 1_048_575) + $keys[substr($segment, -7)];
                $limit -= $done >> 20;
                $allInAll[$position] = $chars[$allInAll[$position]];
                $segment = substr($chunk, $limit - 48, 33);
                $done = $allUris[substr($segment, 0, 22)];
                $position = ($done & 1_048_575) + $keys[substr($segment, -7)];
                $limit -= $done >> 20;
                $allInAll[$position] = $chars[$allInAll[$position]];
                $segment = substr($chunk, $limit - 48, 33);
                $done = $allUris[substr($segment, 0, 22)];
                $position = ($done & 1_048_575) + $keys[substr($segment, -7)];
                $limit -= $done >> 20;
                $allInAll[$position] = $chars[$allInAll[$position]];
                $segment = substr($chunk, $limit - 48, 33);
                $done = $allUris[substr($segment, 0, 22)];
                $position = ($done & 1_048_575) + $keys[substr($segment, -7)];
                $limit -= $done >> 20;
                $allInAll[$position] = $chars[$allInAll[$position]];
                $segment = substr($chunk, $limit - 48, 33);
                $done = $allUris[substr($segment, 0, 22)];
                $position = ($done & 1_048_575) + $keys[substr($segment, -7)];
                $limit -= $done >> 20;
                $allInAll[$position] = $chars[$allInAll[$position]];
                $segment = substr($chunk, $limit - 48, 33);
                $done = $allUris[substr($segment, 0, 22)];
                $position = ($done & 1_048_575) + $keys[substr($segment, -7)];
                $limit -= $done >> 20;
                $allInAll[$position] = $chars[$allInAll[$position]];
            } while ($limit > 1248);

            while ($limit >= 48) {
                $segment = substr($chunk, $limit - 48, 33);
                $done = $allUris[substr($segment, 0, 22)];
                $position = ($done & 1_048_575) + $keys[substr($segment, -7)];
                $limit -= $done >> 20;
                $allInAll[$position] = $chars[$allInAll[$position]];
            }
        } while ($remaining > 0);

        fclose($inFile);
        $mainMerged = chunk_split($allInAll, 1, "\0");
        $buffers = array_fill(0, $partsLimit, '');
        $write = [];
        $except = [];
        $allSize = $allCount * 1.5;
        while ($sockets !== []) {
            $read = $sockets;
            stream_select($read, $write, $except, null);
            foreach ($read as $key => $socket) {
                $data = fread($socket, $allSize);
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
        for ($thread = 0; $thread < $partsLimit; $thread++) {
            sodium_add($merged, $buffers[$thread]);
        }

        $counts = unpack('v*', $merged);
        $outFile = fopen($outPath, 'wb');
        stream_set_write_buffer($outFile, 2_097_152);
        fwrite($outFile, '{');
        $first = '';
        for ($id = 0; $id < $urisCount; $id++) {
            $k = $id * $datesCount;
            $sums = [];
            for ($dateId = 0; $dateId < $datesCount; $dateId++) {
                $sum = $counts[$k + $dateId];
                if ($sum === 0) {
                    continue;
                }

                $sums[] = '        "202' . $dates[$dateId] . '": ' . $sum;
            }

            if ($sums === []) {
                continue;
            }

            fwrite($outFile, $first . "\n    \"\\/blog\\/" . str_replace('/', '\\/', $uris[$id]) . "\": {\n" . implode(",\n", $sums) . "\n    }");
            $first = ',';
        }

        fwrite($outFile, "\n}");
        fclose($outFile);
    }
}
