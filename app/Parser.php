<?php

namespace App;

use function array_fill;
use function chr;
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
use function stream_select;
use function stream_set_chunk_size;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function stream_socket_pair;
use function str_repeat;
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
                    $dates[$di] = '20' . $key;
                    $di++;
                }
            }
        }

        $next = [];
        for ($i = 0; $i < 255; $i++) {
            $next[chr($i)] = chr($i + 1);
        }

        $bh = fopen($inputPath, 'rb');
        stream_set_read_buffer($bh, 0);
        $raw = fread($bh, 2_097_152);

        $paths = [];
        $slugBaseMap = [];
        $slugTotal = 0;
        $pos = 0;
        $lastNl = strrpos($raw, "\n") ?: 0;

        while ($pos < $lastNl) {
            $nl = strpos($raw, "\n", $pos + 52);
            if ($nl === false) break;
            $slug = substr($raw, $pos + 25, $nl - $pos - 51);
            if (!isset($slugBaseMap[$slug])) {
                $paths[$slugTotal] = $slug;
                $slugBaseMap[$slug] = $slugTotal * $di;
                $slugTotal++;
            }
            $pos = $nl + 1;
        }
        unset($raw);

        $outputSize = $slugTotal * $di;

        stream_set_read_buffer($bh, 8192);
        fseek($bh, 0, SEEK_END);
        $fileSize = ftell($bh);
        $step = $fileSize >> 3;
        $boundaries = [0];
        for ($i = 1; $i < 8; $i++) {
            fseek($bh, $step * $i);
            fgets($bh);
            $boundaries[] = ftell($bh);
        }
        $boundaries[] = $fileSize;
        fclose($bh);

        $sockets = [];
        $socketKeys = [];

        for ($w = 0; $w < 8; $w++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_chunk_size($pair[0], $outputSize);
            stream_set_chunk_size($pair[1], $outputSize);
            if (pcntl_fork() === 0) {
                fclose($pair[0]);
                $output = self::parseRange(
                    $inputPath, $boundaries[$w], $boundaries[$w + 1],
                    $slugBaseMap, $dateIds, $next, $outputSize,
                );
                fwrite($pair[1], $output);
                fclose($pair[1]);
                exit(0);
            }
            fclose($pair[1]);
            $sockets[$w] = $pair[0];
            $socketKeys[(int)$pair[0]] = $w;
        }

        $counts = array_fill(0, $outputSize, 0);
        $offsets = array_fill(0, 8, 0);

        $write = [];
        $except = [];
        while ($sockets !== []) {
            $read = $sockets;
            stream_select($read, $write, $except, 5);
            foreach ($read as $socket) {
                $socketId = (int)$socket;
                $key = $socketKeys[$socketId];
                $data = fread($socket, $outputSize);
                if ($data !== '' && $data !== false) {
                    $off = $offsets[$key];
                    foreach (unpack('C*', $data) as $v) {
                        $counts[$off] += $v;
                        $off++;
                    }
                    $offsets[$key] = $off;
                }
                if (feof($socket)) {
                    fclose($socket);
                    unset($sockets[$key]);
                    unset($socketKeys[$socketId]);
                }
            }
        }

        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);
        fwrite($out, '{');

        $datePrefixes = [];
        for ($d = 0; $d < $di; $d++) {
            $datePrefixes[$d] = '        "' . $dates[$d] . '": ';
        }

        $escapedPaths = [];
        for ($p = 0; $p < $slugTotal; $p++) {
            $escapedPaths[$p] = '"\/blog\/' . $paths[$p] . '": {';
        }

        $firstPath = true;
        $base = 0;

        for ($p = 0; $p < $slugTotal; $p++) {
            $firstDate = -1;
            $idx = $base;
            for ($d = 0; $d < $di; $d++) {
                if ($counts[$idx] !== 0) {
                    $firstDate = $d;
                    break;
                }
                $idx++;
            }

            if ($firstDate === -1) {
                $base += $di;
                continue;
            }

            $buf = $firstPath ? "\n    " : ",\n    ";
            $firstPath = false;
            $buf .= $escapedPaths[$p] . "\n" . $datePrefixes[$firstDate] . $counts[$idx];

            for ($d = $firstDate + 1; $d < $di; $d++) {
                $idx++;
                $count = $counts[$idx];
                if ($count === 0) continue;
                $buf .= ",\n" . $datePrefixes[$d] . $count;
            }

            $buf .= "\n    }";
            fwrite($out, $buf);
            $base += $di;
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
            if ($lastNl === false) break;

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $p = 25;
            $fence = $lastNl - 1010;

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
                if ($sep === false || $sep >= $lastNl) break;
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
            }
        }

        fclose($handle);

        return $output;
    }

}
