<?php

namespace App;

use function array_fill;
use function array_values;
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
                $ymStr = ($y % 10) . "-{$mStr}-";
                for ($d = 1; $d <= $maxD; $d++) {
                    $dStr = ($d < 10 ? '0' : '') . $d;
                    $dateIds[$ymStr . $dStr] = $di;
                    $dates[$di] = "20{$y}-{$mStr}-{$dStr}";
                    $di++;
                }
            }
        }

        $next = [];
        $i = 255;
        while ($i-- > 0) {
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

        $noNew = 0;
        while ($pos < $lastNl) {
            $nl = strpos($raw, "\n", $pos + 52);
            if ($nl === false) break;
            $slug = substr($raw, $pos + 25, $nl - $pos - 51);
            if (!isset($slugBaseMap[$slug])) {
                $paths[$slugTotal] = $slug;
                $slugBaseMap[$slug] = $slugTotal * $di;
                $slugTotal++;
                $noNew = 0;
            } else if (++$noNew > 5000) {
                break;
            }
            $pos = $nl + 1;
        }
        unset($raw);

        $outputSize = $slugTotal * $di;

        fseek($bh, 0, SEEK_END);
        $fileSize = ftell($bh);
        $step = $fileSize >> 3;
        $boundaries = [0];
        for ($i = 1; $i < 8; $i++) {
            fseek($bh, $step * $i);
            fgets($bh);
            $boundaries[] = ftell($bh);
        }
        fclose($bh);
        $boundaries[] = $fileSize;

        $sockets = [];

        $w = 7;
        while ($w-- > 0) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_chunk_size($pair[0], $outputSize);
            stream_set_chunk_size($pair[1], $outputSize);
            if (pcntl_fork() === 0) {
                fwrite($pair[1], self::parseRange(
                    $inputPath, $boundaries[$w], $boundaries[$w + 1],
                    $slugBaseMap, $dateIds, $next, $outputSize,
                ));
                exit(0);
            }
            fclose($pair[1]);
            $sockets[$w] = $pair[0];
        }

        $parentOutput = self::parseRange(
            $inputPath, $boundaries[7], $boundaries[8],
            $slugBaseMap, $dateIds, $next, $outputSize,
        );

        $counts = array_values(unpack('C*', $parentOutput));
        unset($parentOutput);

        $offsets = array_fill(0, 7, 0);
        $write = [];
        $except = [];
        while ($sockets !== []) {
            $read = $sockets;
            stream_select($read, $write, $except, 5);
            foreach ($read as $key => $socket) {
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
                }
            }
        }

        self::writeJson($outputPath, $counts, $paths, $dates, $di, $slugTotal);
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
            $chunk = fread($handle, $remaining > 163_840 ? 163_840 : $remaining);
            $chunkLen = strlen($chunk);
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) break;

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, 0 - $tail, SEEK_CUR);
                $remaining += $tail;
            }

            $p = 25;
            $fence = $lastNl - 1000;

            while ($p < $fence) {
                $idx = $slugBaseMap[substr($chunk, $p, ($sep = strpos($chunk, ',', $p)) - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
                $idx = $slugBaseMap[substr($chunk, $p, ($sep = strpos($chunk, ',', $p)) - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
                $idx = $slugBaseMap[substr($chunk, $p, ($sep = strpos($chunk, ',', $p)) - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
                $idx = $slugBaseMap[substr($chunk, $p, ($sep = strpos($chunk, ',', $p)) - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
                $idx = $slugBaseMap[substr($chunk, $p, ($sep = strpos($chunk, ',', $p)) - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
                $idx = $slugBaseMap[substr($chunk, $p, ($sep = strpos($chunk, ',', $p)) - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
                $idx = $slugBaseMap[substr($chunk, $p, ($sep = strpos($chunk, ',', $p)) - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
                $idx = $slugBaseMap[substr($chunk, $p, ($sep = strpos($chunk, ',', $p)) - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
                $idx = $slugBaseMap[substr($chunk, $p, ($sep = strpos($chunk, ',', $p)) - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
                $idx = $slugBaseMap[substr($chunk, $p, ($sep = strpos($chunk, ',', $p)) - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
            }

            while ($p < $lastNl) {
                if (($sep = strpos($chunk, ',', $p)) === false || $sep >= $lastNl) break;
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
            }
        }

        return $output;
    }

    private static function writeJson(
        $outputPath, $counts, $paths, $dates, $dateCount, $slugCount,
    ) {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);
        fwrite($out, '{');

        $firstDatePrefixes = [];
        $datePrefixes = [];
        $d = $dateCount;
        while ($d-- > 0) {
            $firstDatePrefixes[$d] = "\n        \"" . $dates[$d] . '": ';
            $datePrefixes[$d] = ",\n        \"" . $dates[$d] . '": ';
        }

        $escapedPaths = [];
        $p = $slugCount;
        while ($p-- > 0) {
            $escapedPaths[$p] = "\"\/blog\/" . $paths[$p] . '": {';
        }

        $sep = "\n    ";
        $base = 0;

        for ($p = 0; $p < $slugCount; $p++) {
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
