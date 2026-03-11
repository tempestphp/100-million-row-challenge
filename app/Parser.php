<?php

namespace App;

use function array_fill;
use function array_values;
use function chr;
use function chunk_split;
use function fclose;
use function feof;
use function fopen;
use function fread;
use function fseek;
use function fwrite;
use function sodium_add;
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
        if (gc_enabled()) {
            gc_collect_cycles();
        }
        if (function_exists('gc_mem_caches')) {
            gc_mem_caches();
        }
        gc_disable();
        if (function_exists('pcntl_setpriority')) {
            @pcntl_setpriority(-10);
        }

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
                $ymStr = ($y % 10) . '-' . $mStr . '-';
                for ($d = 1; $d <= $maxD; $d++) {
                    $dStr = ($d < 10 ? '0' : '') . $d;
                    $dateIds[$ymStr . $dStr] = $di;
                    $dates[$di] = '20' . $y . '-' . $mStr . '-' . $dStr;
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
            } elseif (++$noNew > 5000) {
                break;
            }
            $pos = $nl + 1;
        }
        unset($raw);

        $outputSize = $slugTotal * $di;

        fseek($bh, 0, SEEK_END);
        $fileSize = ftell($bh);
        fclose($bh);

        // Work-stealing: shared counter via file lock
        $counterFile = tempnam(sys_get_temp_dir(), 'cnt');
        file_put_contents($counterFile, pack('P', 0));
        $workUnit = 8_000_000;

        $sockets = [];

        $w = 8;
        while ($w-- > 0) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_chunk_size($pair[0], $outputSize << 1);
            stream_set_chunk_size($pair[1], $outputSize << 1);
            if (pcntl_fork() === 0) {
                $output = str_repeat("\0", $outputSize);
                $handle = fopen($inputPath, 'rb');
                stream_set_read_buffer($handle, 0);
                $alignHandle = fopen($inputPath, 'rb');
                $ctr = fopen($counterFile, 'c+b');

                while (true) {
                    flock($ctr, LOCK_EX);
                    fseek($ctr, 0);
                    $off = unpack('P', fread($ctr, 8))[1];
                    if ($off >= $fileSize) {
                        flock($ctr, LOCK_UN);
                        break;
                    }
                    $nextOff = $off + $workUnit;
                    if ($nextOff > $fileSize) $nextOff = $fileSize;
                    fseek($ctr, 0);
                    fwrite($ctr, pack('P', $nextOff));
                    flock($ctr, LOCK_UN);

                    if ($off > 0) {
                        fseek($alignHandle, $off);
                        fgets($alignHandle);
                        $start = ftell($alignHandle);
                    } else {
                        $start = 0;
                    }

                    if ($nextOff < $fileSize) {
                        fseek($alignHandle, $nextOff);
                        fgets($alignHandle);
                        $end = ftell($alignHandle);
                    } else {
                        $end = $fileSize;
                    }

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
                            fseek($handle, -$tail, SEEK_CUR);
                            $remaining += $tail;
                        }

                        $p = 25;
                        $fence = $lastNl - 1010;

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
                }

                fclose($handle);
                fclose($alignHandle);
                fclose($ctr);
                fwrite($pair[1], chunk_split($output, 1, "\0"));
                exit(0);
            }
            fclose($pair[1]);
            $sockets[$w] = $pair[0];
        }
        $buffers = array_fill(0, 8, '');

        $write = [];
        $except = [];
        while ($sockets !== []) {
            $read = $sockets;
            stream_select($read, $write, $except, 5);
            foreach ($read as $key => $socket) {
                $data = fread($socket, $outputSize << 1);
                if ($data !== '' && $data !== false) {
                    $buffers[$key] .= $data;
                }
                if (feof($socket)) {
                    fclose($socket);
                    unset($sockets[$key]);
                }
            }
        }

        @unlink($counterFile);

        $merged = $buffers[0];
        for ($w = 1; $w < 8; $w++) {
            sodium_add($merged, $buffers[$w]);
        }
        $counts = array_values(unpack('v*', $merged));

        self::writeJson($outputPath, $counts, $paths, $dates, $di, $slugTotal);
    }

    private static function writeJson(
        $outputPath, $counts, $paths, $dates, $dateCount, $slugCount,
    ) {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);

        $datePrefixes = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "' . $dates[$d] . '": ';
        }

        $escapedPaths = [];
        for ($p = 0; $p < $slugCount; $p++) {
            $escapedPaths[$p] = '"\/blog\/' . str_replace('/', '\/', $paths[$p]) . '": {';
        }

        $sep = "\n    ";
        $base = 0;
        $buf = '{';

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

            if ($firstDate !== -1) {
                $buf .= $sep . $escapedPaths[$p] . "\n" . $datePrefixes[$firstDate] . $counts[$idx];
                $sep = ",\n    ";

                for ($d = $firstDate + 1; $d < $dateCount; $d++) {
                    $idx++;
                    if ($counts[$idx] === 0) continue;
                    $buf .= ",\n" . $datePrefixes[$d] . $counts[$idx];
                }

                $buf .= "\n    }";

                if (strlen($buf) > 2_097_152) {
                    fwrite($out, $buf);
                    $buf = '';
                }
            }
            $base += $dateCount;
        }

        fwrite($out, $buf . "\n}");
        fclose($out);
    }
}
