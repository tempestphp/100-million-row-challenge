<?php

namespace App;

use function array_fill;
use function chr;
use function chunk_split;
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

        $up = [];
        for ($c = 0; $c < 255; $c++) {
            $up[chr($c)] = chr($c + 1);
        }
        $up[chr(255)] = chr(255);

        $tm = [];
        $timeStr = [];
        $numDays = 0;

        for ($yr = 1; $yr <= 6; $yr++) {
            for ($mo = 1; $mo <= 12; $mo++) {
                $limit = match ($mo) {
                    2 => $yr === 4 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };

                $mm = $mo < 10 ? "0$mo" : (string) $mo;
                $prefix = "{$yr}-{$mm}-";

                for ($dy = 1; $dy <= $limit; $dy++) {
                    $dd = $dy < 10 ? "0$dy" : (string) $dy;
                    $key = $prefix . $dd;

                    $tm[$key] = $numDays;
                    $timeStr[$numDays] = "        \"202$key\": ";
                    $numDays++;
                }
            }
        }

        $fd = fopen($inputPath, 'rb');
        stream_set_read_buffer($fd, 0);
        $headBuf = fread($fd, 181000);

        $uriList = [];
        $seen = [];
        $uriCount = 0;
        $ptr = 0;
        $endLine = strrpos($headBuf, "\n") ?: 0;

        while ($ptr < $endLine && $uriCount < 268) {
            $nl = strpos($headBuf, "\n", $ptr + 52);
            if ($nl === false) {
                break;
            }

            $route = substr($headBuf, $ptr + 25, $nl - $ptr - 51);
            if (!isset($seen[$route])) {
                $uriList[$uriCount] = $route;
                $seen[$route] = true;
                $uriCount++;
            }

            $ptr = $nl + 1;
        }
        unset($headBuf, $seen);

        $baseUrl = 'https://stitcher.io/blog/';
        $fm = [];
        $mx = 0;

        for ($u = 0; $u < $uriCount; $u++) {
            $route = $uriList[$u];
            $jmp = strlen($route) + 52;
            if ($jmp > $mx) {
                $mx = $jmp;
            }

            $fm[substr($baseUrl . $route, -22)] = ($jmp << 20) | ($u * $numDays);
        }

        // 8-unroll en vez de 12: menos código caliente y menos temporales.
        $batchLimit = ($mx << 3) + 48;

        fseek($fd, 0, SEEK_END);
        $totalBytes = ftell($fd);

        $tasks = [];
        $bound = 0;
        $startPos = 0;
        $jobN = 0;

        stream_set_read_buffer($fd, 0);

        while ($bound < $totalBytes) {
            $upper = $bound + 33554432;
            if ($upper > $totalBytes) {
                $upper = $totalBytes;
            }

            if ($upper < $totalBytes) {
                fseek($fd, $upper);
                fgets($fd);
                $nextStartPos = ftell($fd);
            } else {
                $nextStartPos = $totalBytes;
            }

            $tasks[$jobN++] = [$startPos, $nextStartPos];
            $startPos = $nextStartPos;
            $bound = $upper;
        }

        fclose($fd);

        $ipc = [];

        for ($t = 0; $t < 8; $t++) {
            $pipe = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_chunk_size($pipe[0], 1174376);
            stream_set_chunk_size($pipe[1], 1174376);

            if (pcntl_fork() === 0) {
                fclose($pipe[0]);

                $st = str_repeat("\0", 587188);
                $in = fopen($inputPath, 'rb');
                stream_set_read_buffer($in, 0);

                for ($job = $t; $job < $jobN; $job += 8) {
                    $task = $tasks[$job];
                    $from = $task[0];
                    $to = $task[1];

                    fseek($in, $from);
                    $rem = $to - $from;

                    while ($rem > 0) {
                        $buf = fread($in, $rem > 131072 ? 131072 : $rem);
                        $n = strlen($buf);
                        if ($n === 0) {
                            break;
                        }

                        $rem -= $n;

                        $brk = strrpos($buf, "\n");
                        if ($brk === false) {
                            break;
                        }

                        $ov = $n - $brk - 1;
                        if ($ov > 0) {
                            fseek($in, -$ov, SEEK_CUR);
                            $rem += $ov;
                        }

                        $p = $brk;

                        while ($p > $batchLimit) {
                            $v = $fm[substr($buf, $p - 48, 22)];
                            $idx = ($v & 0xFFFFF) + $tm[substr($buf, $p - 22, 7)];
                            $p -= $v >> 20;
                            $st[$idx] = $up[$st[$idx]];

                            $v = $fm[substr($buf, $p - 48, 22)];
                            $idx = ($v & 0xFFFFF) + $tm[substr($buf, $p - 22, 7)];
                            $p -= $v >> 20;
                            $st[$idx] = $up[$st[$idx]];

                            $v = $fm[substr($buf, $p - 48, 22)];
                            $idx = ($v & 0xFFFFF) + $tm[substr($buf, $p - 22, 7)];
                            $p -= $v >> 20;
                            $st[$idx] = $up[$st[$idx]];

                            $v = $fm[substr($buf, $p - 48, 22)];
                            $idx = ($v & 0xFFFFF) + $tm[substr($buf, $p - 22, 7)];
                            $p -= $v >> 20;
                            $st[$idx] = $up[$st[$idx]];

                            $v = $fm[substr($buf, $p - 48, 22)];
                            $idx = ($v & 0xFFFFF) + $tm[substr($buf, $p - 22, 7)];
                            $p -= $v >> 20;
                            $st[$idx] = $up[$st[$idx]];

                            $v = $fm[substr($buf, $p - 48, 22)];
                            $idx = ($v & 0xFFFFF) + $tm[substr($buf, $p - 22, 7)];
                            $p -= $v >> 20;
                            $st[$idx] = $up[$st[$idx]];

                            $v = $fm[substr($buf, $p - 48, 22)];
                            $idx = ($v & 0xFFFFF) + $tm[substr($buf, $p - 22, 7)];
                            $p -= $v >> 20;
                            $st[$idx] = $up[$st[$idx]];

                            $v = $fm[substr($buf, $p - 48, 22)];
                            $idx = ($v & 0xFFFFF) + $tm[substr($buf, $p - 22, 7)];
                            $p -= $v >> 20;
                            $st[$idx] = $up[$st[$idx]];
                        }

                        while ($p > 47) {
                            $v = $fm[substr($buf, $p - 48, 22)];
                            $idx = ($v & 0xFFFFF) + $tm[substr($buf, $p - 22, 7)];
                            $p -= $v >> 20;
                            $st[$idx] = $up[$st[$idx]];
                        }
                    }
                }

                fclose($in);
                fwrite($pipe[1], chunk_split($st, 1, "\0"));
                fclose($pipe[1]);
                exit(0);
            }

            fclose($pipe[1]);
            $ipc[$t] = $pipe[0];
        }

        $bins = array_fill(0, 8, '');

        while ($ipc !== []) {
            $rA = $ipc;
            $wA = [];
            $eA = [];
            stream_select($rA, $wA, $eA, null);

            foreach ($rA as $k => $sock) {
                $payload = fread($sock, 8388608);
                if ($payload !== false && $payload !== '') {
                    $bins[$k] .= $payload;
                }

                if (feof($sock)) {
                    fclose($sock);
                    unset($ipc[$k]);
                }
            }
        }

        $master = $bins[0];
        for ($t = 1; $t < 8; $t++) {
            sodium_add($master, $bins[$t]);
        }

        $visits = unpack('v*', $master);

        self::writeJson($outputPath, $visits, $uriList, $timeStr, $uriCount, $numDays);
    }

    private static function writeJson($outputPath, $visits, $uriList, $timeStr, $uriCount, $numDays): void
    {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 2097152);
        fwrite($out, '{');

        $fmtUris = [];
        for ($u = 0; $u < $uriCount; $u++) {
            $fmtUris[$u] = '"\/blog\/' . $uriList[$u] . '": {';
        }

        $sep = "\n    ";
        $base = 1;

        for ($u = 0; $u < $uriCount; $u++) {
            $limit = $base + $numDays;
            $p = $base;

            while ($p < $limit && $visits[$p] === 0) {
                $p++;
            }

            if ($p === $limit) {
                $base = $limit;
                continue;
            }

            $dayBase = $base - 1;
            $json = $sep . $fmtUris[$u] . "\n" . $timeStr[$p - $base] . $visits[$p];
            $sep = ",\n    ";

            for ($p++; $p < $limit; $p++) {
                $hits = $visits[$p];
                if ($hits !== 0) {
                    $json .= ",\n" . $timeStr[$p - $base] . $hits;
                }
            }

            $json .= "\n    }";
            fwrite($out, $json);

            $base = $limit;
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
