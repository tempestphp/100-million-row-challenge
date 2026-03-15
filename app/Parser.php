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
use function intdiv;
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
                    2 => ($yr === 4) ? 29 : 28,
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
        unset($headBuf);

        $fm = [];
        $mx = 0;
        for ($u = 0; $u < $uriCount; $u++) {
            $jmp = strlen($uriList[$u]) + 52;
            if ($jmp > $mx) {
                $mx = $jmp;
            }
            $fm[substr('https://stitcher.io/blog/' . $uriList[$u], -22)] = ($jmp << 20) | ($u * $numDays);
        }

        $guard = ($mx * 12) + 48;
        $gridSize = $uriCount * $numDays;

        fseek($fd, 0, SEEK_END);
        $totalBytes = ftell($fd);

        $segs = [];
        for ($w = 0; $w < $workers; $w++) {
            $from = intdiv($totalBytes * $w, $workers);
            $to = intdiv($totalBytes * ($w + 1), $workers);

            if ($from > 0) {
                fseek($fd, $from);
                fgets($fd);
                $from = ftell($fd);
            }

            if ($w < $workers - 1) {
                fseek($fd, $to);
                fgets($fd);
                $to = ftell($fd);
            } else {
                $to = $totalBytes;
            }

            $segs[$w] = [$from, $to];
        }

        $ipc = [];
        for ($w = 0; $w < $workers - 1; $w++) {
            $pipe = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_chunk_size($pipe[0], $gridSize * 2);
            stream_set_chunk_size($pipe[1], $gridSize * 2);

            if (pcntl_fork() === 0) {
                fclose($pipe[0]);
                $st = str_repeat("\0", $gridSize);
                $in = fopen($inputPath, 'rb');
                stream_set_read_buffer($in, 0);

                [$from, $to] = $segs[$w];
                fseek($in, $from);
                $rem = $to - $from;

                while ($rem > 0) {
                    $buf = fread($in, $rem > 0b100000000000000000 ? 0b100000000000000000 : $rem);
                    $n = strlen($buf);
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

                    while ($p > $guard) {
                        $v=$fm[substr($buf,$p-48,22)]; $idx=($v&0b11111111111111111111)+$tm[substr($buf,$p-22,7)]; $p-=$v>>20; $st[$idx]=$up[$st[$idx]];
                        $v=$fm[substr($buf,$p-48,22)]; $idx=($v&0b11111111111111111111)+$tm[substr($buf,$p-22,7)]; $p-=$v>>20; $st[$idx]=$up[$st[$idx]];
                        $v=$fm[substr($buf,$p-48,22)]; $idx=($v&0b11111111111111111111)+$tm[substr($buf,$p-22,7)]; $p-=$v>>20; $st[$idx]=$up[$st[$idx]];
                        $v=$fm[substr($buf,$p-48,22)]; $idx=($v&0b11111111111111111111)+$tm[substr($buf,$p-22,7)]; $p-=$v>>20; $st[$idx]=$up[$st[$idx]];
                        $v=$fm[substr($buf,$p-48,22)]; $idx=($v&0b11111111111111111111)+$tm[substr($buf,$p-22,7)]; $p-=$v>>20; $st[$idx]=$up[$st[$idx]];
                        $v=$fm[substr($buf,$p-48,22)]; $idx=($v&0b11111111111111111111)+$tm[substr($buf,$p-22,7)]; $p-=$v>>20; $st[$idx]=$up[$st[$idx]];
                        $v=$fm[substr($buf,$p-48,22)]; $idx=($v&0b11111111111111111111)+$tm[substr($buf,$p-22,7)]; $p-=$v>>20; $st[$idx]=$up[$st[$idx]];
                        $v=$fm[substr($buf,$p-48,22)]; $idx=($v&0b11111111111111111111)+$tm[substr($buf,$p-22,7)]; $p-=$v>>20; $st[$idx]=$up[$st[$idx]];
                        $v=$fm[substr($buf,$p-48,22)]; $idx=($v&0b11111111111111111111)+$tm[substr($buf,$p-22,7)]; $p-=$v>>20; $st[$idx]=$up[$st[$idx]];
                        $v=$fm[substr($buf,$p-48,22)]; $idx=($v&0b11111111111111111111)+$tm[substr($buf,$p-22,7)]; $p-=$v>>20; $st[$idx]=$up[$st[$idx]];
                        $v=$fm[substr($buf,$p-48,22)]; $idx=($v&0b11111111111111111111)+$tm[substr($buf,$p-22,7)]; $p-=$v>>20; $st[$idx]=$up[$st[$idx]];
                        $v=$fm[substr($buf,$p-48,22)]; $idx=($v&0b11111111111111111111)+$tm[substr($buf,$p-22,7)]; $p-=$v>>20; $st[$idx]=$up[$st[$idx]];
                    }

                    while ($p >= 48) {
                        $v=$fm[substr($buf,$p-48,22)]; $idx=($v&0b11111111111111111111)+$tm[substr($buf,$p-22,7)]; $p-=$v>>20; $st[$idx]=$up[$st[$idx]];
                    }
                }

                fclose($in);
                fwrite($pipe[1], chunk_split($st, 1, "\0"));
                exit(0);
            }

            fclose($pipe[1]);
            $ipc[$w] = $pipe[0];
        }

        $st = str_repeat("\0", $gridSize);
        [$from, $to] = $segs[$workers - 1];
        fseek($fd, $from);
        $rem = $to - $from;

        while ($rem > 0) {
            $buf = fread($fd, $rem > 0b100000000000000000 ? 0b100000000000000000 : $rem);
            $n = strlen($buf);
            $rem -= $n;

            $brk = strrpos($buf, "\n");
            if ($brk === false) {
                break;
            }

            $ov = $n - $brk - 1;
            if ($ov > 0) {
                fseek($fd, -$ov, SEEK_CUR);
                $rem += $ov;
            }

            $p = $brk;

            while ($p > $guard) {
                $v=$fm[substr($buf,$p-48,22)]; $idx=($v&0b11111111111111111111)+$tm[substr($buf,$p-22,7)]; $p-=$v>>20; $st[$idx]=$up[$st[$idx]];
                $v=$fm[substr($buf,$p-48,22)]; $idx=($v&0b11111111111111111111)+$tm[substr($buf,$p-22,7)]; $p-=$v>>20; $st[$idx]=$up[$st[$idx]];
                $v=$fm[substr($buf,$p-48,22)]; $idx=($v&0b11111111111111111111)+$tm[substr($buf,$p-22,7)]; $p-=$v>>20; $st[$idx]=$up[$st[$idx]];
                $v=$fm[substr($buf,$p-48,22)]; $idx=($v&0b11111111111111111111)+$tm[substr($buf,$p-22,7)]; $p-=$v>>20; $st[$idx]=$up[$st[$idx]];
                $v=$fm[substr($buf,$p-48,22)]; $idx=($v&0b11111111111111111111)+$tm[substr($buf,$p-22,7)]; $p-=$v>>20; $st[$idx]=$up[$st[$idx]];
                $v=$fm[substr($buf,$p-48,22)]; $idx=($v&0b11111111111111111111)+$tm[substr($buf,$p-22,7)]; $p-=$v>>20; $st[$idx]=$up[$st[$idx]];
                $v=$fm[substr($buf,$p-48,22)]; $idx=($v&0b11111111111111111111)+$tm[substr($buf,$p-22,7)]; $p-=$v>>20; $st[$idx]=$up[$st[$idx]];
                $v=$fm[substr($buf,$p-48,22)]; $idx=($v&0b11111111111111111111)+$tm[substr($buf,$p-22,7)]; $p-=$v>>20; $st[$idx]=$up[$st[$idx]];
                $v=$fm[substr($buf,$p-48,22)]; $idx=($v&0b11111111111111111111)+$tm[substr($buf,$p-22,7)]; $p-=$v>>20; $st[$idx]=$up[$st[$idx]];
                $v=$fm[substr($buf,$p-48,22)]; $idx=($v&0b11111111111111111111)+$tm[substr($buf,$p-22,7)]; $p-=$v>>20; $st[$idx]=$up[$st[$idx]];
                $v=$fm[substr($buf,$p-48,22)]; $idx=($v&0b11111111111111111111)+$tm[substr($buf,$p-22,7)]; $p-=$v>>20; $st[$idx]=$up[$st[$idx]];
                $v=$fm[substr($buf,$p-48,22)]; $idx=($v&0b11111111111111111111)+$tm[substr($buf,$p-22,7)]; $p-=$v>>20; $st[$idx]=$up[$st[$idx]];
            }

            while ($p >= 48) {
                $v=$fm[substr($buf,$p-48,22)]; $idx=($v&0b11111111111111111111)+$tm[substr($buf,$p-22,7)]; $p-=$v>>20; $st[$idx]=$up[$st[$idx]];
            }
        }

        fclose($fd);
        $merged = chunk_split($st, 1, "\0");
        $bins = array_fill(0, $workers - 1, '');

        while ($ipc !== []) {
            $rA = $ipc;
            $wA = [];
            $eA = [];
            stream_select($rA, $wA, $eA, null);
            foreach ($rA as $k => $sock) {
                $payload = fread($sock, $gridSize * 2);
                if ($payload !== '' && $payload !== false) {
                    $bins[$k] .= $payload;
                }
                if (feof($sock)) {
                    fclose($sock);
                    unset($ipc[$k]);
                }
            }
        }

        for ($w = 0; $w < $workers - 1; $w++) {
            sodium_add($merged, $bins[$w]);
        }
        $visits = unpack('v*', $merged);

        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 0b1000000000000000000000);
        fwrite($out, '{');

        $fmtUris = [];
        for ($u = 0; $u < $uriCount; $u++) {
            $fmtUris[$u] = '"\/blog\/' . $uriList[$u] . '": {';
        }

        $sep = "\n    ";
        $ptrBase = 1;
        for ($u = 0; $u < $uriCount; $u++) {
            $start = $ptrBase;
            $limit = $ptrBase + $numDays;

            while ($ptrBase < $limit && $visits[$ptrBase] === 0) {
                $ptrBase++;
            }

            if ($ptrBase === $limit) {
                continue;
            }

            $json = $sep . $fmtUris[$u] . "\n" . $timeStr[$ptrBase - $start] . $visits[$ptrBase];
            $sep = ",\n    ";

            for ($ptrBase++; $ptrBase < $limit; $ptrBase++) {
                $hits = $visits[$ptrBase];
                if ($hits !== 0) {
                    $json .= ",\n" . $timeStr[$ptrBase - $start] . $hits;
                }
            }

            $json .= "\n    }";
            fwrite($out, $json);
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
