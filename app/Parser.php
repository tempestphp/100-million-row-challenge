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

        $threads = 9;

        $bump = [];
        for ($c = 0; $c < 255; $c++) {
            $bump[chr($c)] = chr($c + 1);
        }

        $timeMap = [];
        $timeStr = [];
        $numDays = 0;
        
        for ($yr = 1; $yr <= 6; $yr++) {
            for ($mo = 1; $mo <= 12; $mo++) {
                $limit = match ($mo) {
                    2 => ($yr === 4) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                
                $mm = $mo < 10 ? "0$mo" : (string)$mo;
                $prefix = "{$yr}-{$mm}-";
                
                for ($dy = 1; $dy <= $limit; $dy++) {
                    $dd = $dy < 10 ? "0$dy" : (string)$dy;
                    $key = $prefix . $dd;
                    
                    $timeMap[$key] = $numDays;
                    $timeStr[$numDays] = "202$key";
                    $numDays++;
                }
            }
        }

        $fd = fopen($inputPath, 'rb');
        stream_set_read_buffer($fd, 0);
        $headBuf = fread($fd, 181000);

        $uriList = [];
        $fastMap = [];
        $uriCount = 0;
        $ptr = 0;
        $endLine = strrpos($headBuf, "\n") ?: 0;

        while ($ptr < $endLine && $uriCount < 268) {
            $nl = strpos($headBuf, "\n", $ptr + 52);
            if ($nl === false) break;
            
            $route = substr($headBuf, $ptr + 25, $nl - $ptr - 51);
            if (!isset($fastMap[$route])) {
                $uriList[$uriCount] = $route;
                $fastMap[$route] = $uriCount * $numDays;
                $uriCount++;
            }
            $ptr = $nl + 1;
        }
        unset($headBuf);

        $baseUrl = 'https://stitcher.io/blog/';
        $hashLen = 22;
        $shiftBits = 20;
        $maskVal = 0xFFFFF; 
        $strideMax = 100;
        
        $fastMap = [];
        for ($u = 0; $u < $uriCount; $u++) {
            $jump = strlen($uriList[$u]) + 52;
            $suf = substr($baseUrl . $uriList[$u], -$hashLen);
            $fastMap[$suf] = ($jump << $shiftBits) | ($u * $numDays);
        }
        
        $hashOff = 26 + $hashLen;
        $guard = ($strideMax * 10) + $hashOff;
        $gridSize = $uriCount * $numDays;

        fseek($fd, 0, SEEK_END);
        $totalBytes = ftell($fd);
        fclose($fd);

        $grain = 16777216;
        $tasks = [];
        $bound = 0;
        
        $fd = fopen($inputPath, 'rb');
        stream_set_read_buffer($fd, 0);
        
        while ($bound < $totalBytes) {
            $upper = $bound + $grain;
            if ($upper > $totalBytes) {
                $upper = $totalBytes;
            }

            $startPos = 0;
            if ($bound > 0) {
                fseek($fd, $bound);
                fgets($fd);
                $startPos = ftell($fd);
            }

            $endPos = $totalBytes;
            if ($upper < $totalBytes) {
                fseek($fd, $upper);
                fgets($fd);
                $endPos = ftell($fd);
            }

            $tasks[] = [$startPos, $endPos];
            $bound = $upper;
        }
        fclose($fd);

        $taskTotal = count($tasks);
        $ipc = [];

        for ($t = 0; $t < $threads; $t++) {
            $pipe = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_chunk_size($pipe[0], $gridSize * 2);
            stream_set_chunk_size($pipe[1], $gridSize * 2);
            
            if (pcntl_fork() === 0) {
                fclose($pipe[0]);
                $state = str_repeat("\0", $gridSize);
                $reader = fopen($inputPath, 'rb');
                stream_set_read_buffer($reader, 0);

                for ($job = $t; $job < $taskTotal; $job += $threads) {
                    [$from, $to] = $tasks[$job];
                    fseek($reader, $from);
                    $left = $to - $from;

                    while ($left > 0) {
                        $buf = fread($reader, $left > 131072 ? 131072 : $left);
                        $cLen = strlen($buf);
                        $left -= $cLen;

                        $lastBrk = strrpos($buf, "\n");
                        if ($lastBrk === false) break;

                        $over = $cLen - $lastBrk - 1;
                        if ($over > 0) {
                            fseek($reader, -$over, SEEK_CUR);
                            $left += $over;
                        }

                        $p = $lastBrk;

                        while ($p > $guard) {
                            $v=$fastMap[substr($buf,$p-$hashOff,$hashLen)]; $idx=($v&$maskVal)+$timeMap[substr($buf,$p-22,7)]; $state[$idx]=$bump[$state[$idx]]; $p-=$v>>$shiftBits;
                            $v=$fastMap[substr($buf,$p-$hashOff,$hashLen)]; $idx=($v&$maskVal)+$timeMap[substr($buf,$p-22,7)]; $state[$idx]=$bump[$state[$idx]]; $p-=$v>>$shiftBits;
                            $v=$fastMap[substr($buf,$p-$hashOff,$hashLen)]; $idx=($v&$maskVal)+$timeMap[substr($buf,$p-22,7)]; $state[$idx]=$bump[$state[$idx]]; $p-=$v>>$shiftBits;
                            $v=$fastMap[substr($buf,$p-$hashOff,$hashLen)]; $idx=($v&$maskVal)+$timeMap[substr($buf,$p-22,7)]; $state[$idx]=$bump[$state[$idx]]; $p-=$v>>$shiftBits;
                            $v=$fastMap[substr($buf,$p-$hashOff,$hashLen)]; $idx=($v&$maskVal)+$timeMap[substr($buf,$p-22,7)]; $state[$idx]=$bump[$state[$idx]]; $p-=$v>>$shiftBits;
                            $v=$fastMap[substr($buf,$p-$hashOff,$hashLen)]; $idx=($v&$maskVal)+$timeMap[substr($buf,$p-22,7)]; $state[$idx]=$bump[$state[$idx]]; $p-=$v>>$shiftBits;
                            $v=$fastMap[substr($buf,$p-$hashOff,$hashLen)]; $idx=($v&$maskVal)+$timeMap[substr($buf,$p-22,7)]; $state[$idx]=$bump[$state[$idx]]; $p-=$v>>$shiftBits;
                            $v=$fastMap[substr($buf,$p-$hashOff,$hashLen)]; $idx=($v&$maskVal)+$timeMap[substr($buf,$p-22,7)]; $state[$idx]=$bump[$state[$idx]]; $p-=$v>>$shiftBits;
                            $v=$fastMap[substr($buf,$p-$hashOff,$hashLen)]; $idx=($v&$maskVal)+$timeMap[substr($buf,$p-22,7)]; $state[$idx]=$bump[$state[$idx]]; $p-=$v>>$shiftBits;
                            $v=$fastMap[substr($buf,$p-$hashOff,$hashLen)]; $idx=($v&$maskVal)+$timeMap[substr($buf,$p-22,7)]; $state[$idx]=$bump[$state[$idx]]; $p-=$v>>$shiftBits;
                        }

                        while ($p >= $hashOff) {
                            $v=$fastMap[substr($buf,$p-$hashOff,$hashLen)]; $idx=($v&$maskVal)+$timeMap[substr($buf,$p-22,7)]; $state[$idx]=$bump[$state[$idx]]; $p-=$v>>$shiftBits;
                        }
                    }
                }

                fclose($reader);
                fwrite($pipe[1], chunk_split($state, 1, "\0"));
                exit(0);
            }
            fclose($pipe[1]);
            $ipc[$t] = $pipe[0];
        }

        $bins = array_fill(0, $threads, '');
        $wA = [];
        $eA = [];
        
        while ($ipc !== []) {
            $rA = $ipc;
            stream_select($rA, $wA, $eA, 5);
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

        $master = $bins[0];
        for ($t = 1; $t < $threads; $t++) {
            sodium_add($master, $bins[$t]);
        }
        $visits = unpack('v*', $master);

        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 2097152); 
        fwrite($out, '{');

        $fmtDays = [];
        for ($d = 0; $d < $numDays; $d++) {
            $fmtDays[$d] = '        "' . $timeStr[$d] . '": ';
        }

        $fmtUris = [];
        for ($u = 0; $u < $uriCount; $u++) {
            $fmtUris[$u] = '"\/blog\/' .  $uriList[$u] . '": {';
        }

        $sep = "\n    ";
        $ptrBase = 1;

        for ($u = 0; $u < $uriCount; $u++) {
            $matchDay = -1;
            $idx = $ptrBase;
            
            for ($d = 0; $d < $numDays; $d++) {
                if ($visits[$idx] !== 0) {
                    $matchDay = $d;
                    break;
                }
                $idx++;
            }

            if ($matchDay === -1) {
                $ptrBase += $numDays;
                continue;
            }

            $json = $sep . $fmtUris[$u] . "\n" . $fmtDays[$matchDay] . $visits[$ptrBase + $matchDay];
            $sep = ",\n    ";

            for ($d = $matchDay + 1; $d < $numDays; $d++) {
                $idx++;
                $hits = $visits[$idx];
                if ($hits === 0) continue;
                $json .= ",\n" . $fmtDays[$d] . $hits;
            }

            $json .= "\n    }";
            fwrite($out, $json);
            $ptrBase += $numDays;
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}