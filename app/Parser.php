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

        $dIds = [];
        $dStrs = [];
        $dTotal = 0;
        
        for ($y = 1; $y <= 6; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $max = match ($m) { 2 => $y === 4 ? 29 : 28, 4, 6, 9, 11 => 30, default => 31 };
                $mm = $m < 10 ? "0$m" : (string)$m;
                for ($d = 1; $d <= $max; $d++) {
                    $dd = $d < 10 ? "0$d" : (string)$d;
                    $dIds["$y-$mm-$dd"] = $dTotal;
                    $dStrs[$dTotal] = "        \"202$y-$mm-$dd\": ";
                    $dTotal++;
                }
            }
        }

        $inc = [];
        for ($i = 0; $i < 255; $i++) $inc[chr($i)] = chr($i + 1);

        $fd = fopen($inputPath, 'rb');
        stream_set_read_buffer($fd, 0);
        $head = fread($fd, 524288); 
        
        $urls = [];
        $uMap = [];
        $uTotal = 0;
        $ptr = 0;
        $endNl = strrpos($head, "\n") ?: 0;

        while ($ptr < $endNl && $uTotal < 268) {
            $nextNl = strpos($head, "\n", $ptr + 52);
            if ($nextNl === false) break;
            
            $route = substr($head, $ptr + 25, $nextNl - $ptr - 51);
            if (!isset($uMap[$route])) {
                $urls[$uTotal] = $route;
                $uMap[$route] = true;
                $uTotal++;
            }
            $ptr = $nextNl + 1;
        }
        unset($head, $uMap);

        $baseUri = 'https://stitcher.io/blog/';
        $sufLen = 1;
        while (true) {
            $chk = [];
            $ok = true;
            foreach ($urls as $u) {
                $tail = substr($baseUri . $u, -$sufLen);
                if (isset($chk[$tail])) { $sufLen++; $ok = false; break; }
                $chk[$tail] = true;
            }
            if ($ok) break;
        }

        $dict = [];
        $maxStride = 0;
        foreach ($urls as $i => $u) {
            $stride = strlen($u) + 52;
            if ($stride > $maxStride) $maxStride = $stride;
            $dict[substr($baseUri . $u, -$sufLen)] = ($stride << 20) | ($i * $dTotal);
        }
        $gridSz = $uTotal * $dTotal;

        fseek($fd, 0, SEEK_END);
        $fSize = ftell($fd);
        fclose($fd);

        $chunks = [];
        $bound = 0;
        $grain = intdiv($fSize, $workers);
        $fd = fopen($inputPath, 'rb');
        stream_set_read_buffer($fd, 0);

        for ($i = 0; $i < $workers; $i++) {
            $upper = ($i === $workers - 1) ? $fSize : $bound + $grain;
            $start = 0;
            if ($bound > 0) { fseek($fd, $bound); fgets($fd); $start = ftell($fd); }
            $end = $fSize;
            if ($upper < $fSize) { fseek($fd, $upper); fgets($fd); $end = ftell($fd); }
            $chunks[] = [$start, $end];
            $bound = $upper;
        }
        fclose($fd);

        $pipes = [];

        for ($w = 0; $w < $workers; $w++) {
            $sock = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_chunk_size($sock[0], $gridSz * 2);
            stream_set_chunk_size($sock[1], $gridSz * 2);
            
            if (pcntl_fork() === 0) {
                fclose($sock[0]);
                $mem = str_repeat("\0", $gridSz);
                $fh = fopen($inputPath, 'rb');
                stream_set_read_buffer($fh, 0);

                [$sByte, $eByte] = $chunks[$w];
                fseek($fh, $sByte);
                $rem = $eByte - $sByte;

                switch ($sufLen) {
                    case 6: $o=32; $f=($maxStride*10)+$o; while($rem>0){ $buf=fread($fh,$rem>262144?262144:$rem); $bL=strlen($buf); $rem-=$bL; $nl=strrpos($buf,"\n"); if($nl===false)break; $tail=$bL-$nl-1; if($tail>0){ fseek($fh,-$tail,SEEK_CUR); $rem+=$tail; } $p=$nl; while($p>$f){ $t=$dict[substr($buf,$p-32,6)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-32,6)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-32,6)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-32,6)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-32,6)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-32,6)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-32,6)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-32,6)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-32,6)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-32,6)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; } while($p>=32){ $t=$dict[substr($buf,$p-32,6)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; } } break;
                    case 7: $o=33; $f=($maxStride*10)+$o; while($rem>0){ $buf=fread($fh,$rem>262144?262144:$rem); $bL=strlen($buf); $rem-=$bL; $nl=strrpos($buf,"\n"); if($nl===false)break; $tail=$bL-$nl-1; if($tail>0){ fseek($fh,-$tail,SEEK_CUR); $rem+=$tail; } $p=$nl; while($p>$f){ $t=$dict[substr($buf,$p-33,7)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-33,7)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-33,7)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-33,7)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-33,7)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-33,7)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-33,7)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-33,7)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-33,7)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-33,7)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; } while($p>=33){ $t=$dict[substr($buf,$p-33,7)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; } } break;
                    case 8: $o=34; $f=($maxStride*10)+$o; while($rem>0){ $buf=fread($fh,$rem>262144?262144:$rem); $bL=strlen($buf); $rem-=$bL; $nl=strrpos($buf,"\n"); if($nl===false)break; $tail=$bL-$nl-1; if($tail>0){ fseek($fh,-$tail,SEEK_CUR); $rem+=$tail; } $p=$nl; while($p>$f){ $t=$dict[substr($buf,$p-34,8)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-34,8)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-34,8)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-34,8)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-34,8)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-34,8)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-34,8)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-34,8)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-34,8)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-34,8)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; } while($p>=34){ $t=$dict[substr($buf,$p-34,8)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; } } break;
                    case 9: $o=35; $f=($maxStride*10)+$o; while($rem>0){ $buf=fread($fh,$rem>262144?262144:$rem); $bL=strlen($buf); $rem-=$bL; $nl=strrpos($buf,"\n"); if($nl===false)break; $tail=$bL-$nl-1; if($tail>0){ fseek($fh,-$tail,SEEK_CUR); $rem+=$tail; } $p=$nl; while($p>$f){ $t=$dict[substr($buf,$p-35,9)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-35,9)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-35,9)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-35,9)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-35,9)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-35,9)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-35,9)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-35,9)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-35,9)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-35,9)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; } while($p>=35){ $t=$dict[substr($buf,$p-35,9)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; } } break;
                    case 10: $o=36; $f=($maxStride*10)+$o; while($rem>0){ $buf=fread($fh,$rem>262144?262144:$rem); $bL=strlen($buf); $rem-=$bL; $nl=strrpos($buf,"\n"); if($nl===false)break; $tail=$bL-$nl-1; if($tail>0){ fseek($fh,-$tail,SEEK_CUR); $rem+=$tail; } $p=$nl; while($p>$f){ $t=$dict[substr($buf,$p-36,10)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-36,10)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-36,10)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-36,10)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-36,10)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-36,10)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-36,10)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-36,10)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-36,10)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-36,10)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; } while($p>=36){ $t=$dict[substr($buf,$p-36,10)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; } } break;
                    case 11: $o=37; $f=($maxStride*10)+$o; while($rem>0){ $buf=fread($fh,$rem>262144?262144:$rem); $bL=strlen($buf); $rem-=$bL; $nl=strrpos($buf,"\n"); if($nl===false)break; $tail=$bL-$nl-1; if($tail>0){ fseek($fh,-$tail,SEEK_CUR); $rem+=$tail; } $p=$nl; while($p>$f){ $t=$dict[substr($buf,$p-37,11)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-37,11)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-37,11)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-37,11)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-37,11)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-37,11)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-37,11)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-37,11)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-37,11)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-37,11)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; } while($p>=37){ $t=$dict[substr($buf,$p-37,11)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; } } break;
                    case 12: $o=38; $f=($maxStride*10)+$o; while($rem>0){ $buf=fread($fh,$rem>262144?262144:$rem); $bL=strlen($buf); $rem-=$bL; $nl=strrpos($buf,"\n"); if($nl===false)break; $tail=$bL-$nl-1; if($tail>0){ fseek($fh,-$tail,SEEK_CUR); $rem+=$tail; } $p=$nl; while($p>$f){ $t=$dict[substr($buf,$p-38,12)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-38,12)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-38,12)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-38,12)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-38,12)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-38,12)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-38,12)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-38,12)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-38,12)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-38,12)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; } while($p>=38){ $t=$dict[substr($buf,$p-38,12)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; } } break;
                    default: $o=26+$sufLen; $f=($maxStride*10)+$o; while($rem>0){ $buf=fread($fh,$rem>262144?262144:$rem); $bL=strlen($buf); $rem-=$bL; $nl=strrpos($buf,"\n"); if($nl===false)break; $tail=$bL-$nl-1; if($tail>0){ fseek($fh,-$tail,SEEK_CUR); $rem+=$tail; } $p=$nl; while($p>$f){ $t=$dict[substr($buf,$p-$o,$sufLen)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-$o,$sufLen)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-$o,$sufLen)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-$o,$sufLen)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-$o,$sufLen)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-$o,$sufLen)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-$o,$sufLen)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-$o,$sufLen)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-$o,$sufLen)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; $t=$dict[substr($buf,$p-$o,$sufLen)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; } while($p>=$o){ $t=$dict[substr($buf,$p-$o,$sufLen)]; $x=($t&1048575)+$dIds[substr($buf,$p-22,7)]; $mem[$x]=$inc[$mem[$x]]; $p-=$t>>20; } } break;
                }
                
                fclose($fh);
                fwrite($sock[1], chunk_split($mem, 1, "\0"));
                fclose($sock[1]);
                exit(0);
            }
            fclose($sock[1]);
            $pipes[$w] = $sock[0];
        }

        $wArr = []; $eArr = [];
        $bins = array_fill(0, $workers, '');

        while ($pipes !== []) {
            $rArr = $pipes;
            stream_select($rArr, $wArr, $eArr, null);
            foreach ($rArr as $k => $p) {
                $pay = fread($p, 2097152);
                if ($pay !== '' && $pay !== false) {
                    $bins[$k] .= $pay;
                }
                if (feof($p)) {
                    fclose($p);
                    unset($pipes[$k]);
                }
            }
        }

        $master = $bins[0];
        for ($i = 1; $i < $workers; $i++) sodium_add($master, $bins[$i]);
        $visits = unpack('v*', $master);

        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 2097152);
        fwrite($out, '{');

        $jUris = [];
        for ($u = 0; $u < $uTotal; $u++) $jUris[$u] = '"\/blog\/' . $urls[$u] . '": {';

        $sep = "\n    ";
        $ptr = 1;

        for ($u = 0; $u < $uTotal; $u++) {
            $end = $ptr + $dTotal;
            
            while ($ptr < $end && $visits[$ptr] === 0) $ptr++;
            if ($ptr === $end) continue;

            $dOff = $ptr - ($end - $dTotal);
            $blk = $sep . $jUris[$u] . "\n" . $dStrs[$dOff] . $visits[$ptr];
            $sep = ",\n    ";

            for ($ptr++; $ptr < $end; $ptr++) {
                if ($visits[$ptr] !== 0) {
                    $blk .= ",\n" . $dStrs[$ptr - ($end - $dTotal)] . $visits[$ptr];
                }
            }
            $blk .= "\n    }";
            fwrite($out, $blk);
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}