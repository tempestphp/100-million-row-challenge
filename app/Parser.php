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


        $dateTbl = [];
        $jsonDates = [];
        $totalDates = 0;
        
        for ($y = 1; $y <= 6; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $dim = match ($m) { 2 => $y === 4 ? 29 : 28, 4, 6, 9, 11 => 30, default => 31 };
                $mStr = $m < 10 ? "0$m" : (string)$m;
                
                for ($d = 1; $d <= $dim; $d++) {
                    $dStr = $d < 10 ? "0$d" : (string)$d;
                    $dateTbl["$y-$mStr-$dStr"] = $totalDates;
                    $jsonDates[$totalDates] = "        \"202$y-$mStr-$dStr\": ";
                    $totalDates++;
                }
            }
        }


        $add1 = [];
        for ($i = 0; $i < 255; $i++) {
            $add1[chr($i)] = chr($i + 1);
        }
        $add1[chr(255)] = chr(255);


        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);
        $head = fread($fh, 262144);
        
        $slugs = [];
        $slugSeen = [];
        $slugTotal = 0;
        $cursor = 0;
        $lastNl = strrpos($head, "\n") ?: 0;

        while ($cursor < $lastNl && $slugTotal < 268) {
            $nextNl = strpos($head, "\n", $cursor + 52);
            if ($nextNl === false) break;
            
            $slug = substr($head, $cursor + 25, $nextNl - $cursor - 51);
            if (!isset($slugSeen[$slug])) {
                $slugs[$slugTotal] = $slug;
                $slugSeen[$slug] = true;
                $slugTotal++;
            }
            $cursor = $nextNl + 1;
        }


        if ($slugTotal < 268) {
            $tail = substr($head, $lastNl + 1);
            while ($slugTotal < 268 && !feof($fh)) {
                $chunk = $tail . fread($fh, 1048576);
                if ($chunk === '') break;
                
                $lastNl = strrpos($chunk, "\n");
                if ($lastNl === false) { $tail = $chunk; continue; }
                
                $cursor = 25;
                while ($cursor < $lastNl) {
                    $comma = strpos($chunk, ',', $cursor);
                    if ($comma === false || $comma >= $lastNl) break;
                    
                    $slug = substr($chunk, $cursor, $comma - $cursor);
                    if (!isset($slugSeen[$slug])) {
                        $slugs[$slugTotal] = $slug;
                        $slugSeen[$slug] = true;
                        $slugTotal++;
                        if ($slugTotal === 268) break 2;
                    }
                    $cursor = $comma + 52;
                }
                $tail = substr($chunk, $lastNl + 1);
            }
        }
        unset($head, $slugSeen);


        $baseUri = 'https://stitcher.io/blog/';
        $sufLen = 1;
        while (true) {
            $chk = [];
            $ok = true;
            foreach ($slugs as $s) {
                $tail = substr($baseUri . $s, -$sufLen);
                if (isset($chk[$tail])) { $sufLen++; $ok = false; break; }
                $chk[$tail] = true;
            }
            if ($ok) break;
        }


        $jumpTbl = [];
        $maxStride = 0;
        foreach ($slugs as $i => $s) {
            $stride = strlen($s) + 52;
            if ($stride > $maxStride) $maxStride = $stride;
            $jumpTbl[substr($baseUri . $s, -$sufLen)] = ($stride << 20) | ($i * $totalDates);
        }


        fseek($fh, 0, SEEK_END);
        $fSize = ftell($fh);
        fclose($fh);

        $grain = 8388608;
        $chunks = [];
        $offset = 0;
        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);

        while ($offset < $fSize) {
            $hi = $offset + $grain;
            if ($hi > $fSize) $hi = $fSize;
            
            $start = 0;
            if ($offset > 0) { fseek($fh, $offset); fgets($fh); $start = ftell($fh); }
            
            $end = $fSize;
            if ($hi < $fSize) { fseek($fh, $hi); fgets($fh); $end = ftell($fh); }
            
            $chunks[] = [$start, $end];
            $offset = $hi;
        }
        fclose($fh);
        $chunkTotal = count($chunks);


        $workers = 8;
        $memSz = $slugTotal * $totalDates;
        $sockets = [];

        $sOff = 26 + $sufLen;
        $fence = ($maxStride * 10) + $sOff;

        for ($w = 0; $w < $workers; $w++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_chunk_size($pair[0], $memSz << 1);
            stream_set_chunk_size($pair[1], $memSz << 1);
            
            if (pcntl_fork() === 0) {
                fclose($pair[0]);
                $bins = str_repeat("\0", $memSz);
                $fd = fopen($inputPath, 'rb');
                stream_set_read_buffer($fd, 0);


                if ($sufLen === 1) {
                    for ($c = $w; $c < $chunkTotal; $c += $workers) {
                        [$s, $e] = $chunks[$c]; fseek($fd, $s); $left = $e - $s;
                        while ($left > 0) {
                            $buf = fread($fd, $left > 131072 ? 131072 : $left); $bL = strlen($buf); $left -= $bL;
                            $nl = strrpos($buf, "\n"); if ($nl === false) break;
                            $tail = $bL - $nl - 1; if ($tail > 0) { fseek($fd, -$tail, SEEK_CUR); $left += $tail; }
                            
                            $idx = $nl;
                            while ($idx > $fence) {
                                $v=$jumpTbl[$buf[$idx-27]]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                                $v=$jumpTbl[$buf[$idx-27]]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                                $v=$jumpTbl[$buf[$idx-27]]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                                $v=$jumpTbl[$buf[$idx-27]]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                                $v=$jumpTbl[$buf[$idx-27]]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                                $v=$jumpTbl[$buf[$idx-27]]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                                $v=$jumpTbl[$buf[$idx-27]]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                                $v=$jumpTbl[$buf[$idx-27]]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                                $v=$jumpTbl[$buf[$idx-27]]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                                $v=$jumpTbl[$buf[$idx-27]]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                            }
                            while ($idx >= 27) {
                                $v=$jumpTbl[$buf[$idx-27]]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                            }
                        }
                    }
                } elseif ($sufLen === 2) {
                    for ($c = $w; $c < $chunkTotal; $c += $workers) {
                        [$s, $e] = $chunks[$c]; fseek($fd, $s); $left = $e - $s;
                        while ($left > 0) {
                            $buf = fread($fd, $left > 131072 ? 131072 : $left); $bL = strlen($buf); $left -= $bL;
                            $nl = strrpos($buf, "\n"); if ($nl === false) break;
                            $tail = $bL - $nl - 1; if ($tail > 0) { fseek($fd, -$tail, SEEK_CUR); $left += $tail; }
                            
                            $idx = $nl;
                            while ($idx > $fence) {

                                $v=$jumpTbl[$buf[$idx-28].$buf[$idx-27]]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                                $v=$jumpTbl[$buf[$idx-28].$buf[$idx-27]]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                                $v=$jumpTbl[$buf[$idx-28].$buf[$idx-27]]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                                $v=$jumpTbl[$buf[$idx-28].$buf[$idx-27]]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                                $v=$jumpTbl[$buf[$idx-28].$buf[$idx-27]]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                                $v=$jumpTbl[$buf[$idx-28].$buf[$idx-27]]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                                $v=$jumpTbl[$buf[$idx-28].$buf[$idx-27]]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                                $v=$jumpTbl[$buf[$idx-28].$buf[$idx-27]]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                                $v=$jumpTbl[$buf[$idx-28].$buf[$idx-27]]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                                $v=$jumpTbl[$buf[$idx-28].$buf[$idx-27]]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                            }
                            while ($idx >= 28) {
                                $v=$jumpTbl[$buf[$idx-28].$buf[$idx-27]]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                            }
                        }
                    }
                } else {
                    for ($c = $w; $c < $chunkTotal; $c += $workers) {
                        [$s, $e] = $chunks[$c]; fseek($fd, $s); $left = $e - $s;
                        while ($left > 0) {
                            $buf = fread($fd, $left > 131072 ? 131072 : $left); $bL = strlen($buf); $left -= $bL;
                            $nl = strrpos($buf, "\n"); if ($nl === false) break;
                            $tail = $bL - $nl - 1; if ($tail > 0) { fseek($fd, -$tail, SEEK_CUR); $left += $tail; }
                            
                            $idx = $nl;
                            while ($idx > $fence) {
                                $v=$jumpTbl[substr($buf,$idx-$sOff,$sufLen)]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                                $v=$jumpTbl[substr($buf,$idx-$sOff,$sufLen)]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                                $v=$jumpTbl[substr($buf,$idx-$sOff,$sufLen)]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                                $v=$jumpTbl[substr($buf,$idx-$sOff,$sufLen)]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                                $v=$jumpTbl[substr($buf,$idx-$sOff,$sufLen)]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                                $v=$jumpTbl[substr($buf,$idx-$sOff,$sufLen)]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                                $v=$jumpTbl[substr($buf,$idx-$sOff,$sufLen)]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                                $v=$jumpTbl[substr($buf,$idx-$sOff,$sufLen)]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                                $v=$jumpTbl[substr($buf,$idx-$sOff,$sufLen)]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                                $v=$jumpTbl[substr($buf,$idx-$sOff,$sufLen)]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                            }
                            while ($idx >= $sOff) {
                                $v=$jumpTbl[substr($buf,$idx-$sOff,$sufLen)]; $i=($v&1048575)+$dateTbl[substr($buf,$idx-22,7)]; $bins[$i]=$add1[$bins[$i]]; $idx-=$v>>20;
                            }
                        }
                    }
                }
                
                fclose($fd);
                fwrite($pair[1], chunk_split($bins, 1, "\0"));
                fclose($pair[1]);
                exit(0);
            }
            fclose($pair[1]);
            $sockets[$w] = $pair[0];
        }


        $results = array_fill(0, $workers, '');
        $wA = []; $eA = [];
        
        while (!empty($sockets)) {
            $rA = $sockets;
            stream_select($rA, $wA, $eA, null);
            foreach ($rA as $k => $sock) {
                $data = fread($sock, 8388608);
                if ($data !== '' && $data !== false) {
                    $results[$k] .= $data;
                }
                if (feof($sock)) {
                    fclose($sock);
                    unset($sockets[$k]);
                }
            }
        }


        $merged = $results[0];
        for ($i = 1; $i < $workers; $i++) {
            sodium_add($merged, $results[$i]);
        }
        $counts = unpack('v*', $merged);


        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1048576);
        fwrite($out, '{');

        $jsonUrls = [];
        for ($i = 0; $i < $slugTotal; $i++) {
            $jsonUrls[$i] = '"\/blog\/' . str_replace('/', '\/', $slugs[$i]) . '": {';
        }

        $sep = "\n    ";
        $cBase = 1;

        for ($u = 0; $u < $slugTotal; $u++) {
            $limit = $cBase + $totalDates;
            

            while ($cBase < $limit && $counts[$cBase] === 0) $cBase++;
            if ($cBase === $limit) continue;

            $dOff = $cBase - ($limit - $totalDates);
            $block = $sep . $jsonUrls[$u] . "\n" . $jsonDates[$dOff] . $counts[$cBase];
            $sep = ",\n    ";

            for ($cBase++; $cBase < $limit; $cBase++) {
                if ($counts[$cBase] !== 0) {
                    $block .= ",\n" . $jsonDates[$cBase - ($limit - $totalDates)] . $counts[$cBase];
                }
            }
            $block .= "\n    }";
            fwrite($out, $block);
        }
        
        fwrite($out, "\n}");
        fclose($out);
    }
}