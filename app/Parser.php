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

        $dayOffsets = [];
        $dayStrings = [];
        $totalDays = 0;
        
        for ($yr = 1; $yr <= 6; $yr++) {
            for ($mo = 1; $mo <= 12; $mo++) {
                $daysInMonth = match ($mo) {
                    2 => $yr === 4 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $moPad = ($mo < 10 ? '0' : '') . $mo;
                $prefix = "{$yr}-{$moPad}-";
                
                for ($dy = 1; $dy <= $daysInMonth; $dy++) {
                    $dyPad = ($dy < 10 ? '0' : '') . $dy;
                    $dayOffsets[$prefix . $dyPad] = $totalDays;
                    $dayStrings[$totalDays] = "202{$yr}-{$moPad}-{$dyPad}";
                    $totalDays++;
                }
            }
        }

        $charInc = [];
        for ($val = 0; $val < 255; $val++) {
            $charInc[chr($val)] = chr($val + 1);
        }

        $fd = fopen($inputPath, 'rb');
        stream_set_read_buffer($fd, 0);
        $headerData = fread($fd, 181_000);

        $urlPrefix = 'https://stitcher.io/blog/';
        $slugNames = [];
        $slugIndexMap = [];
        $uniqueSlugs = 0;
        $cursor = 0;
        $finalNl = strrpos($headerData, "\n") ?: 0;

        while ($cursor < $finalNl && $uniqueSlugs < 268) {
            $nextNl = strpos($headerData, "\n", $cursor + 52);
            if ($nextNl === false) break;
            
            $extractedSlug = substr($headerData, $cursor + 25, $nextNl - $cursor - 51);
            if (!isset($slugIndexMap[$extractedSlug])) {
                $slugNames[$uniqueSlugs] = $extractedSlug;
                $slugIndexMap[$extractedSlug] = $uniqueSlugs * $totalDays;
                $uniqueSlugs++;
            }
            $cursor = $nextNl + 1;
        }

        if ($uniqueSlugs < 268) {
            $remainder = substr($headerData, $finalNl + 1);
            while ($uniqueSlugs < 268 && !feof($fd)) {
                $block = $remainder . fread($fd, 1_048_576);
                if ($block === '') break;
                
                $finalNl = strrpos($block, "\n");
                if ($finalNl === false) {
                    $remainder = $block;
                    continue;
                }
                
                $cursor = 25;
                while ($cursor < $finalNl) {
                    $commaPos = strpos($block, ',', $cursor);
                    if ($commaPos === false || $commaPos >= $finalNl) break;
                    
                    $extractedSlug = substr($block, $cursor, $commaPos - $cursor);
                    if (!isset($slugIndexMap[$extractedSlug])) {
                        $slugNames[$uniqueSlugs] = $extractedSlug;
                        $slugIndexMap[$extractedSlug] = $uniqueSlugs * $totalDays;
                        $uniqueSlugs++;
                        if ($uniqueSlugs === 268) break 2;
                    }
                    $cursor = $commaPos + 52;
                }
                $remainder = substr($block, $finalNl + 1);
            }
        }
        unset($headerData);

        $suffixLen = 1;
        while (true) {
            $hashPool = [];
            $hasCollision = false;
            for ($i = 0; $i < $uniqueSlugs; $i++) {
                $tail = substr($urlPrefix . $slugNames[$i], -$suffixLen);
                if (isset($hashPool[$tail])) {
                    $suffixLen++;
                    $hasCollision = true;
                    break;
                }
                $hashPool[$tail] = true;
            }
            if (!$hasCollision) break;
        }

        $bitShift = 20;
        $bitMask = (1 << $bitShift) - 1;
        $maxStride = 0;
        $jumpDict = [];
        
        for ($i = 0; $i < $uniqueSlugs; $i++) {
            $stride = strlen($slugNames[$i]) + 52;
            if ($stride > $maxStride) {
                $maxStride = $stride;
            }
            $jumpDict[substr($urlPrefix . $slugNames[$i], -$suffixLen)] = ($stride << $bitShift) | ($i * $totalDays);
        }
        
        $tailOffset = 26 + $suffixLen;
        $timeOffset = 22;
        $timeLen = 7;
        $safetyFence = ($maxStride * 10) + $tailOffset;
        $memCapacity = $uniqueSlugs * $totalDays;

        fseek($fd, 0, SEEK_END);
        $totalBytes = ftell($fd);
        fclose($fd);

        $grainSize = 1 << 22;
        $fileBlocks = [];
        $fd = fopen($inputPath, 'rb');
        stream_set_read_buffer($fd, 0);
        $pos = 0;
        
        while ($pos < $totalBytes) {
            $nextPos = $pos + $grainSize;
            if ($nextPos > $totalBytes) {
                $nextPos = $totalBytes;
            }
            
            $startByte = 0;
            if ($pos > 0) {
                fseek($fd, $pos);
                fgets($fd);
                $startByte = ftell($fd);
            }
            
            $endByte = $totalBytes;
            if ($nextPos < $totalBytes) {
                fseek($fd, $nextPos);
                fgets($fd);
                $endByte = ftell($fd);
            }
            
            $fileBlocks[] = [$startByte, $endByte];
            $pos = $nextPos;
        }
        fclose($fd);
        $blockCount = count($fileBlocks);

        $cores = 8;
        $ipcSockets = [];

        for ($workerId = 0; $workerId < $cores; $workerId++) {
            $pipe = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_chunk_size($pipe[0], $memCapacity << 1);
            stream_set_chunk_size($pipe[1], $memCapacity << 1);
            
            if (pcntl_fork() === 0) {
                fclose($pipe[0]);
                $ramBuffer = str_repeat("\0", $memCapacity);
                $reader = fopen($inputPath, 'rb');
                stream_set_read_buffer($reader, 0);

                for ($b = $workerId; $b < $blockCount; $b += $cores) {
                    [$from, $to] = $fileBlocks[$b];
                    fseek($reader, $from);
                    $bytesLeft = $to - $from;

                    while ($bytesLeft > 0) {
                        $readSz = $bytesLeft > 131_072 ? 131_072 : $bytesLeft;
                        $chunk = fread($reader, $readSz);
                        $chunkSz = strlen($chunk);
                        $bytesLeft -= $chunkSz;

                        $lastBreak = strrpos($chunk, "\n");
                        if ($lastBreak === false) {
                            break;
                        }

                        $overflow = $chunkSz - $lastBreak - 1;
                        if ($overflow > 0) {
                            fseek($reader, -$overflow, SEEK_CUR);
                            $bytesLeft += $overflow;
                        }

                        $ptr = $lastBreak;

                        while ($ptr > $safetyFence) {
                            $packed = $jumpDict[substr($chunk, $ptr - $tailOffset, $suffixLen)];
                            $idx = ($packed & $bitMask) + $dayOffsets[substr($chunk, $ptr - $timeOffset, $timeLen)];
                            $ramBuffer[$idx] = $charInc[$ramBuffer[$idx]];
                            $ptr -= $packed >> $bitShift;

                            $packed = $jumpDict[substr($chunk, $ptr - $tailOffset, $suffixLen)];
                            $idx = ($packed & $bitMask) + $dayOffsets[substr($chunk, $ptr - $timeOffset, $timeLen)];
                            $ramBuffer[$idx] = $charInc[$ramBuffer[$idx]];
                            $ptr -= $packed >> $bitShift;

                            $packed = $jumpDict[substr($chunk, $ptr - $tailOffset, $suffixLen)];
                            $idx = ($packed & $bitMask) + $dayOffsets[substr($chunk, $ptr - $timeOffset, $timeLen)];
                            $ramBuffer[$idx] = $charInc[$ramBuffer[$idx]];
                            $ptr -= $packed >> $bitShift;

                            $packed = $jumpDict[substr($chunk, $ptr - $tailOffset, $suffixLen)];
                            $idx = ($packed & $bitMask) + $dayOffsets[substr($chunk, $ptr - $timeOffset, $timeLen)];
                            $ramBuffer[$idx] = $charInc[$ramBuffer[$idx]];
                            $ptr -= $packed >> $bitShift;

                            $packed = $jumpDict[substr($chunk, $ptr - $tailOffset, $suffixLen)];
                            $idx = ($packed & $bitMask) + $dayOffsets[substr($chunk, $ptr - $timeOffset, $timeLen)];
                            $ramBuffer[$idx] = $charInc[$ramBuffer[$idx]];
                            $ptr -= $packed >> $bitShift;

                            $packed = $jumpDict[substr($chunk, $ptr - $tailOffset, $suffixLen)];
                            $idx = ($packed & $bitMask) + $dayOffsets[substr($chunk, $ptr - $timeOffset, $timeLen)];
                            $ramBuffer[$idx] = $charInc[$ramBuffer[$idx]];
                            $ptr -= $packed >> $bitShift;

                            $packed = $jumpDict[substr($chunk, $ptr - $tailOffset, $suffixLen)];
                            $idx = ($packed & $bitMask) + $dayOffsets[substr($chunk, $ptr - $timeOffset, $timeLen)];
                            $ramBuffer[$idx] = $charInc[$ramBuffer[$idx]];
                            $ptr -= $packed >> $bitShift;

                            $packed = $jumpDict[substr($chunk, $ptr - $tailOffset, $suffixLen)];
                            $idx = ($packed & $bitMask) + $dayOffsets[substr($chunk, $ptr - $timeOffset, $timeLen)];
                            $ramBuffer[$idx] = $charInc[$ramBuffer[$idx]];
                            $ptr -= $packed >> $bitShift;

                            $packed = $jumpDict[substr($chunk, $ptr - $tailOffset, $suffixLen)];
                            $idx = ($packed & $bitMask) + $dayOffsets[substr($chunk, $ptr - $timeOffset, $timeLen)];
                            $ramBuffer[$idx] = $charInc[$ramBuffer[$idx]];
                            $ptr -= $packed >> $bitShift;

                            $packed = $jumpDict[substr($chunk, $ptr - $tailOffset, $suffixLen)];
                            $idx = ($packed & $bitMask) + $dayOffsets[substr($chunk, $ptr - $timeOffset, $timeLen)];
                            $ramBuffer[$idx] = $charInc[$ramBuffer[$idx]];
                            $ptr -= $packed >> $bitShift;
                        }

                        while ($ptr >= $tailOffset) {
                            $packed = $jumpDict[substr($chunk, $ptr - $tailOffset, $suffixLen)];
                            $idx = ($packed & $bitMask) + $dayOffsets[substr($chunk, $ptr - $timeOffset, $timeLen)];
                            $ramBuffer[$idx] = $charInc[$ramBuffer[$idx]];
                            $ptr -= $packed >> $bitShift;
                        }
                    }
                }

                fclose($reader);
                fwrite($pipe[1], chunk_split($ramBuffer, 1, "\0"));
                fclose($pipe[1]);
                exit(0);
            }
            fclose($pipe[1]);
            $ipcSockets[$workerId] = $pipe[0];
        }

        $collected = array_fill(0, $cores, '');
        $wReady = [];
        $eReady = [];
        
        while ($ipcSockets !== []) {
            $rReady = $ipcSockets;
            stream_select($rReady, $wReady, $eReady, 5);
            foreach ($rReady as $id => $sock) {
                $payload = fread($sock, $memCapacity << 1);
                if ($payload !== '' && $payload !== false) {
                    $collected[$id] .= $payload;
                }
                if (feof($sock)) {
                    fclose($sock);
                    unset($ipcSockets[$id]);
                }
            }
        }

        $masterGrid = $collected[0];
        for ($i = 1; $i < $cores; $i++) {
            sodium_add($masterGrid, $collected[$i]);
        }
        
        $finalStats = unpack('v*', $masterGrid);

        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);
        fwrite($out, '{');

        $fmtDays = [];
        for ($d = 0; $d < $totalDays; $d++) {
            $fmtDays[$d] = '        "' . $dayStrings[$d] . '": ';
        }

        $fmtSlugs = [];
        for ($s = 0; $s < $uniqueSlugs; $s++) {
            $fmtSlugs[$s] = '"\/blog\/' . $slugNames[$s] . '": {';
        }

        $delimiter = "\n    ";
        $cursor = 1;

        for ($s = 0; $s < $uniqueSlugs; $s++) {
            $firstMatch = -1;
            $ptr = $cursor;
            
            for ($d = 0; $d < $totalDays; $d++) {
                if ($finalStats[$ptr] !== 0) {
                    $firstMatch = $d;
                    break;
                }
                $ptr++;
            }

            if ($firstMatch === -1) {
                $cursor += $totalDays;
                continue;
            }

            $block = $delimiter . $fmtSlugs[$s] . "\n" . $fmtDays[$firstMatch] . $finalStats[$cursor + $firstMatch];
            $delimiter = ",\n    ";

            for ($d = $firstMatch + 1; $d < $totalDays; $d++) {
                $val = $finalStats[$cursor + $d];
                if ($val !== 0) {
                    $block .= ",\n" . $fmtDays[$d] . $val;
                }
            }

            $block .= "\n    }";
            fwrite($out, $block);
            $cursor += $totalDays;
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}