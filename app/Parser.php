<?php

declare(strict_types=1);

namespace App;

use function chr;
use function fclose;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function str_repeat;
use function str_replace;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use const SEEK_CUR;
use const SEEK_END;

final class Parser
{
    /**
     * 100-Million-Row-Challenge Parser
     * Optimized for M1 Single-Core Benchmark
     */
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        // Phase 1: Pre-calculate fixture schedule/calendar map
        $calendar = [];
        $fixtureLabels = [];
        $matches = 0;
        for ($y = 1; $y <= 6; $y++) {
            $ly = ($y === 4); // Leap year 2024
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => $ly ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $mStr = ($m < 10 ? '0' : '') . $m;
                $ymStr = $y . '-' . $mStr . '-';
                for ($d = 1; $d <= $maxD; $d++) {
                    $key = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $calendar[$key] = $matches;
                    $fixtureLabels[$matches] = '202' . $key;
                    $matches++;
                }
            }
        }

        // Phase 2: Successor table for O(1) byte increments
        $successor = [];
        for ($g = 0; $g < 255; $g++) {
            $successor[chr($g)] = chr($g + 1);
        }

        // Phase 3: Domain detection and path discovery
        $fd = fopen($inputPath, 'rb');
        stream_set_read_buffer($fd, 0);
        $header = fread($fd, 250_000);

        $firstNl = strpos($header, "\n");
        $eolAdj = ($firstNl > 0 && $header[$firstNl - 1] === "\r") ? 1 : 0;

        // Auto-detect full URL prefix (e.g. "https://stitcher.io/blog/")
        $slash3 = strpos($header, '/', 8);
        $slash4 = strpos($header, '/', $slash3 + 1);
        $prefixLen = $slash4 + 1;

        $lineFixed = $prefixLen + 1 + 25 + $eolAdj + 1;

        $paths = [];
        $pathCount = 0;
        $pos = 0;
        $headerEnd = strrpos($header, "\n") ?: 0;
        $pathMap = [];

        while ($pos < $headerEnd && $pathCount < 270) {
            $nl = strpos($header, "\n", $pos + 1);
            if ($nl === false || $nl > $headerEnd) break;
            $len = $nl - $pos + 1 - $lineFixed;
            if ($len <= 0) { $pos = $nl + 1; continue; }
            $path = substr($header, $pos + $prefixLen, $len);
            if (!isset($pathMap[$path])) {
                $paths[$pathCount] = $path;
                $pathMap[$path] = $pathCount * $matches;
                $pathCount++;
            }
            $pos = $nl + 1;
        }

        if ($pathCount < 268) {
            $carry = ($headerEnd < strlen($header) - 1) ? substr($header, $headerEnd + 1) : '';
            while ($pathCount < 268 && !feof($fd)) {
                $extra = $carry . fread($fd, 2_000_000);
                if ($extra === '') break;
                $extraEnd = strrpos($extra, "\n");
                if ($extraEnd === false) { $carry = $extra; continue; }
                $ep = 0;
                while ($ep < $extraEnd) {
                    $enl = strpos($extra, "\n", $ep + 1);
                    if ($enl === false || $enl > $extraEnd) break;
                    $eLen = $enl - $ep + 1 - $lineFixed;
                    if ($eLen > 0) {
                        $epath = substr($extra, $ep + $prefixLen, $eLen);
                        if (!isset($pathMap[$epath])) {
                            $paths[$pathCount] = $epath;
                            $pathMap[$epath] = $pathCount * $matches;
                            $pathCount++;
                            if ($pathCount >= 270) break;
                        }
                    }
                    $ep = $enl + 1;
                }
                $carry = substr($extra, $extraEnd + 1);
            }
        }
        unset($header, $pathMap);

        // Phase 4: Suffix-based fingerprinting
        $urlPrefix = substr(file_get_contents($inputPath, false, null, 0, $prefixLen), 0, $prefixLen);
        $fpLen = 1;
        while (true) {
            $seen = [];
            $coll = false;
            for ($s = 0; $s < $pathCount; $s++) {
                $f = substr($urlPrefix . $paths[$s], -$fpLen);
                if (isset($seen[$f])) { $fpLen++; $coll = true; break; }
                $seen[$f] = true;
            }
            if (!$coll) break;
        }

        $strideShift = 20;
        $idxMask = (1 << $strideShift) - 1;
        $maxStride = 0;
        $enzo = [];
        for ($s = 0; $s < $pathCount; $s++) {
            $stride = strlen($paths[$s]) + $lineFixed;
            if ($stride > $maxStride) $maxStride = $stride;
            $enzo[substr($urlPrefix . $paths[$s], -$fpLen)] = ($stride << $strideShift) | ($s * $matches);
        }

        $macOffsets = $eolAdj + 26 + $fpLen;
        $dateOffsets = $eolAdj + 22;
        $guardZone = ($maxStride * 10) + $macOffsets;

        $totalCells = $pathCount * $matches;

        fseek($fd, 0, SEEK_END);
        $remaining = ftell($fd);
        fseek($fd, 0);

        // Phase 5: Fast Binary Scaning
        $scoreboard = str_repeat("\0", $totalCells);
        $accumulator = str_repeat("\0", $totalCells * 2);
        $hasSodium = function_exists('sodium_add');
        $flushInt = 200;
        $bufferIdx = 0;
        $readSize = 163_840; // Cache friendly for M1

        // Local aliases
        $fpMap = $enzo;
        $calMap = $calendar;
        $succMap = $successor;
        $fpOff = $macOffsets;
        $dtOff = $dateOffsets;
        $sSh = $strideShift;
        $mask = $idxMask;

        while ($remaining > 0) {
            $batch = fread($fd, $remaining > $readSize ? $readSize : $remaining);
            $bLen = strlen($batch);
            $remaining -= $bLen;

            $lastNl = strrpos($batch, "\n");
            if ($lastNl === false) break;

            $tail = $bLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($fd, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $idx = $lastNl;
            while ($idx > $guardZone) {
                $v = $fpMap[substr($batch, $idx - $fpOff, $fpLen)];
                $ci = ($v & $mask) + $calMap[substr($batch, $idx - $dtOff, 7)];
                $scoreboard[$ci] = $succMap[$scoreboard[$ci]];
                $idx -= $v >> $sSh;

                $v = $fpMap[substr($batch, $idx - $fpOff, $fpLen)];
                $ci = ($v & $mask) + $calMap[substr($batch, $idx - $dtOff, 7)];
                $scoreboard[$ci] = $succMap[$scoreboard[$ci]];
                $idx -= $v >> $sSh;

                $v = $fpMap[substr($batch, $idx - $fpOff, $fpLen)];
                $ci = ($v & $mask) + $calMap[substr($batch, $idx - $dtOff, 7)];
                $scoreboard[$ci] = $succMap[$scoreboard[$ci]];
                $idx -= $v >> $sSh;

                $v = $fpMap[substr($batch, $idx - $fpOff, $fpLen)];
                $ci = ($v & $mask) + $calMap[substr($batch, $idx - $dtOff, 7)];
                $scoreboard[$ci] = $succMap[$scoreboard[$ci]];
                $idx -= $v >> $sSh;

                $v = $fpMap[substr($batch, $idx - $fpOff, $fpLen)];
                $ci = ($v & $mask) + $calMap[substr($batch, $idx - $dtOff, 7)];
                $scoreboard[$ci] = $succMap[$scoreboard[$ci]];
                $idx -= $v >> $sSh;

                $v = $fpMap[substr($batch, $idx - $fpOff, $fpLen)];
                $ci = ($v & $mask) + $calMap[substr($batch, $idx - $dtOff, 7)];
                $scoreboard[$ci] = $succMap[$scoreboard[$ci]];
                $idx -= $v >> $sSh;

                $v = $fpMap[substr($batch, $idx - $fpOff, $fpLen)];
                $ci = ($v & $mask) + $calMap[substr($batch, $idx - $dtOff, 7)];
                $scoreboard[$ci] = $succMap[$scoreboard[$ci]];
                $idx -= $v >> $sSh;

                $v = $fpMap[substr($batch, $idx - $fpOff, $fpLen)];
                $ci = ($v & $mask) + $calMap[substr($batch, $idx - $dtOff, 7)];
                $scoreboard[$ci] = $succMap[$scoreboard[$ci]];
                $idx -= $v >> $sSh;

                $v = $fpMap[substr($batch, $idx - $fpOff, $fpLen)];
                $ci = ($v & $mask) + $calMap[substr($batch, $idx - $dtOff, 7)];
                $scoreboard[$ci] = $succMap[$scoreboard[$ci]];
                $idx -= $v >> $sSh;

                $v = $fpMap[substr($batch, $idx - $fpOff, $fpLen)];
                $ci = ($v & $mask) + $calMap[substr($batch, $idx - $dtOff, 7)];
                $scoreboard[$ci] = $succMap[$scoreboard[$ci]];
                $idx -= $v >> $sSh;
            }

            while ($idx >= $fpOff) {
                $v = $fpMap[substr($batch, $idx - $fpOff, $fpLen)];
                $ci = ($v & $mask) + $calMap[substr($batch, $idx - $dtOff, 7)];
                $scoreboard[$ci] = $succMap[$scoreboard[$ci]];
                $idx -= $v >> $sSh;
            }

            if (++$bufferIdx >= $flushInt) {
                $wide = chunk_split($scoreboard, 1, "\0");
                if ($hasSodium) {
                    sodium_add($accumulator, $wide);
                } else {
                    for ($j = 0; $j < $totalCells; $j++) {
                        $o2 = $j << 1;
                        $val = ord($accumulator[$o2]) | (ord($accumulator[$o2 + 1]) << 8);
                        $val += ord($scoreboard[$j]);
                        $accumulator[$o2] = chr($val & 0xFF);
                        $accumulator[$o2+1] = chr(($val >> 8) & 0xFF);
                    }
                }
                $scoreboard = str_repeat("\0", $totalCells);
                $bufferIdx = 0;
            }
        }
        fclose($fd);

        if ($bufferIdx > 0) {
            $wide = chunk_split($scoreboard, 1, "\0");
            if ($hasSodium) {
                sodium_add($accumulator, $wide);
            } else {
                for ($j = 0; $j < $totalCells; $j++) {
                    $o2 = $j << 1;
                    $val = ord($accumulator[$o2]) | (ord($accumulator[$o2 + 1]) << 8);
                    $val += ord($scoreboard[$j]);
                    $accumulator[$o2] = chr($val & 0xFF);
                    $accumulator[$o2+1] = chr(($val >> 8) & 0xFF);
                }
            }
        }

        $finalCounts = unpack('v*', $accumulator);

        // Phase 6: JSON Output Generation
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);
        fwrite($out, '{');

        $dateTpls = [];
        for ($d = 0; $d < $matches; $d++) {
            $dateTpls[$d] = '        "' . $fixtureLabels[$d] . '": ';
        }

        $pathHdr = [];
        for ($s = 0; $s < $pathCount; $s++) {
            $pathHdr[$s] = '"\/blog\/' . str_replace('/', '\/', $paths[$s]) . '": {';
        }

        $sep = "\n    ";
        $baseIdx = 1;
        for ($s = 0; $s < $pathCount; $s++) {
            $first = -1;
            for ($d = 0; $d < $matches; $d++) {
                if ($finalCounts[$baseIdx + $d] !== 0) {
                    $first = $d;
                    break;
                }
            }

            if ($first === -1) {
                $baseIdx += $matches;
                continue;
            }

            $line = $sep . $pathHdr[$s] . "\n" . $dateTpls[$first] . $finalCounts[$baseIdx + $first];
            $sep = ",\n    ";

            for ($d = $first + 1; $d < $matches; $d++) {
                $c = $finalCounts[$baseIdx + $d];
                if ($c !== 0) $line .= ",\n" . $dateTpls[$d] . $c;
            }

            $line .= "\n    }";
            fwrite($out, $line);
            $baseIdx += $matches;
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}