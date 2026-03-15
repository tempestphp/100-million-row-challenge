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
use function intdiv;
use function sodium_add;
use function str_repeat;
use function stream_select;
use function stream_set_chunk_size;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function stream_socket_pair;
use function implode;
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
    private const CHUNK_BYTES = 1_048_576;
    private const NUM_WORKERS = 16;

    public static function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $dateKeyToIndex = [];
        $numDates = 0;
        for ($y = 1; $y <= 6; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => $y === 4 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $mStr = ($m < 10 ? '0' : '') . $m;
                $ymStr = "{$y}-{$mStr}-";
                for ($d = 1; $d <= $maxD; $d++) {
                    $key = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $dateKeyToIndex[$key] = $numDates;
                    $jsonDateLabels[$numDates] = '        "202' . $key . '": ';
                    $numDates++;
                }
            }
        }

        $byteInc = [];
        for ($i = 0; $i < 255; $i++) {
            $byteInc[chr($i)] = chr($i + 1);
        }

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $header = fread($handle, 181_000);

        $uriPrefix = 'https://stitcher.io/blog/';
        $pathSlugs = [];
        $seen = [];
        $numPaths = 0;
        $offset = 0;
        $headerEnd = strrpos($header, "\n") ?: 0;

        while ($offset < $headerEnd && $numPaths < 268) {
            $lineEnd = strpos($header, "\n", $offset + 52);
            if ($lineEnd === false) break;
            $slug = substr($header, $offset + 25, $lineEnd - $offset - 51);
            if (!isset($seen[$slug])) {
                $pathSlugs[$numPaths] = $slug;
                $seen[$slug] = $numPaths * $numDates;
                $numPaths++;
            }
            $offset = $lineEnd + 1;
        }
        unset($header);

        $suffixLen = 1;
        while (true) {
            $seen = [];
            for ($i = 0; $i < $numPaths; $i++) {
                $suffix = substr($uriPrefix . $pathSlugs[$i], -$suffixLen);
                if (isset($seen[$suffix])) {
                    $suffixLen++;
                    continue 2;
                }
                $seen[$suffix] = true;
            }
            break;
        }

        $strideBits = 20;
        $baseMask = (1 << $strideBits) - 1;
        $maxLineLen = 0;
        $uriSuffixToPacked = [];
        for ($i = 0; $i < $numPaths; $i++) {
            $lineLen = strlen($pathSlugs[$i]) + 52;
            if ($lineLen > $maxLineLen) $maxLineLen = $lineLen;
            $uriSuffixToPacked[substr($uriPrefix . $pathSlugs[$i], -$suffixLen)] = ($lineLen << $strideBits) | ($i * $numDates);
        }
        $suffixStartFromNewline = 26 + $suffixLen;
        $dateStartFromNewline = 22;
        $dateKeyLen = 7;
        $unrollMinPos = ($maxLineLen * 16) + $suffixStartFromNewline;

        $cellCount = $numPaths * $numDates;

        fseek($handle, 0, SEEK_END);
        $fileSize = ftell($handle);

        $numWorkers = self::NUM_WORKERS;
        $ranges = [];
        for ($wi = 0; $wi < $numWorkers; $wi++) {
            $from = intdiv($fileSize * $wi, $numWorkers);
            $to   = intdiv($fileSize * ($wi + 1), $numWorkers);
            if ($from > 0) {
                fseek($handle, $from);
                fgets($handle);
                $from = ftell($handle);
            }
            if ($wi < $numWorkers - 1) {
                fseek($handle, $to);
                fgets($handle);
                $to = ftell($handle);
            } else {
                $to = $fileSize;
            }
            $ranges[$wi] = [$from, $to];
        }

        $childSockets = [];

        for ($wi = 0; $wi < $numWorkers - 1; $wi++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_chunk_size($pair[0], $cellCount << 1);
            stream_set_chunk_size($pair[1], $cellCount << 1);
            if (pcntl_fork() === 0) {
                fclose($pair[0]);
                $cells = str_repeat("\0", $cellCount);
                $reader = fopen($inputPath, 'rb');
                stream_set_read_buffer($reader, 0);

                [$from, $to] = $ranges[$wi];
                fseek($reader, $from);
                $remaining = $to - $from;

                while ($remaining > 0) {
                    $chunk = fread($reader, $remaining > self::CHUNK_BYTES ? self::CHUNK_BYTES : $remaining);
                    $chunkLen = strlen($chunk);
                    $remaining -= $chunkLen;

                    $lastNl = strrpos($chunk, "\n");
                    if ($lastNl === false) break;

                    $tail = $chunkLen - $lastNl - 1;
                    if ($tail > 0) {
                        fseek($reader, -$tail, SEEK_CUR);
                        $remaining += $tail;
                    }

                    $p = $lastNl;

                    while ($p > $unrollMinPos) {
                        $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $cells[$idx] = $byteInc[$cells[$idx]];
                        $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $cells[$idx] = $byteInc[$cells[$idx]];
                        $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $cells[$idx] = $byteInc[$cells[$idx]];
                        $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $cells[$idx] = $byteInc[$cells[$idx]];
                        $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $cells[$idx] = $byteInc[$cells[$idx]];
                        $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $cells[$idx] = $byteInc[$cells[$idx]];
                        $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $cells[$idx] = $byteInc[$cells[$idx]];
                        $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $cells[$idx] = $byteInc[$cells[$idx]];
                        $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $cells[$idx] = $byteInc[$cells[$idx]];
                        $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $cells[$idx] = $byteInc[$cells[$idx]];
                        $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $cells[$idx] = $byteInc[$cells[$idx]];
                        $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $cells[$idx] = $byteInc[$cells[$idx]];
                        $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $cells[$idx] = $byteInc[$cells[$idx]];
                        $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $cells[$idx] = $byteInc[$cells[$idx]];
                        $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $cells[$idx] = $byteInc[$cells[$idx]];
                        $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $cells[$idx] = $byteInc[$cells[$idx]];
                        $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $cells[$idx] = $byteInc[$cells[$idx]];
                        $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $cells[$idx] = $byteInc[$cells[$idx]];
                    }

                    while ($p >= $suffixStartFromNewline) {
                        $s = substr($chunk, $p - $suffixStartFromNewline, $suffixLen);
                        $dk = substr($chunk, $p - $dateStartFromNewline, $dateKeyLen);
                        if (!isset($uriSuffixToPacked[$s], $dateKeyToIndex[$dk])) { $p -= $maxLineLen; continue; }
                        $packed = $uriSuffixToPacked[$s];
                        $idx = ($packed & $baseMask) + $dateKeyToIndex[$dk];
                        $p -= $packed >> $strideBits;
                        $cells[$idx] = $byteInc[$cells[$idx]];
                    }
                }

                fclose($reader);
                fwrite($pair[1], chunk_split($cells, 1, "\0"));
                fclose($pair[1]);
                exit(0);
            }
            fclose($pair[1]);
            $childSockets[$wi] = $pair[0];
        }

        $parentCells = str_repeat("\0", $cellCount);
        stream_set_read_buffer($handle, 0);

        [$from, $to] = $ranges[$numWorkers - 1];
        fseek($handle, $from);
        $remaining = $to - $from;

        while ($remaining > 0) {
            $chunk = fread($handle, $remaining > self::CHUNK_BYTES ? self::CHUNK_BYTES : $remaining);
            $chunkLen = strlen($chunk);
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) break;

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $p = $lastNl;

            while ($p > $unrollMinPos) {
                $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $parentCells[$idx] = $byteInc[$parentCells[$idx]];
                $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $parentCells[$idx] = $byteInc[$parentCells[$idx]];
                $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $parentCells[$idx] = $byteInc[$parentCells[$idx]];
                $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $parentCells[$idx] = $byteInc[$parentCells[$idx]];
                $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $parentCells[$idx] = $byteInc[$parentCells[$idx]];
                $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $parentCells[$idx] = $byteInc[$parentCells[$idx]];
                $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $parentCells[$idx] = $byteInc[$parentCells[$idx]];
                $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $parentCells[$idx] = $byteInc[$parentCells[$idx]];
                $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $parentCells[$idx] = $byteInc[$parentCells[$idx]];
                $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $parentCells[$idx] = $byteInc[$parentCells[$idx]];
                $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $parentCells[$idx] = $byteInc[$parentCells[$idx]];
                $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $parentCells[$idx] = $byteInc[$parentCells[$idx]];
                $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $parentCells[$idx] = $byteInc[$parentCells[$idx]];
                $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $parentCells[$idx] = $byteInc[$parentCells[$idx]];
                $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $parentCells[$idx] = $byteInc[$parentCells[$idx]];
                $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $parentCells[$idx] = $byteInc[$parentCells[$idx]];
                $packed = $uriSuffixToPacked[substr($chunk, $p - $suffixStartFromNewline, $suffixLen)]; $idx = ($packed & $baseMask) + $dateKeyToIndex[substr($chunk, $p - $dateStartFromNewline, $dateKeyLen)]; $p -= $packed >> $strideBits; $parentCells[$idx] = $byteInc[$parentCells[$idx]];
            }

            while ($p >= $suffixStartFromNewline) {
                $s = substr($chunk, $p - $suffixStartFromNewline, $suffixLen);
                $dk = substr($chunk, $p - $dateStartFromNewline, $dateKeyLen);
                if (!isset($uriSuffixToPacked[$s], $dateKeyToIndex[$dk])) { $p -= $maxLineLen; continue; }
                $packed = $uriSuffixToPacked[$s];
                $idx = ($packed & $baseMask) + $dateKeyToIndex[$dk];
                $p -= $packed >> $strideBits;
                $parentCells[$idx] = $byteInc[$parentCells[$idx]];
            }
        }

        fclose($handle);
        $merged = chunk_split($parentCells, 1, "\0");

        $payloadSize = $cellCount << 1;
        $childPayloads = array_fill(0, $numWorkers - 1, '');
        while ($childSockets !== []) {
            $read = $childSockets;
            $write = [];
            $except = [];
            stream_select($read, $write, $except, null);
            foreach ($read as $key => $socket) {
                $data = fread($socket, $payloadSize);
                if ($data !== '' && $data !== false) {
                    $childPayloads[$key] .= $data;
                }
                if (feof($socket)) {
                    fclose($socket);
                    unset($childSockets[$key]);
                }
            }
        }

        for ($wi = 0; $wi < $numWorkers - 1; $wi++) {
            sodium_add($merged, $childPayloads[$wi]);
        }
        $counts = unpack('v*', $merged);

        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 4_194_304);

        $escapedPaths = [];
        for ($p = 0; $p < $numPaths; $p++) {
            $escapedPaths[$p] = '"\/blog\/' . $pathSlugs[$p] . '": {';
        }

        $parts = ['{'];
        $sep = "\n    ";
        $base = 1;

        for ($p = 0; $p < $numPaths; $p++) {
            $limit = $base + $numDates;

            while ($base < $limit && $counts[$base] === 0) {
                $base++;
            }

            if ($base === $limit) continue;

            $dOff = $base - ($limit - $numDates);
            $buf = $sep . $escapedPaths[$p] . "\n" . $jsonDateLabels[$dOff] . $counts[$base];
            $sep = ",\n    ";

            for ($base++; $base < $limit; $base++) {
                $count = $counts[$base];
                if ($count === 0) continue;
                $buf .= ",\n" . $jsonDateLabels[$base - ($limit - $numDates)] . $count;
            }

            $buf .= "\n    }";
            $parts[] = $buf;
        }

        fwrite($out, implode('', $parts) . "\n}");
        fclose($out);
    }
}

