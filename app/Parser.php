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

        $dateMap = [];
        $dateList = [];
        $dateCount = 0;

        $y = 1;
        while ($y <= 6) {
            $m = 1;
            while ($m <= 12) {
                $maxD = match ($m) {
                    2 => $y === 4 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };

                $mStr = ($m < 10 ? '0' : '') . $m;
                $ymStr = "{$y}-{$mStr}-";

                $d = 1;
                while ($d <= $maxD) {
                    $key = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $dateMap[$key] = $dateCount;
                    $dateList[$dateCount] = '202' . $key;
                    $dateCount++;
                    $d++;
                }
                $m++;
            }
            $y++;
        }

        $incChar = [];
        $i = 0;
        while ($i < 255) {
            $incChar[chr($i)] = chr($i + 1);
            $i++;
        }

        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);
        $raw = fread($fh, 181_000);

        $prefix = 'https://stitcher.io/blog/';
        $paths = [];
        $slugMap = [];
        $slugCount = 0;
        $cursor = 0;
        $lastNl = strrpos($raw, "\n") ?: 0;

        while ($cursor < $lastNl && $slugCount < 268) {
            $nl = strpos($raw, "\n", $cursor + 52);
            if ($nl === false) break;

            $slug = substr($raw, $cursor + 25, $nl - $cursor - 51);
            if (!isset($slugMap[$slug])) {
                $paths[$slugCount] = $slug;
                $slugMap[$slug] = $slugCount * $dateCount;
                $slugCount++;
            }
            $cursor = $nl + 1;
        }

        $tailLen = 22;
        $shift = 20;
        $mask = (1 << $shift) - 1;
        $maxStride = 100;
        $slugMap = [];

        $p = 0;
        while ($p < $slugCount) {
            $stride = strlen($paths[$p]) + 52;
            $slugMap[substr($prefix . $paths[$p], -$tailLen)] = ($stride << $shift) | ($p * $dateCount);
            $p++;
        }

        $tailOff = 26 + $tailLen;
        $dateOff = 22;
        $fence = ($maxStride * 10) + $tailOff;
        $outputSize = $slugCount * $dateCount;

        fseek($fh, 0, SEEK_END);
        $fileSize = ftell($fh);

        $grain = 1 << 24;
        $segments = [];
        $lo = 0;

        while ($lo < $fileSize) {
            $hi = $lo + $grain;
            if ($hi > $fileSize) $hi = $fileSize;
            $from = 0;
            if ($lo > 0) {
                fseek($fh, $lo);
                fgets($fh);
                $from = ftell($fh);
            }
            $to = $fileSize;
            if ($hi < $fileSize) {
                fseek($fh, $hi);
                fgets($fh);
                $to = ftell($fh);
            }
            $segments[] = [$from, $to];
            $lo = $hi;
        }
        fclose($fh);
        $segCount = count($segments);

        $workers = 9;
        $sockets = [];
        $w = 0;

        while ($w < $workers) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_chunk_size($pair[0], $outputSize * 2);
            stream_set_chunk_size($pair[1], $outputSize * 2);

            if (pcntl_fork() === 0) {
                $output = str_repeat("\0", $outputSize);
                $reader = fopen($inputPath, 'rb');
                stream_set_read_buffer($reader, 0);

                $ci = $w;
                while ($ci < $segCount) {
                    [$from, $to] = $segments[$ci];
                    fseek($reader, $from);
                    $remaining = $to - $from;

                    while ($remaining > 0) {
                        $chunk = fread($reader, $remaining > 131_072 ? 131_072 : $remaining);
                        $chunkLen = strlen($chunk);
                        $remaining -= $chunkLen;

                        $lastNl = strrpos($chunk, "\n");
                        if ($lastNl === false) break;

                        $tail = $chunkLen - $lastNl - 1;
                        if ($tail > 0) {
                            fseek($reader, -$tail, SEEK_CUR);
                            $remaining += $tail;
                        }

                        $ptr = $lastNl;

                        while ($ptr > $fence) {
                            $packed = $slugMap[substr($chunk, $ptr - $tailOff, $tailLen)];
                            $idx = ($packed & $mask) + $dateMap[substr($chunk, $ptr - $dateOff, 7)];
                            $output[$idx] = $incChar[$output[$idx]];
                            $ptr -= $packed >> $shift;

                            $packed = $slugMap[substr($chunk, $ptr - $tailOff, $tailLen)];
                            $idx = ($packed & $mask) + $dateMap[substr($chunk, $ptr - $dateOff, 7)];
                            $output[$idx] = $incChar[$output[$idx]];
                            $ptr -= $packed >> $shift;

                            $packed = $slugMap[substr($chunk, $ptr - $tailOff, $tailLen)];
                            $idx = ($packed & $mask) + $dateMap[substr($chunk, $ptr - $dateOff, 7)];
                            $output[$idx] = $incChar[$output[$idx]];
                            $ptr -= $packed >> $shift;

                            $packed = $slugMap[substr($chunk, $ptr - $tailOff, $tailLen)];
                            $idx = ($packed & $mask) + $dateMap[substr($chunk, $ptr - $dateOff, 7)];
                            $output[$idx] = $incChar[$output[$idx]];
                            $ptr -= $packed >> $shift;

                            $packed = $slugMap[substr($chunk, $ptr - $tailOff, $tailLen)];
                            $idx = ($packed & $mask) + $dateMap[substr($chunk, $ptr - $dateOff, 7)];
                            $output[$idx] = $incChar[$output[$idx]];
                            $ptr -= $packed >> $shift;

                            $packed = $slugMap[substr($chunk, $ptr - $tailOff, $tailLen)];
                            $idx = ($packed & $mask) + $dateMap[substr($chunk, $ptr - $dateOff, 7)];
                            $output[$idx] = $incChar[$output[$idx]];
                            $ptr -= $packed >> $shift;

                            $packed = $slugMap[substr($chunk, $ptr - $tailOff, $tailLen)];
                            $idx = ($packed & $mask) + $dateMap[substr($chunk, $ptr - $dateOff, 7)];
                            $output[$idx] = $incChar[$output[$idx]];
                            $ptr -= $packed >> $shift;

                            $packed = $slugMap[substr($chunk, $ptr - $tailOff, $tailLen)];
                            $idx = ($packed & $mask) + $dateMap[substr($chunk, $ptr - $dateOff, 7)];
                            $output[$idx] = $incChar[$output[$idx]];
                            $ptr -= $packed >> $shift;

                            $packed = $slugMap[substr($chunk, $ptr - $tailOff, $tailLen)];
                            $idx = ($packed & $mask) + $dateMap[substr($chunk, $ptr - $dateOff, 7)];
                            $output[$idx] = $incChar[$output[$idx]];
                            $ptr -= $packed >> $shift;

                            $packed = $slugMap[substr($chunk, $ptr - $tailOff, $tailLen)];
                            $idx = ($packed & $mask) + $dateMap[substr($chunk, $ptr - $dateOff, 7)];
                            $output[$idx] = $incChar[$output[$idx]];
                            $ptr -= $packed >> $shift;
                        }

                        while ($ptr >= $tailOff) {
                            $packed = $slugMap[substr($chunk, $ptr - $tailOff, $tailLen)];
                            $idx = ($packed & $mask) + $dateMap[substr($chunk, $ptr - $dateOff, 7)];
                            $output[$idx] = $incChar[$output[$idx]];
                            $ptr -= $packed >> $shift;
                        }
                    }
                    $ci += $workers;
                }

                fclose($reader);
                fwrite($pair[1], chunk_split($output, 1, "\0"));
                exit(0);
            }
            fclose($pair[1]);
            $sockets[$w] = $pair[0];
            $w++;
        }

        $buffers = array_fill(0, $workers, '');
        $wr = [];
        $ex = [];

        while ($sockets !== []) {
            $rd = $sockets;
            stream_select($rd, $wr, $ex, 5);
            foreach ($rd as $k => $soc) {
                $data = fread($soc, $outputSize * 2);
                if ($data !== '' && $data !== false) {
                    $buffers[$k] .= $data;
                }
                if (feof($soc)) {
                    fclose($soc);
                    unset($sockets[$k]);
                }
            }
        }

        $merged = $buffers[0];
        $wi = $workers - 1;
        while ($wi > 0) {
            sodium_add($merged, $buffers[$wi]);
            $wi--;
        }
        $counts = unpack('v*', $merged);

        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 2_097_152);
        fwrite($out, '{');

        $datePrefixes = [];
        $d = 0;
        while ($d < $dateCount) {
            $datePrefixes[$d] = "        \"{$dateList[$d]}\": ";
            $d++;
        }

        $escapedPaths = [];
        $p = 0;
        while ($p < $slugCount) {
            $escapedPaths[$p] = '"\/blog\/' . $paths[$p] . '": {';
            $p++;
        }

        $sep = "\n    ";
        $base = 1;

        $p = 0;
        while ($p < $slugCount) {
            $firstDate = -1;
            $idx = $base;

            $d = 0;
            while ($d < $dateCount) {
                if ($counts[$idx] !== 0) { $firstDate = $d; break; }
                $idx++;
                $d++;
            }

            if ($firstDate === -1) {
                $base += $dateCount;
                $p++;
                continue;
            }

            $block = $sep . $escapedPaths[$p] . "\n" . $datePrefixes[$firstDate] . $counts[$idx];
            $sep = ",\n    ";

            $d = $firstDate + 1;
            while ($d < $dateCount) {
                $idx++;
                $count = $counts[$idx];
                if ($count !== 0) {
                    $block .= ",\n" . $datePrefixes[$d] . $count;
                }
                $d++;
            }

            $block .= "\n    }";
            fwrite($out, $block);
            $base += $dateCount;
            $p++;
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
