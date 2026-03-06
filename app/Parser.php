<?php

namespace App;

use function strpos;
use function strrpos;
use function substr;
use function strlen;
use function fopen;
use function fclose;
use function fread;
use function fseek;
use function ftell;
use function fgets;
use function fwrite;
use function filesize;
use function gc_disable;
use function pcntl_fork;
use function pack;
use function unpack;
use function chr;
use function array_fill;
use function array_count_values;
use function count;
use function str_replace;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function stream_set_chunk_size;
use function stream_socket_pair;
use function stream_select;
use App\Commands\Visit;
use const SEEK_CUR;
use const STREAM_PF_UNIX;
use const STREAM_SOCK_STREAM;
use const STREAM_IPPROTO_IP;

final class Parser
{
    private const WORKERS = 10;
    private const READ_CHUNK = 163_840;

    public static function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $fileSize = filesize($inputPath);

        // Pre-enumerate all possible dates 2021-2026
        $dateIdChars = [];
        $dates = [];
        $dateCount = 0;
        for ($y = 21; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => $y === 24 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $mStr = ($m < 10 ? '0' : '') . $m;
                $ymStr = "{$y}-{$mStr}-";
                for ($d = 1; $d <= $maxD; $d++) {
                    $key = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $dateIdChars[$key] = chr($dateCount & 0xFF) . chr($dateCount >> 8);
                    $dates[$dateCount] = $key;
                    $dateCount++;
                }
            }
        }

        // Discover slugs: small sample for ordering, Visit::all() for completeness
        $pathIds = [];
        $paths = [];
        $pathCount = 0;

        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);
        $sample = fread($fh, 131_072);
        fclose($fh);

        $lastNl = strrpos($sample, "\n");
        $p = 25;
        while ($p < $lastNl) {
            $sep = strpos($sample, ',', $p);
            if ($sep === false) break;
            $slug = substr($sample, $p, $sep - $p);
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug] = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }
            $p = $sep + 52;
        }
        unset($sample);

        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, 25);
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug] = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }
        }

        $cellTotal = $pathCount * $dateCount;
        $packedSize = $cellTotal * 2;

        // Split file into WORKERS chunks (fixed assignment)
        $boundaries = [0];
        $fh = fopen($inputPath, 'rb');
        for ($i = 1; $i < self::WORKERS; $i++) {
            fseek($fh, (int)($fileSize * $i / self::WORKERS));
            fgets($fh);
            $boundaries[] = ftell($fh);
        }
        fclose($fh);
        $boundaries[] = $fileSize;

        // Fork 9 children, parent takes last chunk
        $sockets = [];

        for ($w = 0; $w < self::WORKERS - 1; $w++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_chunk_size($pair[0], $packedSize);
            stream_set_chunk_size($pair[1], $packedSize);

            $pid = pcntl_fork();
            if ($pid === 0) {
                // Child
                fclose($pair[0]);
                \set_error_handler(fn() => true); \proc_nice(-20); \restore_error_handler();

                $buckets = array_fill(0, $pathCount, '');
                $fh = fopen($inputPath, 'rb');
                stream_set_read_buffer($fh, 0);
                self::parseRange($fh, $boundaries[$w], $boundaries[$w + 1], $pathIds, $dateIdChars, $buckets);
                fclose($fh);

                // Convert buckets to flat counts
                $counts = array_fill(0, $cellTotal, 0);
                $base = 0;
                foreach ($buckets as $bucket) {
                    if ($bucket !== '') {
                        foreach (array_count_values(unpack('v*', $bucket)) as $did => $n) {
                            $counts[$base + $did] += $n;
                        }
                    }
                    $base += $dateCount;
                }

                fwrite($pair[1], pack('v*', ...$counts));
                fclose($pair[1]);
                exit(0);
            }

            // Parent
            fclose($pair[1]);
            $sockets[$w] = $pair[0];
        }

        // Parent parses last chunk
        \set_error_handler(fn() => true); \proc_nice(-20); \restore_error_handler();

        $buckets = array_fill(0, $pathCount, '');
        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);
        $lastW = self::WORKERS - 1;
        self::parseRange($fh, $boundaries[$lastW], $boundaries[$lastW + 1], $pathIds, $dateIdChars, $buckets);
        fclose($fh);

        // Convert parent's buckets to counts
        $counts = array_fill(0, $cellTotal, 0);
        $base = 0;
        foreach ($buckets as $bucket) {
            if ($bucket !== '') {
                foreach (array_count_values(unpack('v*', $bucket)) as $did => $n) {
                    $counts[$base + $did] += $n;
                }
            }
            $base += $dateCount;
        }
        unset($buckets);

        // Collect children via stream_select
        $remaining = count($sockets);
        while ($remaining > 0) {
            $read = array_values($sockets);
            $w = null;
            $e = null;
            stream_select($read, $w, $e, 10);

            foreach ($read as $sock) {
                $data = '';
                while (strlen($data) < $packedSize) {
                    $chunk = fread($sock, $packedSize - strlen($data));
                    if ($chunk === false || $chunk === '') break;
                    $data .= $chunk;
                }

                if (strlen($data) === $packedSize) {
                    $childCounts = unpack('v*', $data);
                    foreach ($childCounts as $k => $v) {
                        if ($v) {
                            $counts[$k - 1] += $v;
                        }
                    }
                }

                fclose($sock);
                $key = array_search($sock, $sockets, true);
                if ($key !== false) {
                    unset($sockets[$key]);
                }
                $remaining--;
            }
        }

        // Reap children
        while (\pcntl_wait($status) > 0) {}

        self::writeJson($outputPath, $counts, $paths, $dates, $dateCount);
    }

    private static function parseRange($fh, $start, $end, $pathIds, $dateIdChars, &$buckets): void
    {
        fseek($fh, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $chunk = fread($fh, $remaining > self::READ_CHUNK ? self::READ_CHUNK : $remaining);
            $cLen = strlen($chunk);
            $remaining -= $cLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) break;

            $tail = $cLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($fh, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $p = 25;
            $fence = $lastNl - 1010;

            while ($p < $fence) {
                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;
            }

            while ($p < $lastNl) {
                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;
            }
        }
    }

    private static function writeJson(
        $outputPath, $counts,
        $paths, $dates, $dateCount,
    ) {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);
        fwrite($out, '{');

        $datePrefixes = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "20' . $dates[$d] . '": ';
        }

        $pathCount = count($paths);
        $escapedPaths = [];
        for ($p = 0; $p < $pathCount; $p++) {
            $escapedPaths[$p] = '"\\/blog\\/' . str_replace('/', '\\/', $paths[$p]) . '"';
        }

        $first = true;

        for ($p = 0; $p < $pathCount; $p++) {
            $base = $p * $dateCount;
            $body = '';
            $sep = '';

            for ($d = 0; $d < $dateCount; $d++) {
                $n = $counts[$base + $d];
                if ($n === 0) continue;
                $body .= $sep . $datePrefixes[$d] . $n;
                $sep = ",\n";
            }

            if ($body === '') continue;

            fwrite($out, ($first ? "\n    " : ",\n    ") . $escapedPaths[$p] . ": {\n" . $body . "\n    }");
            $first = false;
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
