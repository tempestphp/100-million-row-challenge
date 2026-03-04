<?php

namespace App;

\gc_disable();

use function chr;
use function count;
use function fclose;
use function feof;
use function fgets;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function implode;
use function ini_set;
use function pcntl_fork;
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
use const STREAM_IPPROTO_IP;
use const STREAM_PF_UNIX;
use const STREAM_SOCK_STREAM;

final class Parser
{
    public function parse($inputPath, $outputPath)
    {
        ini_set('memory_limit', '-1');

        $fileSize = filesize($inputPath);

        $dateIds = [];
        $dates = [];
        $dateCount = 0;
        for ($y = 21; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => $y === 24 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $mStr = $m < 10 ? "0{$m}" : (string) $m;
                $ymStr = "{$y}-{$mStr}-";
                for ($d = 1; $d <= $maxD; $d++) {
                    $key = $ymStr . ($d < 10 ? "0{$d}" : (string) $d);
                    $dateIds[$key] = $dateCount;
                    $dates[$dateCount] = $key;
                    $dateCount++;
                }
            }
        }

        $next = [];
        for ($i = 0; $i < 255; $i++) {
            $next[chr($i)] = chr($i + 1);
        }

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $sample = fread($handle, 2_097_152);
        fclose($handle);

        $pathIds = [];
        $paths = [];
        $pathCount = 0;

        $lastNl = strrpos($sample, "\n");
        if ($lastNl !== false) {
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
        }
        unset($sample);

        $slugBaseMap = [];
        foreach ($pathIds as $slug => $id) {
            $slugBaseMap[$slug] = $id * $dateCount;
        }

        $outputSize = $pathCount * $dateCount;
        $numWorkers = 8;

        $splits = [0];
        $handle = fopen($inputPath, 'rb');
        for ($w = 1; $w < $numWorkers; $w++) {
            fseek($handle, (int) ($fileSize * $w / $numWorkers));
            fgets($handle);
            $splits[] = ftell($handle);
        }
        $splits[] = $fileSize;
        fclose($handle);

        $sockets = [];
        for ($w = 0; $w < $numWorkers - 1; $w++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_chunk_size($pair[0], $outputSize);
            stream_set_chunk_size($pair[1], $outputSize);
            $pid = pcntl_fork();
            if ($pid === 0) {
                fclose($pair[0]);
                $output = str_repeat(chr(0), $outputSize);
                $fh = fopen($inputPath, 'rb');
                stream_set_read_buffer($fh, 0);
                self::parseRange($fh, $splits[$w], $splits[$w + 1], $slugBaseMap, $dateIds, $next, $output);
                fclose($fh);
                fwrite($pair[1], $output);
                fclose($pair[1]);
                exit(0);
            }
            fclose($pair[1]);
            $sockets[$w] = $pair[0];
        }

        $output = str_repeat(chr(0), $outputSize);
        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);
        self::parseRange($fh, $splits[$numWorkers - 1], $splits[$numWorkers], $slugBaseMap, $dateIds, $next, $output);
        fclose($fh);

        $counts = unpack('C*', $output);
        unset($output);

        while ($sockets !== []) {
            $read = $sockets;
            $write = [];
            $except = [];
            stream_select($read, $write, $except, 5);
            foreach ($read as $socket) {
                $key = \array_search($socket, $sockets, true);
                $data = '';
                while (!feof($socket)) {
                    $data .= fread($socket, $outputSize);
                }
                fclose($socket);
                unset($sockets[$key]);
                foreach (unpack('C*', $data) as $k => $v) {
                    $counts[$k] += $v;
                }
            }
        }

        self::writeJson($outputPath, $counts, $paths, $pathCount, $dates, $dateCount);
    }

    private static function parseRange($handle, $start, $end, $slugBaseMap, $dateIds, $next, &$output)
    {
        fseek($handle, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $chunk = fread($handle, $remaining > 163_840 ? 163_840 : $remaining);
            $chunkLen = strlen($chunk);
            if ($chunkLen === 0) break;
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) break;

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $p = 25;
            $fence = $lastNl - 792;

            while ($p < $fence) {
                $sep = strpos($chunk, ',', $p); $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)]; $output[$idx] = $next[$output[$idx]]; $p = $sep + 52;
                $sep = strpos($chunk, ',', $p); $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)]; $output[$idx] = $next[$output[$idx]]; $p = $sep + 52;
                $sep = strpos($chunk, ',', $p); $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)]; $output[$idx] = $next[$output[$idx]]; $p = $sep + 52;
                $sep = strpos($chunk, ',', $p); $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)]; $output[$idx] = $next[$output[$idx]]; $p = $sep + 52;
                $sep = strpos($chunk, ',', $p); $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)]; $output[$idx] = $next[$output[$idx]]; $p = $sep + 52;
                $sep = strpos($chunk, ',', $p); $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)]; $output[$idx] = $next[$output[$idx]]; $p = $sep + 52;
                $sep = strpos($chunk, ',', $p); $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)]; $output[$idx] = $next[$output[$idx]]; $p = $sep + 52;
                $sep = strpos($chunk, ',', $p); $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)]; $output[$idx] = $next[$output[$idx]]; $p = $sep + 52;
            }

            while ($p < $lastNl) {
                $sep = strpos($chunk, ',', $p);
                if ($sep === false || $sep >= $lastNl) break;
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
            }
        }
    }

    private static function writeJson($outputPath, $counts, $paths, $pathCount, $dates, $dateCount)
    {
        $datePrefixes = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "20' . $dates[$d] . '": ';
        }

        $escapedPaths = [];
        for ($p = 0; $p < $pathCount; $p++) {
            $escapedPaths[$p] = '"\\/blog\\/' . str_replace('/', '\\/', $paths[$p]) . '"';
        }

        $fp = fopen($outputPath, 'wb');
        stream_set_write_buffer($fp, 1_048_576);
        fwrite($fp, '{');
        $firstPath = true;
        $base = 1;

        for ($p = 0; $p < $pathCount; $p++) {
            $dateEntries = [];

            for ($d = 0; $d < $dateCount; $d++) {
                $n = $counts[$base + $d];
                if ($n === 0) continue;
                $dateEntries[] = $datePrefixes[$d] . $n;
            }

            if ($dateEntries === []) {
                $base += $dateCount;
                continue;
            }

            $buf = $firstPath ? "\n    " : ",\n    ";
            $firstPath = false;
            $buf .= $escapedPaths[$p] . ": {\n" . implode(",\n", $dateEntries) . "\n    }";
            fwrite($fp, $buf);
            $base += $dateCount;
        }

        fwrite($fp, "\n}");
        fclose($fp);
    }
}
