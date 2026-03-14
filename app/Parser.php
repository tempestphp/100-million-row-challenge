<?php

namespace App;

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
    private const int WORKERS      = 8;
    private const int SLUG_TOTAL   = 268;
    private const int CHUNK_READ   = 262_144; 
    private const string URL_PREF  = 'https://stitcher.io/blog/';

    public static function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $dateIds = [];
        $dates = [];
        $dc = 0;
        
        for ($y = 0; $y <= 9; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) { 2 => ($y === 0 || $y === 4 || $y === 8) ? 29 : 28, 4, 6, 9, 11 => 30, default => 31 };
                $mStr = ($m < 10 ? '0' : '') . $m;
                $ymStr = "{$y}-{$mStr}-";
                for ($d = 1; $d <= $maxD; $d++) {
                    $dStr = ($d < 10 ? '0' : '') . $d;
                    $dateIds[$ymStr . $dStr] = $dc;
                    $dates[$dc] = '202' . $y . '-' . $mStr . '-' . $dStr;
                    $dc++;
                }
            }
        }

        $next = [];
        for ($i = 0; $i < 255; $i++) { $next[chr($i)] = chr($i + 1); }
        $next[chr(255)] = chr(255); 

        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);

        $paths = [];
        $seen = [];
        $slugTotal = 0;
        
        $raw = fread($fh, 4_194_304); 
        $lastNl = strrpos($raw, "\n") ?: -1;
        $pos = 0;

        while ($pos < $lastNl) {
            $nl = strpos($raw, "\n", $pos + 52);
            if ($nl === false || $nl > $lastNl) { break; }
            $slug = substr($raw, $pos + 25, $nl - $pos - 51);
            if (! isset($seen[$slug])) {
                $paths[$slugTotal] = $slug;
                $seen[$slug] = true;
                $slugTotal++;
            }
            $pos = $nl + 1;
        }

        fseek($fh, 0, SEEK_END);
        $fileSize = ftell($fh);

        $boundaries = [0];
        $chunkSize = (int)($fileSize / self::WORKERS);
        for ($i = 1; $i < self::WORKERS; $i++) {
            fseek($fh, $i * $chunkSize);
            fgets($fh);
            $boundaries[] = ftell($fh);
        }
        $boundaries[] = $fileSize;

        $keyBytes = 1;
        while (true) {
            $keys = [];
            foreach ($paths as $slug) {
                $key = substr(self::URL_PREF . $slug, -$keyBytes);
                if (isset($keys[$key])) { $keyBytes++; continue 2; }
                $keys[$key] = true;
            }
            break;
        }

        $maxStride = 0;
        $slugLookup = [];
        foreach ($paths as $id => $slug) {
            $stride = strlen($slug) + 52;
            if ($stride > $maxStride) { $maxStride = $stride; }
            $slugLookup[substr(self::URL_PREF . $slug, -$keyBytes)] = ($stride << 20) | ($id * $dc);
        }

        $bucketSize = $slugTotal * $dc;
        $frameBytes = $bucketSize << 1;
        $sockets = [];
        
        for ($w = 0; $w < self::WORKERS - 1; $w++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_chunk_size($pair[0], $frameBytes);
            stream_set_chunk_size($pair[1], $frameBytes);

            if (pcntl_fork() === 0) {
                fclose($pair[0]);
                $out = self::processWorker($inputPath, $boundaries[$w], $boundaries[$w+1], $slugLookup, $dateIds, $next, $keyBytes, $maxStride, $bucketSize);
                
                $outStr = chunk_split($out, 1, "\0");
                $written = 0;
                $len = strlen($outStr);
                while ($written < $len) {
                    $res = fwrite($pair[1], $written === 0 ? $outStr : substr($outStr, $written));
                    if ($res === false) break;
                    $written += $res;
                }
                fclose($pair[1]);
                exit(0);
            }

            fclose($pair[1]);
            $sockets[$w] = $pair[0];
        }

        $parentOutput = self::processWorker($inputPath, $boundaries[self::WORKERS - 1], $boundaries[self::WORKERS], $slugLookup, $dateIds, $next, $keyBytes, $maxStride, $bucketSize);
        fclose($fh);
        $merged = chunk_split($parentOutput, 1, "\0");

        $buffers = array_fill(0, self::WORKERS - 1, '');
        $write = [];
        $except = [];

        while ($sockets !== []) {
            $read = $sockets;
            stream_select($read, $write, $except, null);
            foreach ($read as $id => $s) {
                $data = fread($s, $frameBytes);
                if ($data !== '' && $data !== false) { 
                    $buffers[$id] .= $data; 
                }
                if (feof($s)) { 
                    fclose($s); 
                    unset($sockets[$id]); 
                }
            }
        }

        for ($w = 0; $w < self::WORKERS - 1; $w++) { 
            sodium_add($merged, $buffers[$w]); 
        }

        $counts = unpack('v*', $merged);
        
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);
        fwrite($out, '{');

        $datePrefixes = [];
        for ($d = $dc - 1; $d >= 0; $d--) {
            $datePrefixes[$d] = '        "' . $dates[$d] . '": ';
        }

        $pathHeaders = [];
        for ($p = $slugTotal - 1; $p >= 0; $p--) {
            $pathHeaders[$p] = '"\/blog\/' . str_replace('/', '\/', $paths[$p]) . '": {';
        }

        $sep = "\n    ";
        $base = 1;

        for ($p = 0; $p < $slugTotal; $p++) {
            $c = $base;
            $end = $base + $dc;
            
            while ($c < $end && $counts[$c] === 0) {
                $c++;
            }

            if ($c === $end) { 
                $base += $dc; 
                continue; 
            }

            $firstDay = $c - $base;
            $json = $sep . $pathHeaders[$p] . "\n" . $datePrefixes[$firstDay] . $counts[$c];
            $sep = ",\n    ";

            for ($c++; $c < $end; $c++) {
                if ($counts[$c] !== 0) {
                    $json .= ",\n" . $datePrefixes[$c - $base] . $counts[$c];
                }
            }

            $json .= "\n    }";
            fwrite($out, $json);
            $base += $dc;
        }

        fwrite($out, "\n}");
        fclose($out);
    }

    private static function processWorker(string $inputPath, int $start, int $end, array $slugLookup, array $dateIds, array $next, int $keyBytes, int $maxStride, int $bucketSize): string
    {
        $output = str_repeat("\0", $bucketSize);
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);
        $rem = $end - $start;

        if ($keyBytes === 1) {
            $limit = ($maxStride * 16) + 27;
            while ($rem > 0) {
                $toRead = $rem > self::CHUNK_READ ? self::CHUNK_READ : $rem;
                $chunk = fread($handle, $toRead); $chunkLen = strlen($chunk); $rem -= $chunkLen;
                $lastNl = strrpos($chunk, "\n"); if ($lastNl === false) break;
                $tail = $chunkLen - $lastNl - 1; if ($tail > 0) { fseek($handle, -$tail, SEEK_CUR); $rem += $tail; }
                $pos = $lastNl;
                while ($pos > $limit) {
                    $t = $slugLookup[substr($chunk, $pos - 27, 1)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 27, 1)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 27, 1)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 27, 1)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 27, 1)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 27, 1)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 27, 1)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 27, 1)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 27, 1)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 27, 1)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 27, 1)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 27, 1)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 27, 1)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 27, 1)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 27, 1)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 27, 1)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                }
                while ($pos >= 27) { $t = $slugLookup[substr($chunk, $pos - 27, 1)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20; }
            }
        } elseif ($keyBytes === 2) {
            $limit = ($maxStride * 16) + 28;
            while ($rem > 0) {
                $toRead = $rem > self::CHUNK_READ ? self::CHUNK_READ : $rem;
                $chunk = fread($handle, $toRead); $chunkLen = strlen($chunk); $rem -= $chunkLen;
                $lastNl = strrpos($chunk, "\n"); if ($lastNl === false) break;
                $tail = $chunkLen - $lastNl - 1; if ($tail > 0) { fseek($handle, -$tail, SEEK_CUR); $rem += $tail; }
                $pos = $lastNl;
                while ($pos > $limit) {
                    $t = $slugLookup[substr($chunk, $pos - 28, 2)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 28, 2)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 28, 2)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 28, 2)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 28, 2)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 28, 2)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 28, 2)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 28, 2)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 28, 2)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 28, 2)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 28, 2)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 28, 2)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 28, 2)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 28, 2)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 28, 2)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 28, 2)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                }
                while ($pos >= 28) { $t = $slugLookup[substr($chunk, $pos - 28, 2)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20; }
            }
        } elseif ($keyBytes === 3) {
            $limit = ($maxStride * 16) + 29;
            while ($rem > 0) {
                $toRead = $rem > self::CHUNK_READ ? self::CHUNK_READ : $rem;
                $chunk = fread($handle, $toRead); $chunkLen = strlen($chunk); $rem -= $chunkLen;
                $lastNl = strrpos($chunk, "\n"); if ($lastNl === false) break;
                $tail = $chunkLen - $lastNl - 1; if ($tail > 0) { fseek($handle, -$tail, SEEK_CUR); $rem += $tail; }
                $pos = $lastNl;
                while ($pos > $limit) {
                    $t = $slugLookup[substr($chunk, $pos - 29, 3)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 29, 3)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 29, 3)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 29, 3)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 29, 3)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 29, 3)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 29, 3)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 29, 3)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 29, 3)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 29, 3)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 29, 3)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 29, 3)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 29, 3)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 29, 3)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 29, 3)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 29, 3)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                }
                while ($pos >= 29) { $t = $slugLookup[substr($chunk, $pos - 29, 3)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20; }
            }
        } elseif ($keyBytes === 4) {
            $limit = ($maxStride * 16) + 30;
            while ($rem > 0) {
                $toRead = $rem > self::CHUNK_READ ? self::CHUNK_READ : $rem;
                $chunk = fread($handle, $toRead); $chunkLen = strlen($chunk); $rem -= $chunkLen;
                $lastNl = strrpos($chunk, "\n"); if ($lastNl === false) break;
                $tail = $chunkLen - $lastNl - 1; if ($tail > 0) { fseek($handle, -$tail, SEEK_CUR); $rem += $tail; }
                $pos = $lastNl;
                while ($pos > $limit) {
                    $t = $slugLookup[substr($chunk, $pos - 30, 4)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 30, 4)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 30, 4)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 30, 4)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 30, 4)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 30, 4)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 30, 4)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 30, 4)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 30, 4)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 30, 4)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 30, 4)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 30, 4)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 30, 4)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 30, 4)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 30, 4)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - 30, 4)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                }
                while ($pos >= 30) { $t = $slugLookup[substr($chunk, $pos - 30, 4)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20; }
            }
        } else {
            // Fallback Genérico para cualquier tamaño superior
            $ko = 26 + $keyBytes;
            $limit = ($maxStride * 16) + $ko;
            while ($rem > 0) {
                $toRead = $rem > self::CHUNK_READ ? self::CHUNK_READ : $rem;
                $chunk = fread($handle, $toRead); $chunkLen = strlen($chunk); $rem -= $chunkLen;
                $lastNl = strrpos($chunk, "\n"); if ($lastNl === false) break;
                $tail = $chunkLen - $lastNl - 1; if ($tail > 0) { fseek($handle, -$tail, SEEK_CUR); $rem += $tail; }
                $pos = $lastNl;
                while ($pos > $limit) {
                    $t = $slugLookup[substr($chunk, $pos - $ko, $keyBytes)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - $ko, $keyBytes)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - $ko, $keyBytes)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - $ko, $keyBytes)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - $ko, $keyBytes)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - $ko, $keyBytes)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - $ko, $keyBytes)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - $ko, $keyBytes)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - $ko, $keyBytes)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - $ko, $keyBytes)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - $ko, $keyBytes)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - $ko, $keyBytes)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - $ko, $keyBytes)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - $ko, $keyBytes)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - $ko, $keyBytes)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                    $t = $slugLookup[substr($chunk, $pos - $ko, $keyBytes)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20;
                }
                while ($pos >= $ko) { $t = $slugLookup[substr($chunk, $pos - $ko, $keyBytes)]; $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)]; $output[$idx] = $next[$output[$idx]]; $pos -= $t >> 20; }
            }
        }

        fclose($handle);
        return $output;
    }
}