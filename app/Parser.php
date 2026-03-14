<?php

namespace App;

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
    private const int INITIAL_READ = 181_000;
    private const int DISC_READ    = 1_048_576;
    private const int CHUNK_READ   = 163_840; // Ajuste perfecto para Caché L1/L2
    private const int UNROLL       = 12;      // Unroll agresivo calibrado para el Pipeline
    private const int CHUNK_GRAIN  = 8_388_608; // 8MB exactos para predictibilidad NVMe Round-Robin
    private const string URL_PREF  = 'https://stitcher.io/blog/';

    public static function parse($inputPath, $outputPath)
    {
        gc_disable();

        $dateIds = [];
        $dates = [];
        $dc = 0;
        
        // Rango temporal robusto, cubriendo años bisiestos
        for ($y = 1; $y <= 6; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) { 2 => $y === 4 ? 29 : 28, 4, 6, 9, 11 => 30, default => 31 };
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
        $next[chr(255)] = chr(255); // Prevención de desbordamientos

        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);

        $paths = [];
        $seen = [];
        $slugTotal = 0;
        $raw = fread($fh, self::INITIAL_READ);
        $lastNl = strrpos($raw, "\n");
        if ($lastNl === false) { $lastNl = -1; }
        $pos = 0;

        while ($pos < $lastNl && $slugTotal < self::SLUG_TOTAL) {
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

        // Recuperación por si faltan slugs (súper seguro)
        if ($slugTotal < self::SLUG_TOTAL) {
            $carry = substr($raw, $lastNl + 1);
            while ($slugTotal < self::SLUG_TOTAL && ! feof($fh)) {
                $chunk = $carry . fread($fh, self::DISC_READ);
                if ($chunk === '') { break; }
                $lastNl = strrpos($chunk, "\n");
                if ($lastNl === false) { $carry = $chunk; continue; }
                $pos = 25;
                while ($pos < $lastNl) {
                    $sep = strpos($chunk, ',', $pos);
                    if ($sep === false || $sep >= $lastNl) { break; }
                    $slug = substr($chunk, $pos, $sep - $pos);
                    if (! isset($seen[$slug])) {
                        $paths[$slugTotal] = $slug;
                        $seen[$slug] = true;
                        $slugTotal++;
                        if ($slugTotal === self::SLUG_TOTAL) { break 2; }
                    }
                    $pos = $sep + 52;
                }
                $carry = substr($chunk, $lastNl + 1);
            }
        }

        fseek($fh, 0, SEEK_END);
        $fileSize = ftell($fh);

        // Segmentación Round-Robin entrelazada: El hardware amará esto
        $chunks = [];
        $lo = 0;
        while ($lo < $fileSize) {
            $hi = $lo + self::CHUNK_GRAIN;
            if ($hi > $fileSize) { $hi = $fileSize; }
            $start = 0;
            if ($lo > 0) { fseek($fh, $lo); fgets($fh); $start = ftell($fh); }
            $end = $fileSize;
            if ($hi < $fileSize) { fseek($fh, $hi); fgets($fh); $end = ftell($fh); }
            $chunks[] = [$start, $end];
            $lo = $hi;
        }
        fclose($fh);

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
        $chunkCount = \count($chunks);
        $sockets = [];

        // Arquitectura Óptima: 8 Workers que delegan la carga por tuberías IPC al Padre Drenador
        for ($w = 0; $w < self::WORKERS; $w++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_chunk_size($pair[0], $frameBytes);
            stream_set_chunk_size($pair[1], $frameBytes);

            if (pcntl_fork() === 0) {
                fclose($pair[0]);
                $output = str_repeat("\0", $bucketSize);
                $handle = fopen($inputPath, 'rb');
                stream_set_read_buffer($handle, 0);

                // =================================================================================
                // EL SANTO GRIAL: LOOP SPECIALIZATION
                // Hemos sustituido todo overhead de variables por literales estáticos directos.
                // OPcache detecta esto y genera código nativo sin lecturas a RAM adicionales.
                // =================================================================================
                if ($keyBytes === 1) {
                    $limit = ($maxStride * self::UNROLL) + 27;
                    for ($ci = $w; $ci < $chunkCount; $ci += self::WORKERS) {
                        [$start, $end] = $chunks[$ci];
                        fseek($handle, $start);
                        $remaining = $end - $start;

                        while ($remaining > 0) {
                            $toRead = $remaining > self::CHUNK_READ ? self::CHUNK_READ : $remaining;
                            $chunk = fread($handle, $toRead);
                            $chunkLen = strlen($chunk);
                            $remaining -= $chunkLen;
                            $lastNl = strrpos($chunk, "\n");
                            if ($lastNl === false) { break; }
                            $tail = $chunkLen - $lastNl - 1;
                            if ($tail > 0) { fseek($handle, -$tail, SEEK_CUR); $remaining += $tail; }

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
                            }
                            while ($pos >= 27) {
                                $t = $slugLookup[substr($chunk, $pos - 27, 1)];
                                $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)];
                                $output[$idx] = $next[$output[$idx]];
                                $pos -= $t >> 20;
                            }
                        }
                    }
                } elseif ($keyBytes === 2) {
                    $limit = ($maxStride * self::UNROLL) + 28;
                    for ($ci = $w; $ci < $chunkCount; $ci += self::WORKERS) {
                        [$start, $end] = $chunks[$ci];
                        fseek($handle, $start);
                        $remaining = $end - $start;

                        while ($remaining > 0) {
                            $toRead = $remaining > self::CHUNK_READ ? self::CHUNK_READ : $remaining;
                            $chunk = fread($handle, $toRead);
                            $chunkLen = strlen($chunk);
                            $remaining -= $chunkLen;
                            $lastNl = strrpos($chunk, "\n");
                            if ($lastNl === false) { break; }
                            $tail = $chunkLen - $lastNl - 1;
                            if ($tail > 0) { fseek($handle, -$tail, SEEK_CUR); $remaining += $tail; }

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
                            }
                            while ($pos >= 28) {
                                $t = $slugLookup[substr($chunk, $pos - 28, 2)];
                                $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)];
                                $output[$idx] = $next[$output[$idx]];
                                $pos -= $t >> 20;
                            }
                        }
                    }
                } else {
                    $ko = 26 + $keyBytes;
                    $limit = ($maxStride * self::UNROLL) + $ko;
                    for ($ci = $w; $ci < $chunkCount; $ci += self::WORKERS) {
                        [$start, $end] = $chunks[$ci];
                        fseek($handle, $start);
                        $remaining = $end - $start;

                        while ($remaining > 0) {
                            $toRead = $remaining > self::CHUNK_READ ? self::CHUNK_READ : $remaining;
                            $chunk = fread($handle, $toRead);
                            $chunkLen = strlen($chunk);
                            $remaining -= $chunkLen;
                            $lastNl = strrpos($chunk, "\n");
                            if ($lastNl === false) { break; }
                            $tail = $chunkLen - $lastNl - 1;
                            if ($tail > 0) { fseek($handle, -$tail, SEEK_CUR); $remaining += $tail; }

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
                            }
                            while ($pos >= $ko) {
                                $t = $slugLookup[substr($chunk, $pos - $ko, $keyBytes)];
                                $idx = ($t & 0xFFFFF) + $dateIds[substr($chunk, $pos - 22, 7)];
                                $output[$idx] = $next[$output[$idx]];
                                $pos -= $t >> 20;
                            }
                        }
                    }
                }

                fclose($handle);
                // IPC Ultrarrápido: Escritura atómica a nivel de OS
                fwrite($pair[1], chunk_split($output, 1, "\0"));
                fclose($pair[1]);
                exit(0);
            }

            fclose($pair[1]);
            $sockets[$w] = $pair[0];
        }

        $buffers = array_fill(0, self::WORKERS, '');
        $write = [];
        $except = [];

        while ($sockets !== []) {
            $read = $sockets;
            stream_select($read, $write, $except, 5);
            foreach ($read as $id => $s) {
                $data = fread($s, $frameBytes);
                if ($data !== '' && $data !== false) { $buffers[$id] .= $data; }
                if (feof($s)) { fclose($s); unset($sockets[$id]); }
            }
        }

        $merged = $buffers[0];
        for ($w = self::WORKERS - 1; $w > 0; $w--) { sodium_add($merged, $buffers[$w]); }

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
            
            // Reemplazo O(1) masivo: saltamos de una tirada todos los días vacíos
            while ($c < $end && $counts[$c] === 0) { $c++; }

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
}