<?php

namespace App;

use function array_fill;
use function array_slice;
use function fclose;
use function feof;
use function fgets;
use function filesize;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function pack;
use function pcntl_fork;
use function sodium_add;
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
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $workers = 8;
        $fileSize = filesize($inputPath);
        
        $dates = [];
        $datePrefixes = [];
        $dateCount = 0;
        
        for ($y = 21; $y <= 26; $y++) {
            $yStr = (string)$y;
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => ($y === 24) ? 29 : 28, 
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $mStr = ($m < 10 ? '0' : '') . $m;
                $fullY = "20{$yStr}-{$mStr}-";
                
                for ($d = 1; $d <= $maxD; $d++) {
                    $dd = ($d < 10 ? '0' : '') . $d;
                    $dates[$dateCount] = $fullY . $dd;
                    $datePrefixes[$dateCount] = '        "' . $fullY . $dd . '": ';
                    $dateCount++;
                }
            }
        }

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $raw = fread($handle, 4_194_304); 

        $paths = [];
        $slugTotal = 0;
        $pos = 0;
        $lastNl = strrpos($raw, "\n") ?: 0;
        $prefix = 'https://stitcher.io/blog/';
        
        while ($pos < $lastNl) {
            $nl = strpos($raw, "\n", $pos + 52);
            if ($nl === false) break;
            $slug = substr($raw, $pos + 25, $nl - $pos - 51);
            if (!isset($paths[$slug])) {
                $paths[$slug] = $slugTotal++;
            }
            $pos = $nl + 1;
        }
        unset($raw);
        
        $keyBytes = 1;
        while (true) {
            $keys = [];
            foreach ($paths as $slug => $id) {
                $key = substr($prefix . $slug, -$keyBytes);
                if (isset($keys[$key])) { $keyBytes++; continue 2; }
                $keys[$key] = true;
            }
            break;
        }

        $maxStride = 0;
        $combinedLookup = [];
        $escapedPaths = [];
        
        foreach ($paths as $slug => $id) {
            $stride = strlen($slug) + 52;
            if ($stride > $maxStride) { $maxStride = $stride; }
            $slugTail = substr($prefix . $slug, -$keyBytes);
            $baseIdx = $id * $dateCount;
            $tokenBase = $stride << 20;
            
            for ($d = 0; $d < $dateCount; $d++) {
                $combinedKey = $slugTail . ',' . $dates[$d];
                $combinedLookup[$combinedKey] = $tokenBase | ($baseIdx + $d);
            }
            $escapedPaths[$id] = '"\/blog\/' . str_replace('/', '\\/', $slug) . '": {';
        }

        $combinedOffset = 26 + $keyBytes;
        $combinedLen = $keyBytes + 11;
        $bucketSize = $slugTotal * $dateCount;
        $batchLimit = ($maxStride * 8) + $combinedOffset;

        $boundaries = [0];
        $chunkSize = (int)($fileSize / $workers);
        for ($i = 1; $i < $workers; $i++) {
            fseek($handle, $i * $chunkSize);
            fgets($handle);
            $boundaries[] = ftell($handle);
        }
        $boundaries[] = $fileSize;
        fclose($handle);

        $sockets = [];

        for ($w = 0; $w < $workers - 1; $w++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_chunk_size($pair[0], 8_388_608);
            stream_set_chunk_size($pair[1], 8_388_608);
            
            if (pcntl_fork() === 0) {
                fclose($pair[0]);
                self::workerProcess($inputPath, $boundaries[$w], $boundaries[$w+1], $bucketSize, $batchLimit, $combinedOffset, $combinedLen, $combinedLookup, $pair[1]);
                exit(0);
            }
            fclose($pair[1]);
            $sockets[$w] = $pair[0];
        }

        $counts = self::workerProcessLocal($inputPath, $boundaries[$workers-1], $boundaries[$workers], $bucketSize, $batchLimit, $combinedOffset, $combinedLen, $combinedLookup);
        $merged = '';
        for ($i = 0; $i < $bucketSize; $i += 32768) {
            $merged .= pack('V*', ...array_slice($counts, $i, 32768));
        }
        unset($counts);

        $write = [];
        $except = [];
        $buffers = array_fill(0, $workers - 1, '');
        
        while ($sockets !== []) {
            $read = $sockets;
            stream_select($read, $write, $except, null);
            foreach ($read as $socket) {
                $key = \array_search($socket, $sockets, true);
                $data = fread($socket, 8_388_608);
                if ($data !== '' && $data !== false) {
                    $buffers[$key] .= $data;
                }
                if (feof($socket)) {
                    fclose($socket);
                    unset($sockets[$key]);
                }
            }
        }

        for ($w = 0; $w < $workers - 1; $w++) {
            sodium_add($merged, $buffers[$w]);
        }
        
        $finalCounts = unpack('V*', $merged);
        unset($merged, $buffers);

        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);
        fwrite($out, '{');

        $sep = "\n    ";
        $base = 1;

        for ($p = 0; $p < $slugTotal; $p++) {
            $firstDate = -1;
            
            for ($d = 0; $d < $dateCount; $d++) {
                if ($finalCounts[$base + $d] !== 0) {
                    $firstDate = $d;
                    break;
                }
            }

            if ($firstDate === -1) {
                $base += $dateCount;
                continue;
            }

            $buf = $sep . $escapedPaths[$p] . "\n" . $datePrefixes[$firstDate] . $finalCounts[$base + $firstDate];
            $sep = ",\n    ";

            for ($d = $firstDate + 1; $d < $dateCount; $d++) {
                $count = $finalCounts[$base + $d];
                if ($count !== 0) {
                    $buf .= ",\n" . $datePrefixes[$d] . $count;
                }
            }

            $buf .= "\n    }";
            fwrite($out, $buf);
            $base += $dateCount;
        }

        fwrite($out, "\n}");
        fclose($out);
    }

    private static function workerProcessLocal(string $inputPath, int $start, int $end, int $bucketSize, int $batchLimit, int $combinedOffset, int $combinedLen, array $combinedLookup): array
    {
        $counts = array_fill(0, $bucketSize, 0);
        $reader = fopen($inputPath, 'rb');
        stream_set_read_buffer($reader, 0);
        fseek($reader, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $toRead = $remaining > 163_840 ? 163_840 : $remaining;
            $chunk = fread($reader, $toRead);
            $chunkLen = strlen($chunk);
            $remaining -= $chunkLen;
            $lastNl = strrpos($chunk, "\n");
            
            if ($lastNl === false) { break; }
            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) { fseek($reader, -$tail, SEEK_CUR); $remaining += $tail; }

            $pos = $lastNl;

            while ($pos > $batchLimit) {
                $token = $combinedLookup[substr($chunk, $pos - $combinedOffset, $combinedLen)];
                $counts[$token & 0xFFFFF]++;
                $pos -= $token >> 20;

                $token = $combinedLookup[substr($chunk, $pos - $combinedOffset, $combinedLen)];
                $counts[$token & 0xFFFFF]++;
                $pos -= $token >> 20;

                $token = $combinedLookup[substr($chunk, $pos - $combinedOffset, $combinedLen)];
                $counts[$token & 0xFFFFF]++;
                $pos -= $token >> 20;

                $token = $combinedLookup[substr($chunk, $pos - $combinedOffset, $combinedLen)];
                $counts[$token & 0xFFFFF]++;
                $pos -= $token >> 20;

                $token = $combinedLookup[substr($chunk, $pos - $combinedOffset, $combinedLen)];
                $counts[$token & 0xFFFFF]++;
                $pos -= $token >> 20;

                $token = $combinedLookup[substr($chunk, $pos - $combinedOffset, $combinedLen)];
                $counts[$token & 0xFFFFF]++;
                $pos -= $token >> 20;

                $token = $combinedLookup[substr($chunk, $pos - $combinedOffset, $combinedLen)];
                $counts[$token & 0xFFFFF]++;
                $pos -= $token >> 20;

                $token = $combinedLookup[substr($chunk, $pos - $combinedOffset, $combinedLen)];
                $counts[$token & 0xFFFFF]++;
                $pos -= $token >> 20;
            }

            while ($pos >= $combinedOffset) {
                $token = $combinedLookup[substr($chunk, $pos - $combinedOffset, $combinedLen)];
                $counts[$token & 0xFFFFF]++;
                $pos -= $token >> 20;
            }
        }
        
        fclose($reader);
        return $counts;
    }

    private static function workerProcess(string $inputPath, int $start, int $end, int $bucketSize, int $batchLimit, int $combinedOffset, int $combinedLen, array $combinedLookup, $socket): void
    {
        $counts = self::workerProcessLocal($inputPath, $start, $end, $bucketSize, $batchLimit, $combinedOffset, $combinedLen, $combinedLookup);
        $bin = '';
        for ($i = 0; $i < $bucketSize; $i += 32768) {
            $bin .= pack('V*', ...array_slice($counts, $i, 32768));
        }
        $written = 0;
        $len = strlen($bin);
        while ($written < $len) {
            $chunk = $written === 0 ? $bin : substr($bin, $written);
            $res = fwrite($socket, $chunk);
            if ($res === false) break;
            $written += $res;
        }
        fclose($socket);
    }
}