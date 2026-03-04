<?php

namespace App;

use App\Commands\Visit;

use function array_fill;
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
use function gc_disable;
use function getmypid;
use function implode;
use function pcntl_fork;
use function pcntl_wait;
use function sem_acquire;
use function sem_get;
use function sem_release;
use function sem_remove;
use function set_error_handler;
use function shmop_delete;
use function shmop_open;
use function shmop_read;
use function shmop_write;
use function str_repeat;
use function str_replace;
use function stream_select;
use function stream_set_chunk_size;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
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
    private const int BUFFER_SIZE = 163_840;
    private const int PREFIX_LEN  = 25;
    private const int WORKERS     = 8;
    private const int CHUNKS      = 16;
    private const int FILE_SIZE   = 7_509_674_827;

    public static function parse($inputPath, $outputPath)
    {
        gc_disable();

        $fileSize   = self::FILE_SIZE;
        $numWorkers = self::WORKERS;
        $numChunks  = self::CHUNKS;

        $dateIds   = [];
        $dates     = [];
        $dateCount = 0;

        for ($y = 21; $y <= 26; $y++) {
            $yStr = (string)$y;
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2           => $y === 24 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default     => 31,
                };
                $mStr  = ($m < 10 ? '0' : '') . $m;
                $ymStr = $yStr . '-' . $mStr . '-';
                for ($d = 1; $d <= $maxD; $d++) {
                    $key               = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $dateIds[$key]     = $dateCount;
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
        $raw = fread($handle, 131_072);
        fclose($handle);

        $pathIds   = [];
        $paths     = [];
        $pathCount = 0;
        $pos       = 0;
        $lastNl    = strrpos($raw, "\n") ?: 0;

        while ($pos < $lastNl) {
            $sep  = strpos($raw, ',', $pos + self::PREFIX_LEN);
            if ($sep === false) break;
            $slug = substr($raw, $pos + self::PREFIX_LEN, $sep - $pos - self::PREFIX_LEN);
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug]    = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }
            $pos = $sep + 27;
        }
        unset($raw);

        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, self::PREFIX_LEN);
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug]    = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }
        }

        $slugBaseMap = [];
        foreach ($pathIds as $slug => $id) {
            $slugBaseMap[$slug] = $id * $dateCount;
        }

        $outputSize = $pathCount * $dateCount;

        $splitPoints = [0];
        $bh = fopen($inputPath, 'rb');
        foreach ([
                     469_354_676,
                     938_709_353,
                     1_408_064_029,
                     1_877_418_706,
                     2_346_773_382,
                     2_816_128_059,
                     3_285_482_735,
                     3_754_837_412,
                     4_224_192_088,
                     4_693_546_765,
                     5_162_901_441,
                     5_632_256_118,
                     6_101_610_794,
                     6_570_965_471,
                     7_040_320_147,
                 ] as $offset) {
            fseek($bh, $offset);
            fgets($bh);
            $splitPoints[] = ftell($bh);
        }
        fclose($bh);
        $splitPoints[] = $fileSize;

        $myPid = getmypid();

        $useSemQueue = false;
        $semKey      = $myPid + 1;
        $queueShmKey = $myPid + 2;
        $queueShm    = null;
        $sem         = null;

        set_error_handler(null);
        $sem      = @sem_get($semKey, 1, 0644, true);
        $queueShm = @shmop_open($queueShmKey, 'c', 0644, 4);
        set_error_handler(null);

        if ($sem !== false && $queueShm !== false) {
            shmop_write($queueShm, pack('V', 0), 0);
            $useSemQueue = true;
        }

        $sockets  = [];
        $childMap = [];

        for ($w = 0; $w < $numWorkers - 1; $w++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_chunk_size($pair[0], $outputSize);
            stream_set_chunk_size($pair[1], $outputSize);

            $pid = pcntl_fork();
            if ($pid === -1) throw new \RuntimeException('pcntl_fork failed');

            if ($pid === 0) {
                fclose($pair[0]);

                $output = str_repeat(chr(0), $outputSize);
                $fh     = fopen($inputPath, 'rb');
                stream_set_read_buffer($fh, 0);

                if ($useSemQueue) {
                    while (true) {
                        $ci = self::grabChunkSem($queueShm, $sem, $numChunks);
                        if ($ci === -1) break;
                        self::fillOutput($fh, $splitPoints[$ci], $splitPoints[$ci + 1], $slugBaseMap, $dateIds, $next, $output);
                    }
                } else {
                    $qf = fopen(sys_get_temp_dir() . '/p100m_' . $myPid . '_queue', 'c+b');
                    while (true) {
                        $ci = self::grabChunkFlock($qf, $numChunks);
                        if ($ci === -1) break;
                        self::fillOutput($fh, $splitPoints[$ci], $splitPoints[$ci + 1], $slugBaseMap, $dateIds, $next, $output);
                    }
                    fclose($qf);
                }

                fclose($fh);
                fwrite($pair[1], $output);
                fclose($pair[1]);
                exit(0);
            }

            fclose($pair[1]);
            $sockets[$w]  = $pair[0];
            $childMap[$w] = true;
        }

        if (!$useSemQueue) {
            $queueFile = sys_get_temp_dir() . '/p100m_' . $myPid . '_queue';
            file_put_contents($queueFile, pack('V', 0));
        }

        $output = str_repeat(chr(0), $outputSize);
        $fh     = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);

        if ($useSemQueue) {
            while (true) {
                $ci = self::grabChunkSem($queueShm, $sem, $numChunks);
                if ($ci === -1) break;
                self::fillOutput($fh, $splitPoints[$ci], $splitPoints[$ci + 1], $slugBaseMap, $dateIds, $next, $output);
            }
        } else {
            $qf = fopen(sys_get_temp_dir() . '/p100m_' . $myPid . '_queue', 'c+b');
            while (true) {
                $ci = self::grabChunkFlock($qf, $numChunks);
                if ($ci === -1) break;
                self::fillOutput($fh, $splitPoints[$ci], $splitPoints[$ci + 1], $slugBaseMap, $dateIds, $next, $output);
            }
            fclose($qf);
        }

        fclose($fh);

        $counts = array_fill(0, $outputSize, 0);
        $j      = 0;
        foreach (unpack('C*', $output) as $v) {
            $counts[$j++] = $v;
        }
        unset($output);

        while ($sockets !== []) {
            $read   = $sockets;
            $write  = [];
            $except = [];
            stream_select($read, $write, $except, 5);
            foreach ($read as $socket) {
                $key  = array_search($socket, $sockets, true);
                $data = '';
                while (!feof($socket)) {
                    $data .= fread($socket, $outputSize);
                }
                fclose($socket);
                unset($sockets[$key]);
                $j = 0;
                foreach (unpack('C*', $data) as $v) {
                    $counts[$j++] += $v;
                }
            }
        }

        if ($useSemQueue) {
            shmop_delete($queueShm);
            sem_remove($sem);
        } else {
            @unlink(sys_get_temp_dir() . '/p100m_' . $myPid . '_queue');
        }

        self::writeJson($outputPath, $counts, $paths, $dates, $dateCount);
    }

    private static function grabChunkSem($queueShm, $sem, $numChunks)
    {
        sem_acquire($sem);
        $idx = unpack('V', shmop_read($queueShm, 0, 4))[1];
        if ($idx >= $numChunks) {
            sem_release($sem);
            return -1;
        }
        shmop_write($queueShm, pack('V', $idx + 1), 0);
        sem_release($sem);
        return $idx;
    }

    private static function grabChunkFlock($qf, $numChunks)
    {
        flock($qf, LOCK_EX);
        fseek($qf, 0);
        $idx = unpack('V', fread($qf, 4))[1];
        if ($idx >= $numChunks) {
            flock($qf, LOCK_UN);
            return -1;
        }
        fseek($qf, 0);
        fwrite($qf, pack('V', $idx + 1));
        fflush($qf);
        flock($qf, LOCK_UN);
        return $idx;
    }

    private static function fillOutput($handle, $start, $end, $slugBaseMap, $dateIds, $next, &$output)
    {
        fseek($handle, $start);

        $remaining = $end - $start;
        $bufSize   = self::BUFFER_SIZE;
        $prefixLen = self::PREFIX_LEN;

        while ($remaining > 0) {
            $toRead = $remaining > $bufSize ? $bufSize : $remaining;
            $chunk  = fread($handle, $toRead);
            if (!$chunk) break;

            $chunkLen   = strlen($chunk);
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) continue;

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $p     = $prefixLen;
            $fence = $lastNl - 1010;

            while ($p < $fence) {
                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
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

    private static function writeJson($outputPath, $counts, $paths, $dates, $dateCount)
    {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);

        $pathCount    = count($paths);
        $datePrefixes = [];
        $escapedPaths = [];

        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "20' . $dates[$d] . '": ';
        }

        for ($p = 0; $p < $pathCount; $p++) {
            $escapedPaths[$p] = "\"\\/blog\\/" . str_replace('/', '\\/', $paths[$p]) . '"';
        }

        fwrite($out, '{');
        $firstPath = true;
        $base      = 0;

        for ($p = 0; $p < $pathCount; $p++) {
            $dateEntries = [];

            for ($d = 0; $d < $dateCount; $d++) {
                $count = $counts[$base + $d];
                if ($count !== 0) {
                    $dateEntries[] = $datePrefixes[$d] . $count;
                }
            }

            if (empty($dateEntries)) {
                $base += $dateCount;
                continue;
            }

            $sep2      = $firstPath ? '' : ',';
            $firstPath = false;

            fwrite($out,
                $sep2 .
                "\n    " . $escapedPaths[$p] . ": {\n" .
                implode(",\n", $dateEntries) .
                "\n    }"
            );

            $base += $dateCount;
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}