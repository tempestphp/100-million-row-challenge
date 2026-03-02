<?php

namespace App;

use App\Commands\Visit;

use function array_count_values;
use function array_fill;
use function chr;
use function count;
use function fclose;
use function fgets;
use function file_get_contents;
use function file_put_contents;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function getmypid;
use function implode;
use function pack;
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
use function str_replace;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function sys_get_temp_dir;
use function unlink;
use function unpack;

use const SEEK_CUR;
use const WNOHANG;

final class Parser
{
    private const int BUFFER_SIZE = 163_840;
    private const int DISC_SIZE   = 131_072;
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

        $dateIdBytes = [];
        foreach ($dateIds as $date => $id) {
            $dateIdBytes[$date] = chr($id & 0xFF) . chr($id >> 8);
        }

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $raw = fread($handle, self::DISC_SIZE);
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

        $tmpDir    = sys_get_temp_dir();
        $myPid     = getmypid();
        $tmpPrefix = $tmpDir . '/p100m_' . $myPid;

        $shmSegSize = $pathCount * $dateCount * 2;
        $shmHandles = [];
        $useShm     = false;

        $allOk = true;
        for ($w = 0; $w < $numWorkers - 1; $w++) {
            $shmKey = $myPid * 100 + $w;
            set_error_handler(null);
            $shm = @shmop_open($shmKey, 'c', 0644, $shmSegSize);
            set_error_handler(null);
            if ($shm === false) {
                foreach ($shmHandles as [$k, $s]) {
                    shmop_delete($s);
                }
                $shmHandles = [];
                $allOk      = false;
                break;
            }
            $shmHandles[$w] = [$shmKey, $shm];
        }
        $useShm = $allOk;

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
        } else {
            $queueFile = $tmpPrefix . '_queue';
            file_put_contents($queueFile, pack('V', 0));
        }

        $n        = $pathCount * $dateCount;
        $childMap = [];

        for ($w = 0; $w < $numWorkers - 1; $w++) {
            $tmpFile = $tmpPrefix . '_' . $w;
            $pid     = pcntl_fork();
            if ($pid === -1) throw new \RuntimeException('pcntl_fork failed');

            if ($pid === 0) {
                $buckets = array_fill(0, $pathCount, '');
                $fh      = fopen($inputPath, 'rb');
                stream_set_read_buffer($fh, 0);

                if ($useSemQueue) {
                    while (true) {
                        $ci = self::grabChunkSem($queueShm, $sem, $numChunks);
                        if ($ci === -1) break;
                        self::fillBuckets($fh, $splitPoints[$ci], $splitPoints[$ci + 1], $pathIds, $dateIdBytes, $buckets);
                    }
                } else {
                    $qf = fopen($queueFile, 'c+b');
                    while (true) {
                        $ci = self::grabChunkFlock($qf, $numChunks);
                        if ($ci === -1) break;
                        self::fillBuckets($fh, $splitPoints[$ci], $splitPoints[$ci + 1], $pathIds, $dateIdBytes, $buckets);
                    }
                    fclose($qf);
                }

                fclose($fh);

                $counts = self::bucketsToCounts($buckets, $pathCount, $dateCount);
                $packed = pack('v*', ...$counts);

                if ($useShm) {
                    shmop_write($shmHandles[$w][1], $packed, 0);
                } else {
                    file_put_contents($tmpFile, $packed);
                }

                exit(0);
            }

            $childMap[$pid] = $w;
        }

        $buckets = array_fill(0, $pathCount, '');
        $fh      = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);

        if ($useSemQueue) {
            while (true) {
                $ci = self::grabChunkSem($queueShm, $sem, $numChunks);
                if ($ci === -1) break;
                self::fillBuckets($fh, $splitPoints[$ci], $splitPoints[$ci + 1], $pathIds, $dateIdBytes, $buckets);
            }
        } else {
            $qf = fopen($queueFile, 'c+b');
            while (true) {
                $ci = self::grabChunkFlock($qf, $numChunks);
                if ($ci === -1) break;
                self::fillBuckets($fh, $splitPoints[$ci], $splitPoints[$ci + 1], $pathIds, $dateIdBytes, $buckets);
            }
            fclose($qf);
        }

        fclose($fh);

        $counts = self::bucketsToCounts($buckets, $pathCount, $dateCount);

        $pending = count($childMap);
        while ($pending > 0) {
            $pid = pcntl_wait($status, WNOHANG);
            if ($pid <= 0) {
                $pid = pcntl_wait($status);
            }
            if (!isset($childMap[$pid])) continue;

            $w = $childMap[$pid];
            unset($childMap[$pid]);

            if ($useShm) {
                $packed = shmop_read($shmHandles[$w][1], 0, $shmSegSize);
                shmop_delete($shmHandles[$w][1]);
            } else {
                $tmpFile = $tmpPrefix . '_' . $w;
                $packed  = file_get_contents($tmpFile);
                unlink($tmpFile);
            }

            $childCounts = unpack('v*', $packed);
            for ($j = 0, $k = 1; $j < $n; $j++, $k++) {
                $counts[$j] += $childCounts[$k];
            }
            $pending--;
        }

        if ($useSemQueue) {
            shmop_delete($queueShm);
            sem_remove($sem);
        } else {
            unlink($queueFile);
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

    private static function fillBuckets($handle, $start, $end, $pathIds, $dateIdBytes, &$buckets)
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
            $fence = $lastNl - 792;

            while ($p < $fence) {
                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdBytes[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdBytes[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdBytes[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdBytes[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdBytes[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdBytes[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdBytes[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdBytes[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;
            }

            while ($p < $lastNl) {
                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdBytes[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;
            }
        }
    }

    private static function bucketsToCounts(&$buckets, $pathCount, $dateCount)
    {
        $counts = array_fill(0, $pathCount * $dateCount, 0);
        $base   = 0;
        foreach ($buckets as $bucket) {
            if ($bucket !== '') {
                foreach (array_count_values(unpack('v*', $bucket)) as $did => $cnt) {
                    $counts[$base + $did] += $cnt;
                }
            }
            $base += $dateCount;
        }
        return $counts;
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