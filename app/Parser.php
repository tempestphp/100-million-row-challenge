<?php

namespace App;

use App\Commands\Visit;

use function array_count_values;
use function array_fill;
use function array_fill_keys;
use function array_filter;
use function array_slice;
use function chr;
use function count;
use function date;
use function fclose;
use function fgets;
use function file_exists;
use function file_get_contents;
use function file_put_contents;
use function filesize;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function getmypid;
use function intdiv;
use function is_dir;
use function pack;
use function pcntl_fork;
use function pcntl_wait;
use function posix_kill;
use function str_replace;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function strtotime;
use function substr;
use function sys_get_temp_dir;
use function unlink;
use function unpack;

use const SEEK_CUR;
use const SIGKILL;
use const WNOHANG;

final class Parser
{
    private const int WORKERS    = 8;
    private const int CHUNKS     = 8;
    private const int READ_CHUNK = 524_288;
    private const int DISC_SIZE  = 2_097_152;
    private const int PREFIX_LEN = 25;

    public function __call(string $name, array $arguments): mixed
    {
        return static::$name(...$arguments);
    }

    public static function parse($inputPath, $outputPath): void
    {
        gc_disable();

        $fileSize = filesize($inputPath);

        $dh  = fopen($inputPath, 'rb');
        $raw = fread($dh, min(self::DISC_SIZE, $fileSize));
        fclose($dh);

        $pathIds   = [];
        $paths     = [];
        $pathCount = 0;
        $pos       = 0;
        $lastNl    = strrpos($raw, "\n") ?: 0;
        $minDate   = '2026-99-99';
        $maxDate   = '2000-00-00';

        while ($pos < $lastNl) {
            $nl = strpos($raw, "\n", $pos + 52);
            if ($nl === false) break;
            $slug = substr($raw, $pos + self::PREFIX_LEN, $nl - $pos - 51);
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug]    = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }
            $date = substr($raw, $nl - 25, 10);
            if ($date < $minDate) $minDate = $date;
            if ($date > $maxDate) $maxDate = $date;
            $pos = $nl + 1;
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

        $dateChars = [];
        $dates     = [];
        $dateCount = 0;
        $ts        = strtotime($minDate) - 86400 * 60;
        $tsEnd     = strtotime($maxDate) + 86400 * 60;

        while ($ts <= $tsEnd) {
            $full              = date('Y-m-d', $ts);
            $key               = substr($full, 3);
            $dateChars[$key]   = chr($dateCount & 0xFF) . chr($dateCount >> 8);
            $dates[$dateCount] = $full;
            $dateCount++;
            $ts += 86400;
        }

        $pathOffsets = [];
        foreach ($pathIds as $slug => $id) {
            $pathOffsets[$slug] = $id * $dateCount;
        }

        $datePrefixes = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "' . $dates[$d] . '": ';
        }

        $pathPrefixes = [];
        for ($p = 0; $p < $pathCount; $p++) {
            $pathPrefixes[$p] = "\n    \"\\/blog\\/" . str_replace('/', '\\/', $paths[$p]) . '": {';
        }

        $splitPoints = [0];
        $bh = fopen($inputPath, 'rb');
        for ($i = 1; $i < self::CHUNKS; $i++) {
            fseek($bh, intdiv($fileSize * $i, self::CHUNKS));
            fgets($bh);
            $splitPoints[] = ftell($bh);
        }
        fclose($bh);
        $splitPoints[] = $fileSize;

        $chunksPerWorker = self::CHUNKS / self::WORKERS; // = 4
        $workerRanges    = [];
        for ($w = 0; $w < self::WORKERS; $w++) {
            $first              = $w * $chunksPerWorker;
            $workerRanges[$w]   = [$splitPoints[$first], $splitPoints[$first + $chunksPerWorker]];
        }

        $myPid     = getmypid();
        $tmpDir    = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
        $tmpPrefix = $tmpDir . '/p100m_' . $myPid;
        $childMap  = [];

        for ($w = 0; $w < self::WORKERS - 1; $w++) {
            $outFile = $tmpPrefix . '_' . $w;
            $pid     = pcntl_fork();
            if ($pid === 0) {
                $fh = fopen($inputPath, 'rb');
                stream_set_read_buffer($fh, 0);

                $buckets = array_fill_keys($paths, '');
                [$from, $to] = $workerRanges[$w];
                self::fillBuckets($fh, $from, $to, $dateChars, $buckets);

                fclose($fh);

                $counts = self::bucketsToCounts($buckets, $pathOffsets, $pathCount, $dateCount);
                file_put_contents($outFile, pack('v*', ...$counts));

                posix_kill(posix_getpid(), SIGKILL);
            }
            $childMap[$pid] = $outFile;
        }

        $fh      = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);
        $buckets = array_fill_keys($paths, '');
        [$from, $to] = $workerRanges[self::WORKERS - 1];
        self::fillBuckets($fh, $from, $to, $dateChars, $buckets);
        fclose($fh);

        $counts  = self::bucketsToCounts($buckets, $pathOffsets, $pathCount, $dateCount);
        $pending = count($childMap);

        while ($pending > 0) {
            $pid = pcntl_wait($status, WNOHANG);
            if ($pid <= 0) $pid = pcntl_wait($status);
            $outFile = $childMap[$pid];
            if (file_exists($outFile)) {
                $childCounts = unpack('v*', file_get_contents($outFile));
                unlink($outFile);
                $j = 0;
                foreach ($childCounts as $v) $counts[$j++] += $v;
            }
            unset($childMap[$pid]);
            $pending--;
        }

        self::writeJson($outputPath, $counts, $pathPrefixes, $datePrefixes, $pathCount, $dateCount);
    }

    private static function fillBuckets($handle, int $start, int $end, array $dateChars, array &$buckets): void
    {
        fseek($handle, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $toRead   = $remaining > self::READ_CHUNK ? self::READ_CHUNK : $remaining;
            $chunk    = fread($handle, $toRead);
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

            $p     = self::PREFIX_LEN;
            $fence = $lastNl - 650;

            while ($p < $fence) {
                $sep = strpos($chunk, ',', $p);
                $buckets[substr($chunk, $p, $sep - $p)] .= $dateChars[substr($chunk, $sep + 4, 7)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[substr($chunk, $p, $sep - $p)] .= $dateChars[substr($chunk, $sep + 4, 7)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[substr($chunk, $p, $sep - $p)] .= $dateChars[substr($chunk, $sep + 4, 7)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[substr($chunk, $p, $sep - $p)] .= $dateChars[substr($chunk, $sep + 4, 7)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[substr($chunk, $p, $sep - $p)] .= $dateChars[substr($chunk, $sep + 4, 7)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[substr($chunk, $p, $sep - $p)] .= $dateChars[substr($chunk, $sep + 4, 7)];
                $p = $sep + 52;
            }

            while ($p < $lastNl) {
                $sep = strpos($chunk, ',', $p);
                if ($sep === false || $sep >= $lastNl) break;
                $slug = substr($chunk, $p, $sep - $p);
                $key  = substr($chunk, $sep + 4, 7);
                if (isset($dateChars[$key])) $buckets[$slug] .= $dateChars[$key];
                $p = $sep + 52;
            }
        }
    }

    private static function bucketsToCounts(array &$buckets, array $pathOffsets, int $pathCount, int $dateCount): array
    {
        $counts = array_fill(0, $pathCount * $dateCount, 0);
        foreach ($buckets as $slug => $bucket) {
            $base = $pathOffsets[$slug];
            foreach (array_count_values(unpack('v*', $bucket)) as $did => $cnt) {
                $counts[$base + $did] += $cnt;
            }
        }
        return $counts;
    }

    private static function writeJson(
        string $outputPath, array $counts,
        array $pathPrefixes, array $datePrefixes,
        int $pathCount, int $dateCount
    ): void {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);

        $buf    = '{';
        $bufLen = 1;
        $first  = true;
        $base   = 0;

        for ($p = 0; $p < $pathCount; $p++) {
            $nonZero = array_filter(array_slice($counts, $base, $dateCount));

            if ($nonZero) {
                $dateBuf = '';
                $sep     = "\n";
                foreach ($nonZero as $d => $n) {
                    $dateBuf .= $sep . $datePrefixes[$d] . $n;
                    $sep = ",\n";
                }

                $entry   = ($first ? '' : ',') . $pathPrefixes[$p] . $dateBuf . "\n    }";
                $first   = false;
                $buf    .= $entry;
                $bufLen += strlen($entry);

                if ($bufLen > 65536) {
                    fwrite($out, $buf);
                    $buf    = '';
                    $bufLen = 0;
                }
            }

            $base += $dateCount;
        }

        fwrite($out, $buf . "\n}");
        fclose($out);
    }
}