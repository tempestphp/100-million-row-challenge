<?php

namespace App;

use App\Commands\Visit;

use function array_count_values;
use function array_fill;
use function array_keys;
use function array_search;
use function fclose;
use function fflush;
use function fgets;
use function file_get_contents;
use function file_put_contents;
use function filesize;
use function flock;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function getmypid;
use function intdiv;
use function min;
use function pack;
use function pcntl_fork;
use function pcntl_wait;
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

use const LOCK_EX;
use const LOCK_UN;
use const SEEK_CUR;

final class Parser
{
    public function parse($inputPath, $outputPath)
    {
        \gc_disable();
        $fileSize = filesize($inputPath);
        $numChunks = 16;

        // Build all dates 2021-2026
        $dateChars = [];
        $datePrefixes = [];
        $dateCount = 0;
        $dpm = [0,31,28,31,30,31,30,31,31,30,31,30,31];
        for ($y = 2021; $y <= 2026; $y++) {
            $dpm[2] = ($y % 4 === 0) ? 29 : 28;
            $yStr = (string)$y;
            for ($m = 1; $m <= 12; $m++) {
                $mStr = ($m < 10 ? '0' : '') . $m;
                $ym = $yStr . '-' . $mStr . '-';
                for ($d = 1; $d <= $dpm[$m]; $d++) {
                    $full = $ym . (($d < 10) ? '0' : '') . $d;
                    $dateChars[substr($full, 2)] = pack('v', $dateCount);
                    $datePrefixes[$dateCount] = "        \"{$full}\": ";
                    $dateCount++;
                }
            }
        }

        [$pathIds, $pathMap, $pathCount] = self::discover($inputPath, $fileSize);
        $pathPrefixes = [];
        for ($p = 0; $p < $pathCount; $p++) {
            $pathPrefixes[$p] = "\n    \"\\/blog\\/" . str_replace('/', '\\/', $pathMap[$p]) . "\": {";
        }

        // Find chunk boundaries aligned to newlines (many small chunks for work-stealing)
        $boundaries = [0];
        $handle = fopen($inputPath, 'rb');
        for ($i = 1; $i < $numChunks; $i++) {
            fseek($handle, intdiv($fileSize * $i, $numChunks));
            fgets($handle);
            $boundaries[] = ftell($handle);
        }
        $boundaries[] = $fileSize;
        fclose($handle);

        // Work-stealing queue: shared file with atomic counter
        $tmpPrefix = sys_get_temp_dir() . '/parse_' . getmypid();
        $queueFile = $tmpPrefix . '_queue';
        file_put_contents($queueFile, pack('V', 0));

        // Fork child workers
        $pids = [];
        for ($i = 0; $i < 7; $i++) {
            $pid = pcntl_fork();
            if ($pid === -1) continue;
            if ($pid === 0) {
                $buckets = array_fill(0, $pathCount, '');
                $fh = fopen($inputPath, 'rb');
                stream_set_read_buffer($fh, 0);
                $qf = fopen($queueFile, 'c+b');
                while (true) {
                    $ci = self::grabChunk($qf, $numChunks);
                    if ($ci === -1) break;
                    self::processRange($fh, $boundaries[$ci], $boundaries[$ci + 1], $pathIds, $dateChars, $buckets);
                }
                fclose($qf);
                fclose($fh);
                $counts = self::mergeBuckets($buckets, $pathCount, $dateCount);
                file_put_contents($tmpPrefix . "_{$i}", pack('v*', ...$counts));
                exit(0);
            }
            $pids[$i] = $pid;
        }

        // Parent also steals work
        $buckets = array_fill(0, $pathCount, '');
        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);
        $qf = fopen($queueFile, 'c+b');
        while (true) {
            $ci = self::grabChunk($qf, $numChunks);
            if ($ci === -1) break;
            self::processRange($fh, $boundaries[$ci], $boundaries[$ci + 1], $pathIds, $dateChars, $buckets);
        }
        fclose($qf);
        fclose($fh);
        $counts = self::mergeBuckets($buckets, $pathCount, $dateCount);

        // Wait for children and merge
        while ($pids) {
            $pid = pcntl_wait($status);
            $i = array_search($pid, $pids, true);
            if ($i === false) continue;
            unset($pids[$i]);
            $f = $tmpPrefix . "_{$i}";
            $raw = file_get_contents($f);
            unlink($f);
            $childCounts = unpack('v*', $raw);
            $j = 0;
            foreach ($childCounts as $val) {
                $counts[$j++] += $val;
            }
        }
        unlink($queueFile);

        // Write JSON
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);
        $buf = '{';

        $firstPath = true;
        $base = 0;
        for ($p = 0; $p < $pathCount; $p++) {

            $dateBuf = '';
            $sep = "\n";
            for ($d = 0; $d < $dateCount; $d++) {
                $n = $counts[$base + $d];
                if ($n === 0) continue;
                $dateBuf .= $sep . $datePrefixes[$d] . $n;
                $sep = ",\n";
            }

            if ($dateBuf === '') continue;

            $buf .= ($firstPath ? '' : ',') . $pathPrefixes[$p] . $dateBuf . "\n    }";
            $firstPath = false;

            if (strlen($buf) > 65536) {
                fwrite($out, $buf);
                $buf = '';
            }
            $base += $dateCount;
        }

        fwrite($out, $buf . "\n}");
        fclose($out);
    }

    private static function processRange($fh, $start, $end, &$pathIds, &$dateChars, &$buckets)
    {
        fseek($fh, $start);
        $bytesProcessed = 0;
        $toProcess = $end - $start;
        while ($bytesProcessed < $toProcess) {
            $remaining = $toProcess - $bytesProcessed;
            $chunk = fread($fh, $remaining > 131072 ? 131072 : $remaining);
            if (!$chunk) break;
            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) continue;
            $tail = strlen($chunk) - $lastNl - 1;
            if ($tail > 0) {
                fseek($fh, -$tail, SEEK_CUR);
            }
            $bytesProcessed += $lastNl + 1;
            $p = 25;
            $limit = $lastNl - 800;
            while ($p < $limit) {
                $c = strpos($chunk, ",", $p);
                $buckets[$pathIds[substr($chunk, $p, $c - $p)]] .= $dateChars[substr($chunk, $c + 3, 8)];
                $p = $c + 52;
                $c = strpos($chunk, ",", $p);
                $buckets[$pathIds[substr($chunk, $p, $c - $p)]] .= $dateChars[substr($chunk, $c + 3, 8)];
                $p = $c + 52;
                $c = strpos($chunk, ",", $p);
                $buckets[$pathIds[substr($chunk, $p, $c - $p)]] .= $dateChars[substr($chunk, $c + 3, 8)];
                $p = $c + 52;
                $c = strpos($chunk, ",", $p);
                $buckets[$pathIds[substr($chunk, $p, $c - $p)]] .= $dateChars[substr($chunk, $c + 3, 8)];
                $p = $c + 52;
                $c = strpos($chunk, ",", $p);
                $buckets[$pathIds[substr($chunk, $p, $c - $p)]] .= $dateChars[substr($chunk, $c + 3, 8)];
                $p = $c + 52;
                $c = strpos($chunk, ",", $p);
                $buckets[$pathIds[substr($chunk, $p, $c - $p)]] .= $dateChars[substr($chunk, $c + 3, 8)];
                $p = $c + 52;
                $c = strpos($chunk, ",", $p);
                $buckets[$pathIds[substr($chunk, $p, $c - $p)]] .= $dateChars[substr($chunk, $c + 3, 8)];
                $p = $c + 52;
                $c = strpos($chunk, ",", $p);
                $buckets[$pathIds[substr($chunk, $p, $c - $p)]] .= $dateChars[substr($chunk, $c + 3, 8)];
                $p = $c + 52;
            }
            while ($p < $lastNl) {
                $c = strpos($chunk, ",", $p);
                if ($c === false || $c >= $lastNl) break;
                $buckets[$pathIds[substr($chunk, $p, $c - $p)]] .= $dateChars[substr($chunk, $c + 3, 8)];
                $p = $c + 52;
            }
        }
    }

    private static function mergeBuckets(&$buckets, $pathCount, $dateCount)
    {
        $counts = array_fill(0, $pathCount * $dateCount, 0);
        $base = 0;
        foreach ($buckets as $bucket) {
            if ($bucket !== '') {
                foreach (array_count_values(unpack('v*', $bucket)) as $dateId => $n) {
                    $counts[$base + $dateId] += $n;
                }
            }
            $base += $dateCount;
        }
        return $counts;
    }

    private static function discover($inputPath, $fileSize)
    {
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $chunk = fread($handle, min($fileSize, 204800));
        fclose($handle);

        $lastNl = strrpos($chunk, "\n");
        $pathIds = [];
        $pathCount = 0;
        $pos = 0;

        while ($pos < $lastNl) {
            $nlPos = strpos($chunk, "\n", $pos + 54);
            if ($nlPos === false) break;

            $pathStr = substr($chunk, $pos + 25, $nlPos - $pos - 51);
            if (!isset($pathIds[$pathStr])) {
                $pathIds[$pathStr] = $pathCount++;
            }

            $pos = $nlPos + 1;
        }

        foreach (Visit::all() as $visit) {
            $pathStr = substr($visit->uri, 25);
            if (!isset($pathIds[$pathStr])) {
                $pathIds[$pathStr] = $pathCount++;
            }
        }

        $pathMap = array_keys($pathIds);

        return [$pathIds, $pathMap, $pathCount];
    }

    private static function grabChunk($f, $numChunks)
    {
        flock($f, LOCK_EX);
        fseek($f, 0);
        $idx = unpack('V', fread($f, 4))[1];
        if ($idx >= $numChunks) {
            flock($f, LOCK_UN);
            return -1;
        }
        fseek($f, 0);
        fwrite($f, pack('V', $idx + 1));
        fflush($f);
        flock($f, LOCK_UN);
        return $idx;
    }
}
