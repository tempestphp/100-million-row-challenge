<?php

namespace App;

use App\Commands\Visit;

final class Parser
{
    public function parse($inputPath, $outputPath)
    {
        $fileSize = \filesize($inputPath);

        [$pathIds, $pathMap, $pathCount, $dateChars, $dateMap, $dateCount] = self::discover($inputPath, $fileSize);

        // Pre-build output strings
        $datePrefixes = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = "        \"{$dateMap[$d]}\": ";
        }
        $pathPrefixes = [];
        for ($p = 0; $p < $pathCount; $p++) {
            $pathPrefixes[$p] = "\n    \"\\/blog\\/" . \str_replace('/', '\\/', $pathMap[$p]) . "\": {";
        }

        // Single-thread: process entire file
        $buckets = \array_fill(0, $pathCount, '');
        $fh = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($fh, 0);
        self::fillBuckets($fh, 0, $fileSize, $pathIds, $dateChars, $buckets);
        \fclose($fh);
        $counts = self::bucketsToCounts($buckets, $pathCount, $dateCount);

        // Write JSON
        $out = \fopen($outputPath, 'wb');
        \stream_set_write_buffer($out, 1_048_576);
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

            if (\strlen($buf) > 65536) {
                \fwrite($out, $buf);
                $buf = '';
            }
            $base += $dateCount;
        }

        \fwrite($out, $buf . "\n}");
        \fclose($out);
    }

    private static function discover($inputPath, $fileSize)
    {
        $handle = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($handle, 0);
        $chunk = \fread($handle, \min($fileSize, 204800));
        \fclose($handle);

        $lastNl = \strrpos($chunk, "\n");
        $pathIds = [];
        $pathCount = 0;
        $minDate = '9999-99-99';
        $maxDate = '0000-00-00';
        $pos = 0;

        while ($pos < $lastNl) {
            $nlPos = \strpos($chunk, "\n", $pos + 54);
            if ($nlPos === false) break;

            $pathStr = \substr($chunk, $pos + 25, $nlPos - $pos - 51);
            if (!isset($pathIds[$pathStr])) {
                $pathIds[$pathStr] = $pathCount++;
            }

            $date = \substr($chunk, $nlPos - 25, 10);
            if ($date < $minDate) $minDate = $date;
            if ($date > $maxDate) $maxDate = $date;

            $pos = $nlPos + 1;
        }

        foreach (Visit::all() as $visit) {
            $pathStr = \substr($visit->uri, 25);
            if (!isset($pathIds[$pathStr])) {
                $pathIds[$pathStr] = $pathCount++;
            }
        }

        $pathMap = \array_keys($pathIds);

        $dateChars = [];
        $dateMap = [];
        $dateCount = 0;
        $ts = \strtotime($minDate) - 86400 * 7;
        $end = \strtotime($maxDate) + 86400 * 7;
        while ($ts <= $end) {
            $full = \date('Y-m-d', $ts);
            $dateChars[\substr($full, 3)] = \pack('v', $dateCount);
            $dateMap[$dateCount] = $full;
            $dateCount++;
            $ts += 86400;
        }

        return [$pathIds, $pathMap, $pathCount, $dateChars, $dateMap, $dateCount];
    }

    private static function fillBuckets($handle, $start, $end, &$pathIds, &$dateChars, &$buckets)
    {
        \fseek($handle, $start);

        $bytesProcessed = 0;
        $toProcess = $end - $start;

        while ($bytesProcessed < $toProcess) {
            $remaining = $toProcess - $bytesProcessed;
            $chunk = \fread($handle, $remaining > 131072 ? 131072 : $remaining);
            if (!$chunk) break;

            $lastNl = \strrpos($chunk, "\n");
            if ($lastNl === false) continue;

            $tail = \strlen($chunk) - $lastNl - 1;
            if ($tail > 0) {
                \fseek($handle, -$tail, SEEK_CUR);
            }
            $bytesProcessed += $lastNl + 1;

            $p = 25;
            $limit = $lastNl - 600;
            while ($p < $limit) {
                $c = \strpos($chunk, ",", $p);
                $buckets[$pathIds[\substr($chunk, $p, $c - $p)]] .= $dateChars[\substr($chunk, $c + 4, 7)];
                $p = $c + 52;

                $c = \strpos($chunk, ",", $p);
                $buckets[$pathIds[\substr($chunk, $p, $c - $p)]] .= $dateChars[\substr($chunk, $c + 4, 7)];
                $p = $c + 52;

                $c = \strpos($chunk, ",", $p);
                $buckets[$pathIds[\substr($chunk, $p, $c - $p)]] .= $dateChars[\substr($chunk, $c + 4, 7)];
                $p = $c + 52;

                $c = \strpos($chunk, ",", $p);
                $buckets[$pathIds[\substr($chunk, $p, $c - $p)]] .= $dateChars[\substr($chunk, $c + 4, 7)];
                $p = $c + 52;
            }
            while ($p < $lastNl) {
                $c = \strpos($chunk, ",", $p);
                if ($c === false || $c >= $lastNl) break;
                $buckets[$pathIds[\substr($chunk, $p, $c - $p)]] .= $dateChars[\substr($chunk, $c + 4, 7)];
                $p = $c + 52;
            }
        }
    }

    private static function bucketsToCounts(&$buckets, $pathCount, $dateCount)
    {
        $counts = \array_fill(0, $pathCount * $dateCount, 0);
        $base = 0;
        foreach ($buckets as $bucket) {
            if ($bucket !== '') {
                foreach (\array_count_values(\unpack('v*', $bucket)) as $dateId => $n) {
                    $counts[$base + $dateId] += $n;
                }
            }
            $base += $dateCount;
        }
        return $counts;
    }

}