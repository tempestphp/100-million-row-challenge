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
use function filesize;
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
    private const int WORKERS = 10;
    private const int READ_CHUNK = 163_840;
    private const int DISCOVER_SIZE = 2_097_152;

    public function parse($inputPath, $outputPath)
    {
        gc_disable();

        $fileSize = filesize($inputPath);

        $dateIds = [];
        $dates = [];
        $dateCount = 0;

        // LICM: Pre-calculate constants
        for ($y = 20; $y <= 26; $y++) {
            $yearBase = $y + 2000;
            $isLeapYear = ($yearBase % 4 === 0);

            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => $isLeapYear ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $mStr = ($m < 10 ? '0' : '') . $m;
                $ymStr = $y . '-' . $mStr . '-';

                for ($d = 1; $d <= $maxD; $d++) {
                    $key = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $dateIds[$key] = $dateCount;
                    $dates[$dateCount] = $key;
                    $dateCount++;
                }
            }
        }

        $dateIdChars = [];
        foreach ($dateIds as $date => $id) {
            $dateIdChars[$date] = chr($id & 0xFF) . chr($id >> 8);
        }

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $warmUpSize = $fileSize > self::DISCOVER_SIZE ? self::DISCOVER_SIZE : $fileSize;
        $raw = fread($handle, $warmUpSize);
        fclose($handle);

        $pathIds = [];
        $paths = [];
        $pathCount = 0;
        $pos = 0;
        $lastNl = strrpos($raw, "\n");

        // LICM: Pre-calculate constants
        $slugOffset = 25;
        $slugEnd = 51;
        $nextLineOffset = 52;

        while ($pos < $lastNl) {
            $nlPos = strpos($raw, "\n", $pos + $nextLineOffset);
            if ($nlPos === false) break;

            $slug = substr($raw, $pos + $slugOffset, $nlPos - $pos - $slugEnd);
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug] = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }

            $pos = $nlPos + 1;
        }
        unset($raw);

        // LICM: Pre-calculate constant
        $uriOffset = 25;

        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, $uriOffset);
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug] = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }
        }

        $boundaries = [0];
        $bh = fopen($inputPath, 'rb');

        // LICM: Pre-calculate invariants outside loop
        $workers = self::WORKERS;
        $fileSizePerWorker = $fileSize / $workers;

        for ($i = 1; $i < $workers; $i++) {
            fseek($bh, (int) ($fileSizePerWorker * $i));
            fgets($bh);
            $boundaries[] = ftell($bh);
        }
        fclose($bh);
        $boundaries[] = $fileSize;

        $tmpDir = sys_get_temp_dir();
        $myPid = getmypid();
        $childMap = [];

        // LICM: Pre-calculate worker count
        $workerCount = $workers - 1;
        $tmpPrefix = $tmpDir . '/p100m_' . $myPid . '_';

        for ($w = 0; $w < $workerCount; $w++) {
            $tmpFile = $tmpPrefix . $w;
            $pid = pcntl_fork();
            if ($pid === 0) {
                $wCounts = $this->parseRange(
                    $inputPath, $boundaries[$w], $boundaries[$w + 1],
                    $pathIds, $dateIdChars, $pathCount, $dateCount,
                );
                file_put_contents($tmpFile, pack('v*', ...$wCounts));
                exit(0);
            }
            $childMap[$pid] = $tmpFile;
        }

        $counts = $this->parseRange(
            $inputPath, $boundaries[$workerCount], $boundaries[$workers],
            $pathIds, $dateIdChars, $pathCount, $dateCount,
        );

        $pending = count($childMap);
        while ($pending > 0) {
            $pid = pcntl_wait($status, WNOHANG);
            if ($pid <= 0) {
                $pid = pcntl_wait($status);
            }
            if (!isset($childMap[$pid])) continue;
            $tmpFile = $childMap[$pid];
            $wCounts = unpack('v*', file_get_contents($tmpFile));
            unlink($tmpFile);
            $j = 0;
            foreach ($wCounts as $v) {
                $counts[$j++] += $v;
            }
            $pending--;
        }

        $this->writeJson($outputPath, $counts, $paths, $dates, $dateCount);
    }

    private function parseRange(
        $inputPath, $start, $end,
        $pathIds, $dateIdChars,
        $pathCount, $dateCount,
    ) {
        $buckets = array_fill(0, $pathCount, '');
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);
        $remaining = $end - $start;

        // LICM: Move constant outside loop
        $readChunk = self::READ_CHUNK;

        while ($remaining > 0) {
            $toRead = $remaining > $readChunk ? $readChunk : $remaining;
            $chunk = fread($handle, $toRead);
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
            $fence = $lastNl - 720;

            // LICM: Unrolled loop with reduced array lookups
            while ($p < $fence) {
                $sep = strpos($chunk, ',', $p);
                $slug = substr($chunk, $p, $sep - $p);
                $date = substr($chunk, $sep + 3, 8);
                $buckets[$pathIds[$slug]] .= $dateIdChars[$date];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $slug = substr($chunk, $p, $sep - $p);
                $date = substr($chunk, $sep + 3, 8);
                $buckets[$pathIds[$slug]] .= $dateIdChars[$date];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $slug = substr($chunk, $p, $sep - $p);
                $date = substr($chunk, $sep + 3, 8);
                $buckets[$pathIds[$slug]] .= $dateIdChars[$date];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $slug = substr($chunk, $p, $sep - $p);
                $date = substr($chunk, $sep + 3, 8);
                $buckets[$pathIds[$slug]] .= $dateIdChars[$date];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $slug = substr($chunk, $p, $sep - $p);
                $date = substr($chunk, $sep + 3, 8);
                $buckets[$pathIds[$slug]] .= $dateIdChars[$date];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $slug = substr($chunk, $p, $sep - $p);
                $date = substr($chunk, $sep + 3, 8);
                $buckets[$pathIds[$slug]] .= $dateIdChars[$date];
                $p = $sep + 52;
            }

            while ($p < $lastNl) {
                $sep = strpos($chunk, ',', $p);
                if ($sep === false || $sep >= $lastNl) break;
                $slug = substr($chunk, $p, $sep - $p);
                $date = substr($chunk, $sep + 3, 8);
                $buckets[$pathIds[$slug]] .= $dateIdChars[$date];
                $p = $sep + 52;
            }
        }

        fclose($handle);

        // LICM: Pre-calculate total size
        $totalSize = $pathCount * $dateCount;
        $counts = array_fill(0, $totalSize, 0);

        for ($p = 0; $p < $pathCount; $p++) {
            if ($buckets[$p] === '') continue;
            $offset = $p * $dateCount;
            foreach (array_count_values(unpack('v*', $buckets[$p])) as $did => $count) {
                $counts[$offset + $did] += $count;
            }
        }

        return $counts;
    }

    private function writeJson(
        $outputPath, $counts, $paths,
        $dates, $dateCount,
    ) {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);
        fwrite($out, '{');

        // LICM: Pre-calculate date prefixes
        $datePrefixes = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "20' . $dates[$d] . '": ';
        }

        $pathCount = count($paths);

        // LICM: Pre-calculate escaped paths
        $escapedPaths = [];
        for ($p = 0; $p < $pathCount; $p++) {
            $escapedPaths[$p] = "\"\\/blog\\/" . str_replace('/', '\\/', $paths[$p]) . "\"";
        }

        // LICM: Pre-calculate constants
        $firstPath = true;
        $comma = ",\n    ";
        $newline = "\n    ";
        $objectStart = ": {\n";
        $objectEnd = "\n    }";
        $joinSeparator = ",\n";

        for ($p = 0; $p < $pathCount; $p++) {
            $base = $p * $dateCount;
            $dateEntries = [];

            for ($d = 0; $d < $dateCount; $d++) {
                $count = $counts[$base + $d];
                if ($count === 0) continue;
                $dateEntries[] = $datePrefixes[$d] . $count;
            }

            if ($dateEntries === []) continue;

            $buf = $firstPath ? $newline : $comma;
            $firstPath = false;
            $buf .= $escapedPaths[$p] . $objectStart . implode($joinSeparator, $dateEntries) . $objectEnd;
            fwrite($out, $buf);
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
