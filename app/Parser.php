<?php

namespace App;

use App\Commands\Visit;

use const SEEK_CUR;
use const WNOHANG;

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

final class Parser
{
    private const int WORKERS = 10;

    private const int READ_CHUNK = 163_840;

    public static function parse($inputPath, $outputPath)
    {
        gc_disable();

        $fileSize = filesize($inputPath);

        $dateIdChars = [];
        $dates = [];
        $dateCount = 0;

        for ($y = 21; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => $y === 24 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $mStr = ($m < 10 ? '0' : '') . $m;
                $ymStr = $y . '-' . $mStr . '-';
                for ($d = 1; $d <= $maxD; $d++) {
                    $key = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $dates[$dateCount] = $key;
                    $dateIdChars[$key] = chr($dateCount & 0xFF) . chr($dateCount >> 8);
                    $dateCount++;
                }
            }
        }

        $binaryResource = fopen($inputPath, 'rb');
        stream_set_read_buffer($binaryResource, 0);
        $warmUpSize = $fileSize > 131072 ? 131072 : $fileSize;
        $raw = fread($binaryResource, $warmUpSize);

        $boundaries = [0];
        for ($w = 1; $w < self::WORKERS; $w++) {
            fseek($binaryResource, (int) ($fileSize * $w / self::WORKERS));
            fgets($binaryResource);
            $boundaries[] = ftell($binaryResource);
        }

        fclose($binaryResource);

        $pathIds = [];
        $paths = [];
        $pathCount = 0;

        $lastNl = strrpos($raw, "\n");
        $p = 0;

        while ($p < $lastNl) {
            $sep = strpos($raw, ',', $p + 25);
            if ($sep === false) break;

            $slug = substr($raw, $p + 25, $sep - $p - 25);
            if (! isset($pathIds[$slug])) {
                $pathIds[$slug] = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }

            $p = $sep + 27;
        }
        unset($raw);

        foreach (Visit::SLUGS as $slug) {
            if (! isset($pathIds[$slug])) {
                $pathIds[$slug] = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }
        }

        $boundaries[] = $fileSize;

        $tmpDir = sys_get_temp_dir();
        $myPid = getmypid();
        $children = [];

        for ($w = 0; $w < self::WORKERS - 1; $w++) {
            $tmpFile = "{$tmpDir}/p100m-{$myPid}-{$w}";
            $pid = pcntl_fork();
            if ($pid === 0) {
                $wCounts = self::parseRange($inputPath, $boundaries[$w], $boundaries[$w + 1], $pathIds, $dateIdChars, $pathCount, $dateCount);
                file_put_contents($tmpFile, pack('v*', ...$wCounts));
                exit(0);
            }
            $children[$pid] = $tmpFile;
        }

        $counts = self::parseRange($inputPath, $boundaries[self::WORKERS - 1], $boundaries[self::WORKERS], $pathIds, $dateIdChars, $pathCount, $dateCount);

        $pending = count($children);
        while ($pending > 0) {
            $pid = pcntl_wait($status, WNOHANG);
            if ($pid <= 0) {
                $pid = pcntl_wait($status);
            }

            if (! isset($children[$pid])) {
                continue;
            }

            $tmpFile = $children[$pid];
            $wCounts = unpack('v*', file_get_contents($tmpFile));
            unlink($tmpFile);
            $j = 0;
            foreach ($wCounts as $v) {
                $counts[$j] += $v;
                $j++;
            }
            $pending--;
        }

        self::writeJson($outputPath, $counts, $paths, $dates, $dateCount);
    }

    private static function parseRange($inputPath, $start, $end, $pathIds, $dateIdChars, $pathCount, $dateCount)
    {
        $buckets = array_fill(0, $pathCount, '');
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);
        $remaining = $end - $start;
        $readChunk = self::READ_CHUNK;

        while ($remaining > 0) {
            $toRead = $remaining > $readChunk ? $readChunk : $remaining;
            $chunk = fread($handle, $toRead);
            $chunkLen = strlen($chunk);
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) {
                break;
            }

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $p = 25;
            $fence = $lastNl - 808;

            while ($p < $fence) {
                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;
            }

            while ($p < $lastNl) {
                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;
            }
        }

        fclose($handle);

        $counts = array_fill(0, $pathCount * $dateCount, 0);
        for ($i = 0; $i < $pathCount; $i++) {
            if ($buckets[$i] === '') {
                continue;
            }
            $offset = $i * $dateCount;
            foreach (array_count_values(unpack('v*', $buckets[$i])) as $dateId => $count) {
                $counts[$offset + $dateId] += $count;
            }
        }

        return $counts;
    }

    private static function writeJson($outputPath, $counts, $paths, $dates, $dateCount)
    {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);
        fwrite($out, '{');

        $pathCount = count($paths);

        $datePrefixes = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "20' . $dates[$d] . '": ';
        }

        $escapedPaths = [];
        for ($i = 0; $i < $pathCount; $i++) {
            $escapedPaths[$i] = "\"\\/blog\\/" . str_replace('/', '\\/', $paths[$i]) . "\"";
        }

        $firstPath = true;
        for ($i = 0; $i < $pathCount; $i++) {
            $base = $i * $dateCount;
            $dateEntries = [];

            for ($d = 0; $d < $dateCount; $d++) {
                $c = $counts[$base + $d];
                if ($c === 0) {
                    continue;
                }
                $dateEntries[] = $datePrefixes[$d] . $c;
            }

            if ($dateEntries === []) {
                continue;
            }

            $buf = $firstPath ? "\n    " : ",\n    ";
            $firstPath = false;
            $buf .= $escapedPaths[$i] . ": {\n" . implode(",\n", $dateEntries) . "\n    }";
            fwrite($out, $buf);
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
