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

        $fileSize = 7_509_674_827;

        $dateIds = [];
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
                $ymStr = "{$y}-{$mStr}-";
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
        $warmUpSize = self::DISCOVER_SIZE;
        $raw = fread($handle, $warmUpSize);
        fclose($handle);

        $pathIds = [];
        $paths = [];
        $pathCount = 0;
        $pos = 0;
        $lastNl = strrpos($raw, "\n");

        while ($pos < $lastNl) {
            $nlPos = strpos($raw, "\n", $pos + 52);

            $slug = substr($raw, $pos + 25, $nlPos - $pos - 51);
            if (isset($pathIds[$slug])) {
                $pos = $nlPos + 1;
                continue;
            }
            $pathIds[$slug] = $pathCount;
            $paths[$pathCount] = $slug;
            $pathCount++;

            $pos = $nlPos + 1;
        }
        unset($raw);

        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, 25);
            if (isset($pathIds[$slug])) {
                continue;
            }
            $pathIds[$slug] = $pathCount;
            $paths[$pathCount] = $slug;
            $pathCount++;
        }

        $boundaries = [0];
        $bh = fopen($inputPath, 'rb');
        for ($i = 1; $i < self::WORKERS; $i++) {
            fseek($bh, (int) ($fileSize * $i / self::WORKERS));
            fgets($bh);
            $boundaries[] = ftell($bh);
        }
        fclose($bh);
        $boundaries[] = $fileSize;

        $tmpDir = sys_get_temp_dir();
        $myPid = getmypid();
        $childMap = [];

        for ($w = 0; $w < self::WORKERS - 1; $w++) {
            $tmpFile = "{$tmpDir}/p100m_{$myPid}_{$w}";
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
            $inputPath, $boundaries[self::WORKERS - 1], $boundaries[self::WORKERS],
            $pathIds, $dateIdChars, $pathCount, $dateCount,
        );

        $pending = count($childMap);
        while ($pending > 0) {
            $pid = pcntl_wait($status, WNOHANG);
            if ($pid <= 0) {
                $pid = pcntl_wait($status);
            }
            $tmpFile = $childMap[$pid];
            $wCounts = unpack('v*', file_get_contents($tmpFile));
            unlink($tmpFile);
            $j = 0;
            foreach ($wCounts as $v) {
                $counts[$j] += $v;
                $j++;
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

        while ($remaining > 0) {
            $toRead = $remaining > self::READ_CHUNK ? self::READ_CHUNK : $remaining;
            $chunk = fread($handle, $toRead);
            $chunkLen = strlen($chunk);
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) break;

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $p = 25;
            $fence = $lastNl - 600;

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
            }

            while ($p < $lastNl) {
                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;
            }
        }

        fclose($handle);

        $counts = array_fill(0, $pathCount * $dateCount, 0);
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
        $outputPath,
        $counts,
        $paths,
        $dates,
        $dateCount,
    ) {
        $output = fopen($outputPath, 'wb');
        stream_set_write_buffer($output, 1_048_576);

        $datePrefixes = [];
        for ($date = 0; $date < $dateCount; $date++) {
            $datePrefixes[$date] = '        "20' . $dates[$date] . '": ';
        }

        $escapedPaths = [];
        $pathCount = count($paths);
        for ($path = 0; $path < $pathCount; $path++) {
            $escapedPaths[$path] = '"\/blog\/' . str_replace('/', '\\/', $paths[$path]) . '"';
        }

        fwrite($output, '{');
        $first = true;

        for ($path = 0; $path < $pathCount; $path++) {
            $base = $path * $dateCount;
            $body = '';
            $sep = '';

            for ($date = 0; $date < $dateCount; $date++) {
                $value = $counts[$base + $date];
                if ($value === 0) {
                    continue;
                }
                $body .= $sep . $datePrefixes[$date] . $value;
                $sep = ",\n";
            }

            if ($body === '') {
                continue;
            }

            fwrite(
                $output,
                ($first ? '' : ',') . "\n    " . $escapedPaths[$path] . ": {\n" . $body . "\n    }",
            );
            $first = false;
        }

        fwrite($output, "\n}");
        fclose($output);
    }
}
