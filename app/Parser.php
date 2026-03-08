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
use function pack;
use function pcntl_fork;
use function pcntl_wait;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function str_replace;
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
    private const int READ_BYTES = 2_097_152;
    private const int PROBE_BYTES = 2_097_152;

    public function parse($inputPath, $outputPath)
    {
        gc_disable();
        $fileSize = filesize($inputPath);

        $dateChars = [];
        $dateMap = [];
        $dateCount = 0;
        for ($y = 20; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => (($y + 2000) % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $mStr = ($m < 10 ? '0' : '') . $m;
                $ymStr = $y . '-' . $mStr . '-';
                for ($d = 1; $d <= $maxD; $d++) {
                    $dStr = ($d < 10 ? '0' : '') . $d;
                    $shortKey = substr((string)$y, 1) . '-' . $mStr . '-' . $dStr;
                    $dateChars[$shortKey] = chr($dateCount & 0xFF) . chr($dateCount >> 8);
                    $dateMap[$dateCount] = '20' . $ymStr . $dStr;
                    $dateCount++;
                }
            }
        }

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $warmUpSize = $fileSize > self::PROBE_BYTES ? self::PROBE_BYTES : $fileSize;
        $chunk = fread($handle, $warmUpSize);
        fclose($handle);

        $lastNl = strrpos($chunk, "\n");

        $pathIds = [];
        $paths = [];
        $pathCount = 0;

        if ($lastNl !== false) {
            $pos = 0;
            while ($pos < $lastNl) {
                $nlPos = strpos($chunk, "\n", $pos + 52);
                if ($nlPos === false) break;

                $path = substr($chunk, $pos + 25, $nlPos - $pos - 51);
                if (!isset($pathIds[$path])) {
                    $pathIds[$path] = $pathCount;
                    $paths[$pathCount] = $path;
                    $pathCount++;
                }

                $pos = $nlPos + 1;
            }
        }
        unset($chunk);

        foreach (Visit::all() as $visit) {
            $path = substr($visit->uri, 25);
            if (!isset($pathIds[$path])) {
                $pathIds[$path] = $pathCount;
                $paths[$pathCount] = $path;
                $pathCount++;
            }
        }

        $datePrefixes = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "' . $dateMap[$d] . '": ';
        }

        $pathPrefixes = [];
        for ($p = 0; $p < $pathCount; $p++) {
            $pathPrefixes[$p] = "\n    \"\\/blog\\/" . str_replace('/', '\\/', $paths[$p]) . "\": {";
        }

        $boundaries = [0];
        $handle = fopen($inputPath, 'rb');
        for ($i = 1; $i < self::WORKERS; $i++) {
            fseek($handle, (int) ($fileSize * $i / self::WORKERS));
            fgets($handle);
            $boundaries[] = ftell($handle);
        }
        fclose($handle);
        $boundaries[] = $fileSize;

        $tmpDir = sys_get_temp_dir();
        $myPid = getmypid();
        $childMap = [];

        for ($w = 0; $w < self::WORKERS - 1; $w++) {
            $tmpFile = $tmpDir . '/p100m_' . $myPid . '_' . $w;
            $pid = pcntl_fork();

            if ($pid === 0) {
                $buckets = array_fill(0, $pathCount, '');
                $this->fillBuckets($inputPath, $boundaries[$w], $boundaries[$w + 1], $pathIds, $dateChars, $buckets);
                $counts = $this->bucketsToCounts($buckets, $pathCount, $dateCount);
                file_put_contents($tmpFile, pack('v*', ...$counts));
                exit(0);
            }

            $childMap[$pid] = $tmpFile;
        }

        $buckets = array_fill(0, $pathCount, '');
        $this->fillBuckets($inputPath, $boundaries[self::WORKERS - 1], $boundaries[self::WORKERS], $pathIds, $dateChars, $buckets);
        $counts = $this->bucketsToCounts($buckets, $pathCount, $dateCount);

        $pending = count($childMap);
        while ($pending > 0) {
            $pid = pcntl_wait($status, WNOHANG);
            if ($pid <= 0) {
                $pid = pcntl_wait($status);
            }
            if (!isset($childMap[$pid])) continue;
            $wCounts = unpack('v*', file_get_contents($childMap[$pid]));
            unlink($childMap[$pid]);
            $j = 0;
            foreach ($wCounts as $v) {
                $counts[$j++] += $v;
            }
            $pending--;
        }

        $this->writeJson($outputPath, $counts, $pathPrefixes, $datePrefixes, $pathCount, $dateCount);
    }

    private function fillBuckets($inputPath, $start, $end, $pathIds, $dateChars, &$buckets)
    {
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);
        $processed = 0;
        $toProcess = $end - $start;

        while ($processed < $toProcess) {
            $remaining = $toProcess - $processed;
            $chunk = fread($handle, $remaining > self::READ_BYTES ? self::READ_BYTES : $remaining);
            if (!$chunk) break;

            $chunkLen = strlen($chunk);
            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) continue;

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
            }
            $processed += $lastNl + 1;

            $p = 25;
            $fence = $lastNl - 600;

            while ($p < $fence) {
                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateChars[substr($chunk, $sep + 4, 7)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateChars[substr($chunk, $sep + 4, 7)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateChars[substr($chunk, $sep + 4, 7)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateChars[substr($chunk, $sep + 4, 7)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateChars[substr($chunk, $sep + 4, 7)];
                $p = $sep + 52;
            }

            while ($p < $lastNl) {
                $sep = strpos($chunk, ',', $p);
                if ($sep === false || $sep >= $lastNl) break;
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateChars[substr($chunk, $sep + 4, 7)];
                $p = $sep + 52;
            }
        }

        fclose($handle);
    }

    private function bucketsToCounts(&$buckets, $pathCount, $dateCount)
    {
        $counts = array_fill(0, $pathCount * $dateCount, 0);
        $base = 0;
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

    private function writeJson(
        $outputPath, $counts, $pathPrefixes,
        $datePrefixes, $pathCount, $dateCount,
    ) {
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

            if ($dateBuf === '') {
                $base += $dateCount;
                continue;
            }

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
}
