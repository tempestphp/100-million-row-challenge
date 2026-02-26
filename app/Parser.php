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
use function pcntl_waitpid;
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
    public function parse($inputPath, $outputPath)
    {
        gc_disable();

        $fileSize = filesize($inputPath);

        $dateChars = [];
        $dates = [];
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
                    $key = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $dateChars[$key] = chr($dateCount & 0xFF) . chr($dateCount >> 8);
                    $dates[$dateCount] = $key;
                    $dateCount++;
                }
            }
        }

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $sample = fread($handle, $fileSize > 2_097_152 ? 2_097_152 : $fileSize);
        fclose($handle);

        $pathIds = [];
        $paths = [];
        $pathCount = 0;

        $lastNl = strrpos($sample, "\n");
        if ($lastNl !== false) {
            $pos = 0;
            while ($pos < $lastNl) {
                $nl = strpos($sample, "\n", $pos + 52);
                if ($nl === false) break;
                $slug = substr($sample, $pos + 25, $nl - $pos - 51);
                if (!isset($pathIds[$slug])) {
                    $pathIds[$slug] = $pathCount;
                    $paths[$pathCount] = $slug;
                    $pathCount++;
                }
                $pos = $nl + 1;
            }
        }
        unset($sample);

        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, 25);
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug] = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }
        }

        $numWorkers = 12;
        $chunkSize = 4_194_304;

        $splits = [0];
        $handle = fopen($inputPath, 'rb');
        for ($w = 1; $w < $numWorkers; $w++) {
            fseek($handle, (int) ($fileSize * $w / $numWorkers));
            fgets($handle);
            $splits[] = ftell($handle);
        }
        $splits[] = $fileSize;
        fclose($handle);

        $tmpDir = sys_get_temp_dir();
        $tmpPrefix = $tmpDir . '/p_' . getmypid() . '_';
        $totalCells = $pathCount * $dateCount;

        $children = [];
        for ($w = 0; $w < $numWorkers - 1; $w++) {
            $pid = pcntl_fork();
            if ($pid === -1) continue;
            if ($pid === 0) {
                $buckets = array_fill(0, $pathCount, '');
                $handle = fopen($inputPath, 'rb');
                stream_set_read_buffer($handle, 0);
                fseek($handle, $splits[$w]);
                $remaining = $splits[$w + 1] - $splits[$w];

                while ($remaining > 0) {
                    $chunk = fread($handle, $remaining > $chunkSize ? $chunkSize : $remaining);
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

                    $pos = 0;
                    $fence = $lastNl - 720;

                    while ($pos < $fence) {
                        $nl = strpos($chunk, "\n", $pos + 52);
                        $buckets[$pathIds[substr($chunk, $pos + 25, $nl - $pos - 51)]] .= $dateChars[substr($chunk, $nl - 23, 8)];
                        $pos = $nl + 1;

                        $nl = strpos($chunk, "\n", $pos + 52);
                        $buckets[$pathIds[substr($chunk, $pos + 25, $nl - $pos - 51)]] .= $dateChars[substr($chunk, $nl - 23, 8)];
                        $pos = $nl + 1;

                        $nl = strpos($chunk, "\n", $pos + 52);
                        $buckets[$pathIds[substr($chunk, $pos + 25, $nl - $pos - 51)]] .= $dateChars[substr($chunk, $nl - 23, 8)];
                        $pos = $nl + 1;

                        $nl = strpos($chunk, "\n", $pos + 52);
                        $buckets[$pathIds[substr($chunk, $pos + 25, $nl - $pos - 51)]] .= $dateChars[substr($chunk, $nl - 23, 8)];
                        $pos = $nl + 1;

                        $nl = strpos($chunk, "\n", $pos + 52);
                        $buckets[$pathIds[substr($chunk, $pos + 25, $nl - $pos - 51)]] .= $dateChars[substr($chunk, $nl - 23, 8)];
                        $pos = $nl + 1;

                        $nl = strpos($chunk, "\n", $pos + 52);
                        $buckets[$pathIds[substr($chunk, $pos + 25, $nl - $pos - 51)]] .= $dateChars[substr($chunk, $nl - 23, 8)];
                        $pos = $nl + 1;
                    }
                    while ($pos < $lastNl) {
                        $nl = strpos($chunk, "\n", $pos + 52);
                        if ($nl === false) break;
                        $buckets[$pathIds[substr($chunk, $pos + 25, $nl - $pos - 51)]] .= $dateChars[substr($chunk, $nl - 23, 8)];
                        $pos = $nl + 1;
                    }
                }

                fclose($handle);

                $counts = array_fill(0, $totalCells, 0);
                for ($p = 0; $p < $pathCount; $p++) {
                    if ($buckets[$p] === '') continue;
                    $offset = $p * $dateCount;
                    foreach (array_count_values(unpack('v*', $buckets[$p])) as $did => $cnt) {
                        $counts[$offset + $did] += $cnt;
                    }
                }

                $fh = fopen($tmpPrefix . $w, 'wb');
                $batch = [];
                $batchCount = 0;
                foreach ($counts as $value) {
                    $batch[] = $value;
                    if (++$batchCount === 8192) {
                        fwrite($fh, pack('V*', ...$batch));
                        $batch = [];
                        $batchCount = 0;
                    }
                }
                if ($batchCount > 0) {
                    fwrite($fh, pack('V*', ...$batch));
                }
                fclose($fh);
                exit(0);
            }
            $children[$pid] = $w;
        }

        $buckets = array_fill(0, $pathCount, '');
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $splits[$numWorkers - 1]);
        $remaining = $splits[$numWorkers] - $splits[$numWorkers - 1];

        while ($remaining > 0) {
            $chunk = fread($handle, $remaining > $chunkSize ? $chunkSize : $remaining);
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

            $pos = 0;
            $fence = $lastNl - 720;

            while ($pos < $fence) {
                $nl = strpos($chunk, "\n", $pos + 52);
                $buckets[$pathIds[substr($chunk, $pos + 25, $nl - $pos - 51)]] .= $dateChars[substr($chunk, $nl - 23, 8)];
                $pos = $nl + 1;

                $nl = strpos($chunk, "\n", $pos + 52);
                $buckets[$pathIds[substr($chunk, $pos + 25, $nl - $pos - 51)]] .= $dateChars[substr($chunk, $nl - 23, 8)];
                $pos = $nl + 1;

                $nl = strpos($chunk, "\n", $pos + 52);
                $buckets[$pathIds[substr($chunk, $pos + 25, $nl - $pos - 51)]] .= $dateChars[substr($chunk, $nl - 23, 8)];
                $pos = $nl + 1;

                $nl = strpos($chunk, "\n", $pos + 52);
                $buckets[$pathIds[substr($chunk, $pos + 25, $nl - $pos - 51)]] .= $dateChars[substr($chunk, $nl - 23, 8)];
                $pos = $nl + 1;

                $nl = strpos($chunk, "\n", $pos + 52);
                $buckets[$pathIds[substr($chunk, $pos + 25, $nl - $pos - 51)]] .= $dateChars[substr($chunk, $nl - 23, 8)];
                $pos = $nl + 1;

                $nl = strpos($chunk, "\n", $pos + 52);
                $buckets[$pathIds[substr($chunk, $pos + 25, $nl - $pos - 51)]] .= $dateChars[substr($chunk, $nl - 23, 8)];
                $pos = $nl + 1;
            }
            while ($pos < $lastNl) {
                $nl = strpos($chunk, "\n", $pos + 52);
                if ($nl === false) break;
                $buckets[$pathIds[substr($chunk, $pos + 25, $nl - $pos - 51)]] .= $dateChars[substr($chunk, $nl - 23, 8)];
                $pos = $nl + 1;
            }
        }
        fclose($handle);

        $counts = array_fill(0, $totalCells, 0);
        for ($p = 0; $p < $pathCount; $p++) {
            if ($buckets[$p] === '') continue;
            $offset = $p * $dateCount;
            foreach (array_count_values(unpack('v*', $buckets[$p])) as $did => $cnt) {
                $counts[$offset + $did] += $cnt;
            }
        }
        unset($buckets);

        $pendingW = count($children);
        while ($pendingW > 0) {
            $pid = pcntl_waitpid(-1, $status, WNOHANG);
            if ($pid <= 0) {
                $pid = pcntl_waitpid(-1, $status);
            }
            $w = $children[$pid];
            $j = 0;
            foreach (unpack('V*', file_get_contents($tmpPrefix . $w)) as $v) {
                $counts[$j++] += $v;
            }
            unlink($tmpPrefix . $w);
            $pendingW--;
        }

        $datePrefixes = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "20' . $dates[$d] . '": ';
        }

        $escapedPaths = [];
        for ($p = 0; $p < $pathCount; $p++) {
            $escapedPaths[$p] = '"\\/blog\\/' . str_replace('/', '\\/', $paths[$p]) . '"';
        }

        $fp = fopen($outputPath, 'wb');
        stream_set_write_buffer($fp, 1_048_576);
        fwrite($fp, '{');
        $firstPath = true;

        for ($p = 0; $p < $pathCount; $p++) {
            $base = $p * $dateCount;
            $buf = '';
            $sep = '';

            for ($d = 0; $d < $dateCount; $d++) {
                $n = $counts[$base + $d];
                if ($n === 0) continue;
                $buf .= $sep . $datePrefixes[$d] . $n;
                $sep = ",\n";
            }

            if ($buf === '') continue;

            fwrite($fp, ($firstPath ? '' : ',') . "\n    " . $escapedPaths[$p] . ": {\n" . $buf . "\n    }");
            $firstPath = false;
        }

        fwrite($fp, "\n}");
        fclose($fp);
    }
}
