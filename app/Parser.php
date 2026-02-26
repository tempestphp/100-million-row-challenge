<?php

namespace App;

use function array_count_values;
use function array_fill;
use function chr;
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
use function is_dir;
use function pack;
use function pcntl_fork;
use function pcntl_waitpid;
use function shell_exec;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function str_replace;
use function strrpos;
use function substr;
use function sys_get_temp_dir;
use function trim;
use function unlink;
use function unpack;

use const SEEK_CUR;

final class Parser
{
    private const PROBE_BYTES = 4_194_304;
    private const READ_CHUNK = 1_048_576;
    private const PARALLEL_THRESHOLD = 10_485_760;
    private const DATE_PAD_DAYS = 14;

    public function parse($inputPath, $outputPath)
    {
        gc_disable();

        $fileSize = filesize($inputPath);

        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);
        $probe = fread($fh, $fileSize > self::PROBE_BYTES ? self::PROBE_BYTES : $fileSize);
        fclose($fh);

        $dateIds = [];
        $dateChars = [];
        $dates = [];
        $dateCount = 0;

        $pathIds = [];
        $paths = [];
        $pathCount = 0;
        $minDate = '9999-99-99';
        $maxDate = '0000-00-00';

        $probeLen = strlen($probe);
        $p = 0;
        while ($p + 52 <= $probeLen) {
            $nl = strpos($probe, "\n", $p + 52);
            if ($nl === false) {
                break;
            }

            $slug = substr($probe, $p + 25, $nl - $p - 51);
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug] = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }

            $date = substr($probe, $nl - 25, 10);
            if ($date < $minDate) {
                $minDate = $date;
            }
            if ($date > $maxDate) {
                $maxDate = $date;
            }

            $p = $nl + 1;
        }
        unset($probe);

        if ($minDate === '9999-99-99') {
            $minDate = '2021-01-01';
            $maxDate = '2026-12-31';
        }

        $startTs = (int) \strtotime($minDate . ' 00:00:00 UTC') - (self::DATE_PAD_DAYS * 86_400);
        $endTs = (int) \strtotime($maxDate . ' 00:00:00 UTC') + (self::DATE_PAD_DAYS * 86_400);

        $minAllowedTs = (int) \strtotime('2020-01-01 00:00:00 UTC');
        $maxAllowedTs = (int) \strtotime('2026-12-31 00:00:00 UTC');

        if ($startTs < $minAllowedTs) {
            $startTs = $minAllowedTs;
        }
        if ($endTs > $maxAllowedTs) {
            $endTs = $maxAllowedTs;
        }

        for ($ts = $startTs; $ts <= $endTs; $ts += 86_400) {
            $full = \gmdate('Y-m-d', $ts);
            $short = substr($full, 2, 8);
            $dateIds[$short] = $dateCount;
            $dateChars[$short] = chr($dateCount & 0xFF) . chr($dateCount >> 8);
            $dates[$dateCount] = $short;
            $dateCount++;
        }

        $stride = $dateCount;
        $totalCells = $pathCount * $stride;

        if ($fileSize < self::PARALLEL_THRESHOLD) {
            $counts = $this->parseSequential(
                $inputPath,
                $fileSize,
                $pathIds,
                $dateIds,
                $pathCount,
                $stride,
                $totalCells,
            );
            $this->writeJson($outputPath, $counts, $paths, $dates, $pathCount, $stride);
            return;
        }

        $numWorkers = (int) trim((string) (shell_exec('nproc 2>/dev/null') ?: '8'));
        if ($numWorkers < 2) {
            $numWorkers = 2;
        } elseif ($numWorkers > 16) {
            $numWorkers = 16;
        }

        $numSegments = $numWorkers * 2;
        $segmentBounds = [0];
        $fh = fopen($inputPath, 'rb');
        for ($s = 1; $s < $numSegments; $s++) {
            fseek($fh, (int) ($fileSize * $s / $numSegments));
            fgets($fh);
            $segmentBounds[] = ftell($fh);
        }
        $segmentBounds[] = $fileSize;
        fclose($fh);

        $tmpDir = sys_get_temp_dir();
        if (is_dir('/dev/shm')) {
            $probe = '/dev/shm/.p33_probe_' . getmypid() . '_' . substr((string) \microtime(true), -6);
            if (@file_put_contents($probe, '') !== false) {
                @unlink($probe);
                $tmpDir = '/dev/shm';
            }
        }
        $tmpPrefix = $tmpDir . '/p33_' . getmypid() . '_';

        $childFiles = [];

        for ($w = 0; $w < $numWorkers; $w++) {
            $tmpFile = $tmpPrefix . $w;
            $pid = pcntl_fork();

            if ($pid === 0) {
                $counts = $this->parseWorkerRanges(
                    $inputPath,
                    $w,
                    $numWorkers,
                    $numSegments,
                    $segmentBounds,
                    $pathIds,
                    $dateChars,
                    $pathCount,
                    $stride,
                    $totalCells,
                );

                file_put_contents($tmpFile, pack('V*', ...$counts));
                exit(0);
            }

            $childFiles[$pid] = $tmpFile;
        }

        $counts = null;
        while ($childFiles !== []) {
            $pid = pcntl_waitpid(-1, $status);
            if (!isset($childFiles[$pid])) {
                continue;
            }

            $blob = file_get_contents($childFiles[$pid]);
            $decoded = unpack('V*', $blob);

            if ($counts === null) {
                $counts = [];
                $j = 0;
                foreach ($decoded as $v) {
                    $counts[$j++] = $v;
                }
            } else {
                $j = 0;
                foreach ($decoded as $v) {
                    $counts[$j++] += $v;
                }
            }

            unlink($childFiles[$pid]);
            unset($childFiles[$pid]);
        }

        if ($counts === null) {
            $counts = array_fill(0, $totalCells, 0);
        }

        $this->writeJson($outputPath, $counts, $paths, $dates, $pathCount, $stride);
    }

    private function parseWorkerRanges(
        $inputPath,
        $workerId,
        $numWorkers,
        $numSegments,
        $segmentBounds,
        $pathIds,
        $dateChars,
        $pathCount,
        $stride,
        $totalCells,
    ) {
        $buckets = array_fill(0, $pathCount, '');

        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);

        for ($segId = $workerId; $segId < $numSegments; $segId += $numWorkers) {
            $start = $segmentBounds[$segId];
            $end = $segmentBounds[$segId + 1];

            fseek($fh, $start);
            $remaining = $end - $start;

            while ($remaining > 0) {
                $raw = fread($fh, $remaining > self::READ_CHUNK ? self::READ_CHUNK : $remaining);
                $len = strlen($raw);
                if ($len === 0) {
                    break;
                }

                $remaining -= $len;

                $lastNl = strrpos($raw, "\n");
                if ($lastNl === false) {
                    break;
                }

                $tail = $len - $lastNl - 1;
                if ($tail > 0) {
                    fseek($fh, -$tail, SEEK_CUR);
                    $remaining += $tail;
                }

                $p = 0;
                $fence = $lastNl - 1024;

                while ($p < $fence) {
                    $nl = strpos($raw, "\n", $p + 52);
                    $buckets[$pathIds[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateChars[substr($raw, $nl - 23, 8)];
                    $p = $nl + 1;

                    $nl = strpos($raw, "\n", $p + 52);
                    $buckets[$pathIds[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateChars[substr($raw, $nl - 23, 8)];
                    $p = $nl + 1;

                    $nl = strpos($raw, "\n", $p + 52);
                    $buckets[$pathIds[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateChars[substr($raw, $nl - 23, 8)];
                    $p = $nl + 1;

                    $nl = strpos($raw, "\n", $p + 52);
                    $buckets[$pathIds[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateChars[substr($raw, $nl - 23, 8)];
                    $p = $nl + 1;

                    $nl = strpos($raw, "\n", $p + 52);
                    $buckets[$pathIds[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateChars[substr($raw, $nl - 23, 8)];
                    $p = $nl + 1;

                    $nl = strpos($raw, "\n", $p + 52);
                    $buckets[$pathIds[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateChars[substr($raw, $nl - 23, 8)];
                    $p = $nl + 1;

                    $nl = strpos($raw, "\n", $p + 52);
                    $buckets[$pathIds[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateChars[substr($raw, $nl - 23, 8)];
                    $p = $nl + 1;

                    $nl = strpos($raw, "\n", $p + 52);
                    $buckets[$pathIds[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateChars[substr($raw, $nl - 23, 8)];
                    $p = $nl + 1;
                }

                while ($p < $lastNl) {
                    $search = $p + 52;
                    if ($search > $lastNl) {
                        break;
                    }

                    $nl = strpos($raw, "\n", $search);
                    if ($nl === false) {
                        break;
                    }

                    $buckets[$pathIds[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateChars[substr($raw, $nl - 23, 8)];
                    $p = $nl + 1;
                }
            }
        }

        fclose($fh);

        $counts = array_fill(0, $totalCells, 0);

        for ($pathId = 0; $pathId < $pathCount; $pathId++) {
            if ($buckets[$pathId] === '') {
                continue;
            }

            $base = $pathId * $stride;

            foreach (array_count_values(unpack('v*', $buckets[$pathId])) as $dateId => $n) {
                $counts[$base + $dateId] += $n;
            }
        }

        return $counts;
    }

    private function parseSequential(
        $inputPath,
        $fileSize,
        $pathIds,
        $dateIds,
        $pathCount,
        $stride,
        $totalCells,
    ) {
        $pathOffsets = [];
        foreach ($pathIds as $path => $id) {
            $pathOffsets[$path] = $id * $stride;
        }

        $counts = array_fill(0, $totalCells, 0);

        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);

        $remaining = $fileSize;
        while ($remaining > 0) {
            $raw = fread($fh, $remaining > self::READ_CHUNK ? self::READ_CHUNK : $remaining);
            $len = strlen($raw);
            if ($len === 0) {
                break;
            }

            $remaining -= $len;

            $lastNl = strrpos($raw, "\n");
            if ($lastNl === false) {
                break;
            }

            $tail = $len - $lastNl - 1;
            if ($tail > 0) {
                fseek($fh, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $p = 0;
            while ($p < $lastNl) {
                $search = $p + 52;
                if ($search > $lastNl) {
                    break;
                }

                $nl = strpos($raw, "\n", $search);
                if ($nl === false) {
                    break;
                }

                $counts[$pathOffsets[substr($raw, $p + 25, $nl - $p - 51)] + $dateIds[substr($raw, $nl - 23, 8)]]++;
                $p = $nl + 1;
            }
        }

        fclose($fh);

        return $counts;
    }

    private function writeJson($outputPath, $counts, $paths, $dates, $pathCount, $dateCount)
    {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);

        $datePrefixes = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "20' . $dates[$d] . '": ';
        }

        $jsonPaths = [];
        for ($p = 0; $p < $pathCount; $p++) {
            $jsonPaths[$p] = '"\\/blog\\/' . str_replace('/', '\\/', $paths[$p]) . '"';
        }

        fwrite($out, '{');
        $first = true;

        for ($p = 0; $p < $pathCount; $p++) {
            $base = $p * $dateCount;
            $body = '';
            $sep = '';

            for ($d = 0; $d < $dateCount; $d++) {
                $n = $counts[$base + $d];
                if ($n === 0) {
                    continue;
                }

                $body .= $sep . $datePrefixes[$d] . $n;
                $sep = ",\n";
            }

            if ($body === '') {
                continue;
            }

            fwrite(
                $out,
                ($first ? '' : ',')
                . "\n    " . $jsonPaths[$p] . ": {\n"
                . $body
                . "\n    }"
            );

            $first = false;
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
