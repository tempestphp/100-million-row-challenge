<?php

declare(strict_types=1);

namespace App;

use function strpos;
use function strrpos;
use function substr;
use function strlen;
use function fopen;
use function fclose;
use function fread;
use function fseek;
use function ftell;
use function fgets;
use function fwrite;
use function filesize;
use function getmypid;
use function pcntl_fork;
use function pcntl_waitpid;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function array_fill;
use function array_keys;
use function asort;
use function pack;
use function unpack;
use function file_put_contents;
use function file_get_contents;
use function unlink;
use function min;
use function is_dir;
use function sys_get_temp_dir;
use function implode;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);

        $pathIds = [];
        $paths = [];
        $dateIds = [];
        $dates = [];
        $numPaths = 0;
        $numDates = 0;

        $handle = fopen($inputPath, 'rb');
        $sample = fread($handle, min($fileSize, 8388608));
        fclose($handle);

        $lastNl = strrpos($sample, "\n");
        if ($lastNl === false) $lastNl = strlen($sample);

        $pos = 0;
        while ($pos < $lastNl) {
            $nl = strpos($sample, "\n", $pos + 55);
            if ($nl === false) break;

            $path = substr($sample, $pos + 25, $nl - $pos - 51);
            if (!isset($pathIds[$path])) {
                $pathIds[$path] = $numPaths;
                $paths[$numPaths] = $path;
                $numPaths++;
            }

            $date = substr($sample, $nl - 23, 8);
            if (!isset($dateIds[$date])) {
                $dateIds[$date] = $numDates;
                $dates[$numDates] = $date;
                $numDates++;
            }

            $pos = $nl + 1;
        }
        unset($sample);

        $fast = [];
        foreach ($paths as $id => $path) {
            $len = strlen($path);
            $first = $path[0];
            $last = $path[$len - 1];
            if (!isset($fast[$len][$first][$last])) {
                $fast[$len][$first][$last] = $id;
            } else {
                $fast[$len][$first][$last] = -1;
            }
        }

        $bounds = [0];
        $handle = fopen($inputPath, 'rb');
        for ($i = 1; $i < 4; $i++) {
            fseek($handle, (int)($i * $fileSize / 4));
            fgets($handle);
            $bounds[] = ftell($handle);
        }
        $bounds[] = $fileSize;
        fclose($handle);

        $stride = $numDates;
        $total = $numPaths * $stride;
        $tmpDir = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
        $pid = getmypid();
        $children = [];
        $tmpFiles = [];

        for ($w = 1; $w < 4; $w++) {
            $tmpFile = $tmpDir . '/parse_' . $pid . '_' . $w;
            $tmpFiles[$w] = $tmpFile;

            $child = pcntl_fork();
            if ($child === 0) {
                $counts = $this->processChunk(
                    $inputPath, $bounds[$w], $bounds[$w + 1],
                    $pathIds, $dateIds, $fast, $total, $stride
                );
                file_put_contents($tmpFile, pack('V*', ...$counts));
                exit(0);
            }
            $children[$w] = $child;
        }

        $counts = $this->processChunk(
            $inputPath, $bounds[0], $bounds[1],
            $pathIds, $dateIds, $fast, $total, $stride
        );

        foreach ($children as $w => $child) {
            pcntl_waitpid($child, $status);
            $workerCounts = unpack('V*', file_get_contents($tmpFiles[$w]));
            @unlink($tmpFiles[$w]);
            $j = 0;
            foreach ($workerCounts as $val) {
                $counts[$j++] += $val;
            }
        }

        $sortedDates = $dates;
        asort($sortedDates);
        $dateOrder = array_keys($sortedDates);

        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1048576);

        fwrite($out, '{');
        $first = true;
        foreach ($paths as $pathId => $path) {
            $buf = $first ? '' : ',';
            $first = false;
            $buf .= "\n    \"\/blog\/{$path}\": {";

            $entries = [];
            $base = $pathId * $stride;
            foreach ($dateOrder as $dateId) {
                $count = $counts[$base + $dateId];
                if ($count === 0) continue;
                $entries[] = "        \"20{$sortedDates[$dateId]}\": {$count}";
            }

            $buf .= "\n" . implode(",\n", $entries) . "\n    }";
            fwrite($out, $buf);
        }
        fwrite($out, "\n}");
        fclose($out);
    }

    private function processChunk(
        string $inputPath,
        int $start,
        int $end,
        array $pathIds,
        array $dateIds,
        array $fast,
        int $total,
        int $stride
    ): array {
        $counts = array_fill(0, $total, 0);

        $handle = fopen($inputPath, 'rb');
        fseek($handle, $start);
        stream_set_read_buffer($handle, 0);

        $remaining = $end - $start;

        while ($remaining > 0) {
            $chunk = fread($handle, $remaining > 1048576 ? 1048576 : $remaining);
            if ($chunk === '' || $chunk === false) break;
            $len = strlen($chunk);
            $remaining -= $len;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) {
                $lastNl = $len;
            } else {
                $excess = $len - $lastNl - 1;
                if ($excess > 0) {
                    fseek($handle, -$excess, SEEK_CUR);
                    $remaining += $excess;
                }
            }

            $pos = 0;
            while ($pos < $lastNl) {
                $nl = strpos($chunk, "\n", $pos + 55);
                if ($nl === false) break;

                $pathId = $fast[$nl - $pos - 51][$chunk[$pos + 25]][$chunk[$nl - 27]] ?? -1;
                if ($pathId < 0) {
                    $pathId = $pathIds[substr($chunk, $pos + 25, $nl - $pos - 51)] ?? -1;
                }

                $dateId = $dateIds[substr($chunk, $nl - 23, 8)] ?? -1;

                if ($pathId >= 0 && $dateId >= 0) {
                    $counts[$pathId * $stride + $dateId]++;
                }

                $pos = $nl + 1;
            }
        }
        fclose($handle);

        return $counts;
    }
}
