<?php

namespace App;

use function array_fill;
use function ceil;
use function date;
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
use function ksort;
use function min;
use function pack;
use function pcntl_fork;
use function pcntl_waitpid;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function sys_get_temp_dir;
use function time;
use function unlink;
use function unpack;

final class Parser
{
    // Number of parallel worker processes (one per core on M1)
    private const int THREADS = 8;

    // strlen('https://stitcher.io/blog/') — fixed URL prefix skipped in hot loop
    private const int URL_PREFIX_LEN = 25;

    // fread chunk size in bytes — tune this for M1 memory bandwidth
    private const int BUFFER_SIZE = 2_097_152;

    // strlen('yyyy-mm-dd') — date portion of the flat key
    private const int DATE_LEN = 10;

    // Offset from commaPos to the start of the next line:
    // comma(1) + datetime(25) + \n(1) = 27
    private const int LINE_ADVANCE = 27;

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $boundaries = $this->calculateBoundaries($inputPath);
        $urls = $this->discoverUrls($inputPath);
        [$dateMap, $dateIndex, $dateCount] = $this->buildDateMap();
        $pathMap = $this->buildPathMap($urls, $dateCount);
        $pathCount = count($urls);
        $parentPid = getmypid();
        $childPids = [];

        for ($i = 0; $i < self::THREADS; $i++) {
            $childPid = pcntl_fork();
            if ($childPid === -1)
                exit('Fork failed');

            if ($childPid === 0) {
                gc_disable();
                [$start, $end] = $boundaries[$i];
                $counts = $this->processChunk($inputPath, $start, $end, $pathMap, $dateMap, $pathCount, $dateCount);
                file_put_contents(sys_get_temp_dir()."/csv_{$parentPid}_{$i}.dat", pack('V*', ...$counts));
                exit(0);
            }

            $childPids[] = $childPid;
        }

        foreach ($childPids as $childPid) {
            pcntl_waitpid($childPid, $status);
        }

        $totals = $this->mergePartials($parentPid, $pathCount, $dateCount);
        $result = $this->buildOutput($totals, $urls, $dateIndex, $dateCount);
        $this->writeJson($outputPath, $result);
    }

    private function calculateBoundaries(string $inputPath): array
    {
        $filesize = filesize($inputPath);
        $chunkSize = (int) ceil($filesize / self::THREADS);
        $boundaries = [];
        $start = 0;

        $fp = fopen($inputPath, 'rb');
        stream_set_read_buffer($fp, 0);

        for ($i = 0; $i < (self::THREADS - 1); $i++) {
            fseek($fp, $start + $chunkSize);
            fgets($fp);
            $end = ftell($fp);
            $boundaries[] = [$start, $end];
            $start = $end;
        }

        $boundaries[] = [$start, $filesize];
        fclose($fp);

        return $boundaries;
    }

    private function discoverUrls(string $inputPath): array
    {
        $fp = fopen($inputPath, 'rb');
        stream_set_read_buffer($fp, 0);

        $discovered = [];
        $prevCount = -1;

        while (true) {
            $chunk = fread($fp, self::BUFFER_SIZE);
            if ($chunk === false || $chunk === '')
                break;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false)
                continue;

            $pos = 0;
            while ($pos < $lastNl) {
                $comma = strpos($chunk, ',', $pos + self::URL_PREFIX_LEN);
                if ($comma === false)
                    break;
                $discovered[substr($chunk, $pos + self::URL_PREFIX_LEN, $comma - $pos - self::URL_PREFIX_LEN)] = true;
                $pos = $comma + self::LINE_ADVANCE;
            }

            // Stop once a full chunk passes with no new slugs
            $currentCount = count($discovered);
            if ($currentCount === $prevCount)
                break;
            $prevCount = $currentCount;
        }

        fclose($fp);

        return array_keys($discovered);
    }

    private function buildDateMap(): array
    {
        $dateMap = [];
        $dateIndex = [];
        $count = 0;

        $from = time() - (6 * 365 * 86400);
        $to = time() + 86400;

        for ($ts = $from; $ts <= $to; $ts += 86400) {
            $date = date('Y-m-d', $ts);
            if (isset($dateMap[$date]))
                continue;

            $dateMap[$date] = $count;
            $dateIndex[$count] = $date;
            $count++;
        }

        return [$dateMap, $dateIndex, $count];
    }

    private function buildPathMap(array $urls, int $dateCount): array
    {
        $pathMap = [];
        foreach ($urls as $id => $slug) {
            $pathMap[$slug] = $id * $dateCount;
        }

        return $pathMap;
    }

    private function processChunk(
        string $inputPath,
        int $start,
        int $end,
        array $pathMap,
        array $dateMap,
        int $pathCount,
        int $dateCount,
    ): array {
        $fp = fopen($inputPath, 'rb');
        stream_set_read_buffer($fp, 0);
        fseek($fp, $start);

        $remaining = $end - $start;
        $counts = array_fill(0, $pathCount * $dateCount, 0);

        while ($remaining > 0) {
            $chunk = fread($fp, min(self::BUFFER_SIZE, $remaining));
            if ($chunk === false || $chunk === '')
                break;

            $lastNewline = strrpos($chunk, "\n");
            $tail = strlen($chunk) - $lastNewline - 1;
            if ($tail > 0)
                fseek($fp, -$tail, SEEK_CUR);
            $remaining -= $lastNewline + 1;

            $pos = 0;
            while ($pos < $lastNewline) {
                $comma = strpos($chunk, ',', $pos + self::URL_PREFIX_LEN);
                $slug = substr($chunk, $pos + self::URL_PREFIX_LEN, $comma - $pos - self::URL_PREFIX_LEN);
                $date = substr($chunk, $comma + 1, self::DATE_LEN);
                $counts[$pathMap[$slug] + $dateMap[$date]]++;
                $pos = $comma + self::LINE_ADVANCE;
            }
        }

        fclose($fp);

        return $counts;
    }

    private function mergePartials(int $parentPid, int $pathCount, int $dateCount): array
    {
        $totals = array_fill(0, $pathCount * $dateCount, 0);
        $tmpDir = sys_get_temp_dir();

        for ($i = 0; $i < self::THREADS; $i++) {
            $file = "{$tmpDir}/csv_{$parentPid}_{$i}.dat";
            $partial = unpack('V*', file_get_contents($file));
            unlink($file);
            foreach ($partial as $j => $count) {
                $totals[$j - 1] += $count;
            }
        }

        return $totals;
    }

    private function buildOutput(array $totals, array $urls, array $dateIndex, int $dateCount): array
    {
        $result = [];

        foreach ($urls as $pathId => $slug) {
            $base = $pathId * $dateCount;
            $dates = [];

            for ($d = 0; $d < $dateCount; $d++) {
                if ($totals[$base + $d] > 0) {
                    $dates[$dateIndex[$d]] = $totals[$base + $d];
                }
            }

            if ($dates !== []) {
                ksort($dates);
                $result[$slug] = $dates;
            }
        }

        return $result;
    }

    private function writeJson(string $outputPath, array $result): void
    {
        $fh = fopen($outputPath, 'wb');
        stream_set_write_buffer($fh, 0);
        $buf = "{\n";
        $firstPath = true;

        foreach ($result as $path => $dates) {
            if (! $firstPath)
                $buf .= ",\n";
            $firstPath = false;

            $buf .= "    \"\/blog\/$path\": {\n";
            $firstDate = true;

            foreach ($dates as $date => $count) {
                if (! $firstDate)
                    $buf .= ",\n";
                $firstDate = false;
                $buf .= "        \"$date\": $count";
            }

            $buf .= "\n    }";

            if (strlen($buf) > 65536) {
                fwrite($fh, $buf);
                $buf = '';
            }
        }

        fwrite($fh, $buf."\n}");
        fclose($fh);
    }
}
