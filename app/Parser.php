<?php

namespace App;

use function array_fill;
use function ceil;
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
use function unlink;
use function unpack;

final class Parser
{
    // Number of parallel worker processes (one per core on M1)
    private const int THREADS = 16;

    // strlen('https://stitcher.io/blog/') — fixed URL prefix skipped in hot loop
    private const int URL_PREFIX_LEN = 25;

    // fread chunk size in bytes — tune this for M1 memory bandwidth
    private const int BUFFER_SIZE = 4_194_304;

    // From comma position to start of slug on next line:
    // comma(1) + datetime(25) + \n(1) + prefix(25) = 52
    private const int STRIDE = 52;

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $boundaries = $this->calculateBoundaries($inputPath);
        $urls = $this->discoverUrls($inputPath);
        [$dateMap, $dateIndex, $dateCount] = $this->buildDateMap();
        $pathMap = $this->buildPathMap($urls, $dateCount);
        $pathCount = count($urls);
        $parentPid = getmypid();
        $childPidToIndex = [];
        $tmpDir = sys_get_temp_dir();

        // Fork children for chunks 1..THREADS-1; parent handles chunk 0 directly
        for ($i = 1; $i < self::THREADS; $i++) {
            $childPid = pcntl_fork();
            if ($childPid === -1)
                exit('Fork failed');

            if ($childPid === 0) {
                gc_disable();
                [$start, $end] = $boundaries[$i];
                $counts = $this->processChunk($inputPath, $start, $end, $pathMap, $dateMap, $pathCount, $dateCount);
                file_put_contents("{$tmpDir}/csv_{$parentPid}_{$i}.dat", pack('V*', ...$counts));
                exit(0);
            }

            $childPidToIndex[$childPid] = $i;
        }

        // Parent crunches chunk 0 while children run in parallel
        [$start, $end] = $boundaries[0];
        $tally = $this->processChunk($inputPath, $start, $end, $pathMap, $dateMap, $pathCount, $dateCount);

        $pending = self::THREADS - 1;
        while ($pending > 0) {
            $pid = pcntl_waitpid(-1, $status); // -1 = get the first to finish
            if ($pid > 0) {
                $i = $childPidToIndex[$pid];
                $file = "{$tmpDir}/csv_{$parentPid}_{$i}.dat";
                $raw = file_get_contents($file);
                unlink($file);
                $j = 0;
                foreach (unpack('V*', $raw) as $v) {
                    $tally[$j++] += $v;
                }
                $pending--;
            }
        }

        $result = $this->buildOutput($tally, $urls, $dateIndex, $dateCount);
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
                $pos = $comma + 27; // comma(1) + datetime(25) + \n(1)
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

        $daysInMonth = [
            '2020-02' => 29,
            '2020-03' => 31,
            '2020-04' => 30,
            '2020-05' => 31,
            '2020-06' => 30,
            '2020-07' => 31,
            '2020-08' => 31,
            '2020-09' => 30,
            '2020-10' => 31,
            '2020-11' => 30,
            '2020-12' => 31,
            '2021-01' => 31,
            '2021-02' => 28,
            '2021-03' => 31,
            '2021-04' => 30,
            '2021-05' => 31,
            '2021-06' => 30,
            '2021-07' => 31,
            '2021-08' => 31,
            '2021-09' => 30,
            '2021-10' => 31,
            '2021-11' => 30,
            '2021-12' => 31,
            '2022-01' => 31,
            '2022-02' => 28,
            '2022-03' => 31,
            '2022-04' => 30,
            '2022-05' => 31,
            '2022-06' => 30,
            '2022-07' => 31,
            '2022-08' => 31,
            '2022-09' => 30,
            '2022-10' => 31,
            '2022-11' => 30,
            '2022-12' => 31,
            '2023-01' => 31,
            '2023-02' => 28,
            '2023-03' => 31,
            '2023-04' => 30,
            '2023-05' => 31,
            '2023-06' => 30,
            '2023-07' => 31,
            '2023-08' => 31,
            '2023-09' => 30,
            '2023-10' => 31,
            '2023-11' => 30,
            '2023-12' => 31,
            '2024-01' => 31,
            '2024-02' => 29,
            '2024-03' => 31,
            '2024-04' => 30,
            '2024-05' => 31,
            '2024-06' => 30,
            '2024-07' => 31,
            '2024-08' => 31,
            '2024-09' => 30,
            '2024-10' => 31,
            '2024-11' => 30,
            '2024-12' => 31,
            '2025-01' => 31,
            '2025-02' => 28,
            '2025-03' => 31,
            '2025-04' => 30,
            '2025-05' => 31,
            '2025-06' => 30,
            '2025-07' => 31,
            '2025-08' => 31,
            '2025-09' => 30,
            '2025-10' => 31,
            '2025-11' => 30,
            '2025-12' => 31,
            '2026-01' => 31,
            '2026-02' => 28,
        ];

        foreach ($daysInMonth as $prefix => $dim) {
            for ($d = 1; $d <= $dim; $d++) {
                $date = $prefix.'-'.($d < 10 ? '0'.$d : (string) $d);
                $dateMap[substr($date, 3)] = $count; // 7-char key: skip leading "202"
                $dateIndex[$count] = $date; // full date for output
                $count++;
            }
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

            // $p points past the URL prefix, no per-row +25 offset needed
            $p = self::URL_PREFIX_LEN;
            $fence = $lastNewline - 104; // 2 × stride safety margin for unrolled loop

            while ($p < $fence) {
                $sep = strpos($chunk, ',', $p);
                $counts[$pathMap[substr($chunk, $p, $sep - $p)] + $dateMap[substr($chunk, $sep + 4, 7)]]++;
                $p = $sep + self::STRIDE;

                $sep = strpos($chunk, ',', $p);
                $counts[$pathMap[substr($chunk, $p, $sep - $p)] + $dateMap[substr($chunk, $sep + 4, 7)]]++;
                $p = $sep + self::STRIDE;
            }

            while ($p < $lastNewline) {
                $sep = strpos($chunk, ',', $p);
                if ($sep === false)
                    break;
                $counts[$pathMap[substr($chunk, $p, $sep - $p)] + $dateMap[substr($chunk, $sep + 4, 7)]]++;
                $p = $sep + self::STRIDE;
            }
        }

        fclose($fp);

        return $counts;
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
