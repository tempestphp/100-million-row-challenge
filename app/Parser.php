<?php

namespace App;

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
use function getmypid;
use function igbinary_serialize;
use function igbinary_unserialize;
use function intdiv;
use function ksort;
use function min;
use function pcntl_fork;
use function pcntl_waitpid;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function sys_get_temp_dir;
use function unlink;

final class Parser
{
    // Number of parallel worker processes (one per core on M1)
    private const THREADS = 8;

    // strlen('https://stitcher.io/blog/') — fixed URL prefix skipped in hot loop
    private const URL_PREFIX_LEN = 25;

    // fread chunk size in bytes — tune this for M1 memory bandwidth
    private const BUFFER_SIZE = 2_097_152;

    // strlen('yyyy-mm-dd') — date portion of the flat key
    private const DATE_LEN = 10;

    // strlen(',yyyy-mm-dd') — key suffix: comma + date appended to slug in hot loop
    private const DATE_KEY_LEN = 11;

    // Offset from commaPos to the start of the next line:
    // comma(1) + datetime(25) + \n(1) = 27
    private const LINE_ADVANCE = 27;

    // Multiplier for integer key: pathId * PATH_ID_MULTIPLIER + dateId
    // Must exceed the max number of unique dates (~1825); 2000 gives headroom
    private const PATH_ID_MULTIPLIER = 2000;

    public function parse(string $inputPath, string $outputPath): void
    {
        $filesize = filesize($inputPath);
        $tmpDir = sys_get_temp_dir();
        $uid = getmypid();

        $chunkSize = (int) ceil($filesize / self::THREADS);
        $pids = [];

        for ($i = 0; $i < self::THREADS; $i++) {
            $pid = pcntl_fork();
            if ($pid === -1)
                exit('Fork failed');

            if ($pid === 0) {
                // --- CHILD PROCESS ---
                $startByte = $i * $chunkSize;
                $endByte = min(($i + 1) * $chunkSize, $filesize);

                $fp = fopen($inputPath, 'rb');
                fseek($fp, $startByte);
                if ($i > 0) {
                    fseek($fp, $startByte - 1);
                    if (fread($fp, 1) !== "\n")
                        fgets($fp); // skip partial line only if mid-line
                }

                $bytesRemaining = $endByte - ftell($fp);
                $results = [];
                $slugMap = []; // slug string → int id
                $dateMap = []; // date string → int id
                $slugRevMap = []; // int id → slug string
                $dateRevMap = []; // int id → date string
                $slugCount = 0;
                $dateCount = 0;
                $buffer = '';

                while ($bytesRemaining > 0) {
                    $chunk = fread($fp, min(self::BUFFER_SIZE, $bytesRemaining));
                    if ($chunk === false || $chunk === '')
                        break;
                    $bytesRemaining -= strlen($chunk);

                    if ($buffer !== '') {
                        $chunk = $buffer.$chunk;
                        $buffer = '';
                    }

                    $lastNl = strrpos($chunk, "\n");
                    if ($lastNl === false) {
                        $buffer = $chunk;
                        continue;
                    }

                    if ($lastNl < (strlen($chunk) - 1)) {
                        $buffer = substr($chunk, $lastNl + 1);
                    }

                    // All rows: https://stitcher.io/blog/SLUG,yyyy-mm-ddT00:00:00+00:00
                    // Skip URL_PREFIX_LEN chars; integer key = pathId * PATH_ID_MULTIPLIER + dateId
                    // Next line is always at commaPos + LINE_ADVANCE
                    $pos = 0;
                    while ($pos < $lastNl) {
                        $commaPos = strpos($chunk, ',', $pos + self::URL_PREFIX_LEN);
                        $slug = substr($chunk, $pos + self::URL_PREFIX_LEN, $commaPos - $pos - self::URL_PREFIX_LEN);
                        $date = substr($chunk, $commaPos + 1, self::DATE_LEN);

                        if (! isset($slugMap[$slug])) {
                            $slugRevMap[$slugCount] = $slug;
                            $slugMap[$slug] = $slugCount++;
                        }
                        if (! isset($dateMap[$date])) {
                            $dateRevMap[$dateCount] = $date;
                            $dateMap[$date] = $dateCount++;
                        }

                        $intKey = ($slugMap[$slug] * self::PATH_ID_MULTIPLIER) + $dateMap[$date];
                        if (isset($results[$intKey])) {
                            $results[$intKey]++;
                        } else {
                            $results[$intKey] = 1;
                        }
                        $pos = $commaPos + self::LINE_ADVANCE;
                    }
                }

                // Handle remaining partial line at chunk boundary
                if ($buffer !== '') {
                    $rest = fgets($fp);
                    if ($rest !== false)
                        $buffer .= $rest;
                    if (strlen($buffer) > self::URL_PREFIX_LEN) {
                        $commaPos = strpos($buffer, ',', self::URL_PREFIX_LEN);
                        if ($commaPos !== false) {
                            $slug = substr($buffer, self::URL_PREFIX_LEN, $commaPos - self::URL_PREFIX_LEN);
                            $date = substr($buffer, $commaPos + 1, self::DATE_LEN);

                            if (! isset($slugMap[$slug])) {
                                $slugRevMap[$slugCount] = $slug;
                                $slugMap[$slug] = $slugCount++;
                            }
                            if (! isset($dateMap[$date])) {
                                $dateRevMap[$dateCount] = $date;
                                $dateMap[$date] = $dateCount++;
                            }

                            $intKey = ($slugMap[$slug] * self::PATH_ID_MULTIPLIER) + $dateMap[$date];
                            if (isset($results[$intKey])) {
                                $results[$intKey]++;
                            } else {
                                $results[$intKey] = 1;
                            }
                        }
                    }
                }

                fclose($fp);

                // Convert integer keys back to string keys for IPC
                $stringResults = [];
                foreach ($results as $intKey => $count) {
                    $slug = $slugRevMap[intdiv($intKey, self::PATH_ID_MULTIPLIER)];
                    $date = $dateRevMap[$intKey % self::PATH_ID_MULTIPLIER];
                    $stringResults[$slug.','.$date] = $count;
                }

                file_put_contents("{$tmpDir}/csv_{$uid}_{$i}.dat", igbinary_serialize($stringResults));
                exit(0);
            }

            $pids[] = $pid;
        }

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $merged = [];
        for ($i = 0; $i < self::THREADS; $i++) {
            $tempFile = "{$tmpDir}/csv_{$uid}_{$i}.dat";
            /** @var array<string, int> $partial */
            $partial = igbinary_unserialize(file_get_contents($tempFile));
            unlink($tempFile);

            foreach ($partial as $key => $count) {
                if (isset($merged[$key])) {
                    $merged[$key] += $count;
                } else {
                    $merged[$key] = $count;
                }
            }
        }

        $output = [];
        foreach ($merged as $key => $count) {
            $output[substr($key, 0, -self::DATE_KEY_LEN)][substr($key, -self::DATE_LEN)] = $count;
        }

        foreach ($output as &$dates) {
            ksort($dates);
        }

        $this->jsonOutput($outputPath, $output);
    }

    private function jsonOutput(string $outputPath, array $results): void
    {
        $output = "{\n";

        $firstPath = true;
        foreach ($results as $path => $dates) {
            if (! $firstPath) {
                $output .= ",\n";
            }
            $firstPath = false;

            $output .= "    \"\/blog\/$path\": {\n";

            $firstDate = true;
            foreach ($dates as $date => $count) {
                if (! $firstDate) {
                    $output .= ",\n";
                }
                $firstDate = false;
                $output .= "        \"$date\": $count";
            }
            $output .= "\n    }";
        }
        $output .= "\n}";
        file_put_contents($outputPath, $output);
    }
}
