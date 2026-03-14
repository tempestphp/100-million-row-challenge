<?php

namespace App;

use Exception;

final class Parser
{
    // Skips the fixed 19-byte URL prefix 'https://stitcher.io', then captures
    // the path and the date portion of the ISO 8601 timestamp.
    private const string RE = '/^.{19}([^,]+),(\d{4}-\d{2}-\d{2})/m';

    // Small read buffer keeps the PCRE subject string hot in L1 cache.
    private const int BUF = 4096;

    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);
        if ($fileSize === false || $fileSize === 0) {
            file_put_contents($outputPath, "{\n}");
            return;
        }

        $numWorkers = $this->cpuCount();
        if ($numWorkers > 1 && $fileSize > 32 * 1024 * 1024 && function_exists('pcntl_fork')) {
            $stats = $this->parseParallel($inputPath, $fileSize, $numWorkers);
        } else {
            $stats = $this->parseSegment($inputPath, 0, $fileSize);
        }

        foreach ($stats as $path => $_) {
            ksort($stats[$path]);
        }

        $json = json_encode($stats, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
        file_put_contents($outputPath, str_replace('/', '\\/', $json));
    }

    private function cpuCount(): int
    {
        static $count = null;
        if ($count !== null) {
            return $count;
        }

        $n = (int) @shell_exec('nproc 2>/dev/null');
        if ($n < 2) {
            $n = (int) @shell_exec('sysctl -n hw.logicalcpu 2>/dev/null');
        }

        return $count = ($n > 1 ? min($n, 16) : 4);
    }

    private function parseParallel(string $inputPath, int $fileSize, int $numWorkers): array
    {
        $segments = $this->computeSegments($inputPath, $fileSize, $numWorkers);
        $actualWorkers = count($segments);

        // Prefer igbinary when available — smaller payload, faster round-trip.
        $useIgbinary = function_exists('igbinary_serialize');

        $tmpFiles = [];
        $pids = [];

        for ($w = 0; $w < $actualWorkers; $w++) {
            $tmpFile = tempnam(sys_get_temp_dir(), 'php1brc');
            $tmpFiles[$w] = $tmpFile;
            [$segStart, $segEnd] = $segments[$w];

            $pid = pcntl_fork();
            if ($pid === -1) {
                break;
            }

            if ($pid === 0) {
                $stats = $this->parseSegment($inputPath, $segStart, $segEnd);
                file_put_contents(
                    $tmpFile,
                    $useIgbinary ? igbinary_serialize($stats) : serialize($stats)
                );
                exit(0);
            }

            $pids[$w] = $pid;
        }

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $merged = [];
        for ($w = 0; $w < $actualWorkers; $w++) {
            if (!isset($tmpFiles[$w]) || !file_exists($tmpFiles[$w])) {
                continue;
            }

            $raw = file_get_contents($tmpFiles[$w]);
            unlink($tmpFiles[$w]);
            $partial = $useIgbinary ? igbinary_unserialize($raw) : unserialize($raw);

            if (!is_array($partial)) {
                continue;
            }

            foreach ($partial as $path => $dates) {
                if (!isset($merged[$path])) {
                    $merged[$path] = $dates;
                } else {
                    foreach ($dates as $date => $count) {
                        if (isset($merged[$path][$date])) {
                            $merged[$path][$date] += $count;
                        } else {
                            $merged[$path][$date] = $count;
                        }
                    }
                }
            }
        }

        return $merged;
    }

    /**
     * Splits the file into byte ranges aligned to line boundaries so each
     * worker can seek directly to its start without re-scanning from the top.
     */
    private function computeSegments(string $inputPath, int $fileSize, int $numWorkers): array
    {
        $chunkSize = (int) ceil($fileSize / $numWorkers);
        $handle = fopen($inputPath, 'rb');
        $segments = [];
        $pos = 0;

        for ($w = 0; $w < $numWorkers; $w++) {
            $start = $pos;
            $end = min($pos + $chunkSize, $fileSize);

            if ($end >= $fileSize) {
                $segments[] = [$start, $fileSize];
                break;
            }

            // Advance to the next newline so we never split a record.
            fseek($handle, $end);
            $peek = fread($handle, 256);
            $nl = strpos($peek, "\n");
            $end = $nl !== false ? $end + $nl + 1 : $fileSize;

            $segments[] = [$start, $end];
            $pos = $end;

            if ($pos >= $fileSize) {
                break;
            }
        }

        fclose($handle);
        return $segments;
    }

    private function parseSegment(string $inputPath, int $start, int $end): array
    {
        $handle = fopen($inputPath, 'rb');
        if ($start > 0) {
            fseek($handle, $start);
        }

        $stats = [];
        $remaining = $end - $start;
        $carry = '';

        while ($remaining > 0) {
            $toRead = min(self::BUF, $remaining);
            $chunk = fread($handle, $toRead);
            if ($chunk === false || $chunk === '') {
                break;
            }
            $remaining -= strlen($chunk);

            $block = $carry . $chunk;
            $lastNl = strrpos($block, "\n");

            if ($lastNl !== false) {
                $carry = substr($block, $lastNl + 1);
                $block = substr($block, 0, $lastNl);
            } else {
                $carry = $block;
                continue;
            }

            preg_match_all(self::RE, $block, $m);
            $dates = $m[2];
            foreach ($m[1] as $i => $p) {
                $d = $dates[$i];
                if (isset($stats[$p][$d])) {
                    $stats[$p][$d]++;
                } else {
                    $stats[$p][$d] = 1;
                }
            }
        }

        // Handle a trailing line that had no terminating newline.
        if ($carry !== '') {
            preg_match_all(self::RE, $carry, $m);
            $dates = $m[2];
            foreach ($m[1] as $i => $p) {
                $d = $dates[$i];
                if (isset($stats[$p][$d])) {
                    $stats[$p][$d]++;
                } else {
                    $stats[$p][$d] = 1;
                }
            }
        }

        fclose($handle);
        return $stats;
    }
}
