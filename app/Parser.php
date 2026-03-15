<?php

namespace App;

use RuntimeException;

final class Parser
{
    private const string PATTERN = '/^https:\/\/stitcher\.io([^,]++),(\d{4}-\d{2}-\d{2})/m';
    private const int BUFFER_SIZE = 4096;
    private const int SMALL_SEGMENT_BYTES = 16 * 1024 * 1024;
    private const int PARALLEL_SEGMENT_BYTES = 20 * 1024 * 1024;

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();
        @ini_set('memory_limit', '-1');

        $fileSize = filesize($inputPath);

        if ($fileSize === false || $fileSize === 0) {
            file_put_contents($outputPath, "{\n}");
            return;
        }

        $numWorkers = min(
            $this->cpuCount(),
            max(2, (int) ceil($fileSize / self::PARALLEL_SEGMENT_BYTES)),
        );

        if ($numWorkers > 1 && $fileSize > self::PARALLEL_SEGMENT_BYTES && function_exists('pcntl_fork')) {
            $stats = $this->parseParallel($inputPath, $fileSize, $numWorkers);
        } else {
            $stats = $this->parseSegment($inputPath, 0, $fileSize);
        }

        foreach ($stats as $path => $_) {
            ksort($stats[$path], SORT_STRING);
        }

        $json = json_encode($stats, JSON_PRETTY_PRINT);

        if (!is_string($json)) {
            throw new RuntimeException('Unable to encode parser output as JSON.');
        }

        file_put_contents($outputPath, $json);
    }

    private function cpuCount(): int
    {
        static $count = null;

        if ($count !== null) {
            return $count;
        }

        $detected = 0;

        if ($detected < 2 && function_exists('shell_exec')) {
            $detected = (int) @shell_exec('nproc 2>/dev/null');
        }

        if ($detected < 2 && function_exists('shell_exec')) {
            $detected = (int) @shell_exec('sysctl -n hw.logicalcpu 2>/dev/null');
        }

        if ($detected < 2 && @is_readable('/proc/cpuinfo')) {
            $cpuinfo = @file_get_contents('/proc/cpuinfo');

            if ($cpuinfo !== false) {
                $detected = substr_count($cpuinfo, 'processor');
            }
        }

        if ($detected < 2 && function_exists('posix_sysconf') && defined('POSIX_SC_NPROCESSORS_ONLN')) {
            $detected = (int) posix_sysconf(POSIX_SC_NPROCESSORS_ONLN);
        }

        return $count = $detected > 1 ? min($detected, 16) : 8;
    }

    private function parseParallel(string $inputPath, int $fileSize, int $numWorkers): array
    {
        $segments = $this->computeSegments($inputPath, $fileSize, $numWorkers);
        $workerCount = count($segments);
        $useIgbinary = function_exists('igbinary_serialize');
        $tmpFiles = [];
        $pids = [];
        $forkedWorkers = 0;

        for ($worker = 0; $worker < $workerCount; $worker++) {
            $tmpFiles[$worker] = tempnam(sys_get_temp_dir(), 'tempest-parse-');

            if ($tmpFiles[$worker] === false) {
                throw new RuntimeException('Unable to create a temporary file for parser output.');
            }

            [$segmentStart, $segmentEnd] = $segments[$worker];
            $pid = pcntl_fork();

            if ($pid === -1) {
                break;
            }

            if ($pid === 0) {
                gc_disable();
                @ini_set('memory_limit', '-1');

                $stats = $this->parseSegment($inputPath, $segmentStart, $segmentEnd);
                file_put_contents(
                    $tmpFiles[$worker],
                    $useIgbinary ? igbinary_serialize($stats) : serialize($stats),
                );

                exit(0);
            }

            $pids[$worker] = $pid;
            $forkedWorkers++;
        }

        $partials = [];
        $pidToWorker = array_flip($pids);
        $remaining = count($pids);

        while ($remaining > 0) {
            $pid = pcntl_waitpid(-1, $status, WNOHANG);

            if ($pid > 0 && isset($pidToWorker[$pid])) {
                $worker = $pidToWorker[$pid];
                $remaining--;

                $raw = file_get_contents($tmpFiles[$worker]);
                @unlink($tmpFiles[$worker]);

                $partial = $raw === false
                    ? null
                    : ($useIgbinary ? igbinary_unserialize($raw) : unserialize($raw));

                if (is_array($partial)) {
                    $partials[$worker] = $partial;
                }
            } elseif ($pid === 0) {
                usleep(100);
            }
        }

        for ($worker = $forkedWorkers; $worker < $workerCount; $worker++) {
            @unlink($tmpFiles[$worker]);

            [$segmentStart, $segmentEnd] = $segments[$worker];
            $partials[$worker] = $this->parseSegment($inputPath, $segmentStart, $segmentEnd);
        }

        ksort($partials);

        $merged = [];

        foreach ($partials as $partial) {
            $this->mergeInto($merged, $partial);
        }

        return $merged;
    }

    private function computeSegments(string $inputPath, int $fileSize, int $numWorkers): array
    {
        $chunkSize = (int) ceil($fileSize / $numWorkers);
        $handle = fopen($inputPath, 'rb');

        if ($handle === false) {
            throw new RuntimeException("Unable to open input file: {$inputPath}");
        }

        $segments = [];
        $position = 0;

        for ($worker = 0; $worker < $numWorkers; $worker++) {
            $start = $position;
            $end = min($position + $chunkSize, $fileSize);

            if ($end >= $fileSize) {
                $segments[] = [$start, $fileSize];
                break;
            }

            fseek($handle, $end);
            $peek = fread($handle, 256);
            $newline = $peek === false ? false : strpos($peek, "\n");
            $end = $newline === false ? $fileSize : $end + $newline + 1;

            $segments[] = [$start, $end];
            $position = $end;

            if ($position >= $fileSize) {
                break;
            }
        }

        fclose($handle);

        return $segments;
    }

    private function parseSegment(string $inputPath, int $start, int $end): array
    {
        $segmentSize = $end - $start;

        if ($segmentSize < self::SMALL_SEGMENT_BYTES) {
            $content = file_get_contents($inputPath, false, null, $start, $segmentSize);

            if ($content === false || $content === '') {
                return [];
            }

            return $this->parseBlock($content);
        }

        $handle = fopen($inputPath, 'rb');

        if ($handle === false) {
            throw new RuntimeException("Unable to open input file: {$inputPath}");
        }

        if ($start > 0) {
            fseek($handle, $start);
        }

        $stats = [];
        $remaining = $segmentSize;
        $carry = '';

        while ($remaining > 0) {
            $readSize = min(self::BUFFER_SIZE, $remaining);
            $chunk = fread($handle, $readSize);

            if ($chunk === false || $chunk === '') {
                break;
            }

            $remaining -= strlen($chunk);
            $block = $carry . $chunk;
            $lastNewline = strrpos($block, "\n");

            if ($lastNewline === false) {
                $carry = $block;
                continue;
            }

            $carry = substr($block, $lastNewline + 1);
            $this->collectMatches(substr($block, 0, $lastNewline), $stats);
        }

        if ($carry !== '') {
            $this->collectMatches($carry, $stats);
        }

        fclose($handle);

        return $stats;
    }

    private function parseBlock(string $block): array
    {
        $stats = [];
        $this->collectMatches($block, $stats);

        return $stats;
    }

    private function collectMatches(string $block, array &$stats): void
    {
        if ($block === '') {
            return;
        }

        preg_match_all(self::PATTERN, $block, $matches);

        if (!isset($matches[1], $matches[2])) {
            return;
        }

        $paths = $matches[1];
        $dates = $matches[2];
        $matchCount = count($paths);

        for ($index = 0; $index < $matchCount; $index++) {
            $path = $paths[$index];
            $date = $dates[$index];

            if (isset($stats[$path][$date])) {
                ++$stats[$path][$date];
            } else {
                $stats[$path][$date] = 1;
            }
        }
    }

    private function mergeInto(array &$merged, array $partial): void
    {
        foreach ($partial as $path => $dates) {
            if (!isset($merged[$path])) {
                $merged[$path] = $dates;
                continue;
            }

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
