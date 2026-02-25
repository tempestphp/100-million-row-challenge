<?php

namespace App;

final class Parser
{
    private const WORKERS = 3;
    private const PATH_START = 19;
    private const DATE_BITS = 12;
    private const DATE_MASK = 0xFFF;

    public function parse(string $inputPath, string $outputPath): void
    {
        \gc_disable();
        \set_time_limit(0);
        \ini_set('memory_limit', '1536M');

        if (!\is_file($inputPath)) {
            $this->writeEmptyOutput($outputPath);

            return;
        }

        if (!\function_exists('pcntl_fork')) {
            $this->writeEmptyOutput($outputPath);

            return;
        }

        $fileSize = \filesize($inputPath);
        if (!\is_int($fileSize) || $fileSize <= 0) {
            $this->writeEmptyOutput($outputPath);

            return;
        }

        $tmpDir = \dirname($outputPath) . '/.parser_tmp_' . \getmypid() . '_' . \substr(\uniqid('', true), -8);
        if (!\mkdir($tmpDir, 0777, true) && !\is_dir($tmpDir)) {
            $this->writeEmptyOutput($outputPath);

            return;
        }

        $fragFiles = [];
        $failed = false;

        try {
            $pids = [];

            for ($worker = 0; $worker < self::WORKERS; $worker++) {
                $fragFiles[$worker] = $tmpDir . '/frag' . $worker;
                $chunkStart = \intdiv($fileSize * $worker, self::WORKERS);
                $chunkEnd = \intdiv($fileSize * ($worker + 1), self::WORKERS);

                $pid = \pcntl_fork();
                if ($pid === -1) {
                    $failed = true;
                    break;
                }

                if ($pid === 0) {
                    $ok = $this->parseChunk($inputPath, $chunkStart, $chunkEnd, $fragFiles[$worker]);
                    \exit($ok ? 0 : 1);
                }

                $pids[] = $pid;
            }

            foreach ($pids as $pid) {
                $status = 0;
                \pcntl_waitpid($pid, $status);

                if (!\pcntl_wifexited($status) || \pcntl_wexitstatus($status) !== 0) {
                    $failed = true;
                }
            }

            if ($failed) {
                $this->writeEmptyOutput($outputPath);

                return;
            }

            if (!$this->mergeFragments($fragFiles, $outputPath)) {
                $this->writeEmptyOutput($outputPath);
            }
        } finally {
            $this->cleanupTmpDir($tmpDir);
        }
    }

    private function parseChunk(string $inputPath, int $chunkStart, int $chunkEnd, string $fragmentPath): bool
    {
        $input = \fopen($inputPath, 'rb');
        if ($input === false) {
            return false;
        }

        if ($chunkStart > 0) {
            if (\fseek($input, $chunkStart - 1) !== 0) {
                \fclose($input);

                return false;
            }

            \fgets($input);
        } else {
            if (\fseek($input, 0) !== 0) {
                \fclose($input);

                return false;
            }
        }

        $pos = \ftell($input);
        if (!\is_int($pos)) {
            \fclose($input);

            return false;
        }

        $pathToId = [];
        $paths = [];
        $dateToId = [];
        $dates = [];
        $firstOffsets = [];
        $counts = [];

        while (($line = \fgets($input)) !== false) {
            $lineStart = $pos;
            $lineLength = \strlen($line);
            $pos += $lineLength;

            if ($lineStart >= $chunkEnd) {
                break;
            }

            $comma = \strpos($line, ',');
            $path = \substr($line, self::PATH_START, $comma - self::PATH_START);
            $date = \substr($line, $comma + 1, 10);

            if (!isset($pathToId[$path])) {
                $pathId = \count($paths);
                $pathToId[$path] = $pathId;
                $paths[$pathId] = $path;
                $firstOffsets[$pathId] = $lineStart;
            } else {
                $pathId = $pathToId[$path];
            }

            if (!isset($dateToId[$date])) {
                $dateId = \count($dates);
                if ($dateId > self::DATE_MASK) {
                    \fclose($input);

                    return false;
                }

                $dateToId[$date] = $dateId;
                $dates[$dateId] = $date;
            } else {
                $dateId = $dateToId[$date];
            }

            $key = ($pathId << self::DATE_BITS) | $dateId;
            if (isset($counts[$key])) {
                ++$counts[$key];
            } else {
                $counts[$key] = 1;
            }
        }

        \fclose($input);

        $fragment = \fopen($fragmentPath, 'wb');
        if ($fragment === false) {
            return false;
        }

        $payload = \serialize([$paths, $dates, $counts, $firstOffsets]);
        $written = \fwrite($fragment, $payload);
        \fclose($fragment);

        return \is_int($written) && $written === \strlen($payload);
    }

    private function mergeFragments(array $fragmentPaths, string $outputPath): bool
    {
        $globalPathToId = [];
        $globalPaths = [];
        $globalPathFirst = [];

        $globalDateToId = [];
        $globalDates = [];

        $globalCounts = [];

        foreach ($fragmentPaths as $fragmentPath) {
            $payload = $this->loadFragment($fragmentPath);
            if ($payload === null) {
                return false;
            }

            [$workerPaths, $workerDates, $workerCounts, $workerFirstOffsets] = $payload;

            $pathIdRemap = [];
            foreach ($workerPaths as $workerPathId => $path) {
                if (isset($globalPathToId[$path])) {
                    $globalPathId = $globalPathToId[$path];
                } else {
                    $globalPathId = \count($globalPaths);
                    $globalPathToId[$path] = $globalPathId;
                    $globalPaths[$globalPathId] = $path;
                }

                $pathIdRemap[$workerPathId] = $globalPathId;

                $offset = $workerFirstOffsets[$workerPathId] ?? null;
                if (\is_int($offset) && (!isset($globalPathFirst[$globalPathId]) || $offset < $globalPathFirst[$globalPathId])) {
                    $globalPathFirst[$globalPathId] = $offset;
                }
            }

            $dateIdRemap = [];
            foreach ($workerDates as $workerDateId => $date) {
                if (isset($globalDateToId[$date])) {
                    $globalDateId = $globalDateToId[$date];
                } else {
                    $globalDateId = \count($globalDates);
                    if ($globalDateId > self::DATE_MASK) {
                        return false;
                    }

                    $globalDateToId[$date] = $globalDateId;
                    $globalDates[$globalDateId] = $date;
                }

                $dateIdRemap[$workerDateId] = $globalDateId;
            }

            foreach ($workerCounts as $workerKey => $count) {
                if (!\is_int($count) || $count < 1) {
                    continue;
                }

                $workerPathId = $workerKey >> self::DATE_BITS;
                $workerDateId = $workerKey & self::DATE_MASK;

                if (!isset($pathIdRemap[$workerPathId], $dateIdRemap[$workerDateId])) {
                    continue;
                }

                $globalPathId = $pathIdRemap[$workerPathId];
                $globalDateId = $dateIdRemap[$workerDateId];
                $globalKey = ($globalPathId << self::DATE_BITS) | $globalDateId;

                if (isset($globalCounts[$globalKey])) {
                    $globalCounts[$globalKey] += $count;
                } else {
                    $globalCounts[$globalKey] = $count;
                }
            }
        }

        \asort($globalPathFirst, \SORT_NUMERIC);

        $perPathDates = [];
        foreach ($globalCounts as $globalKey => $count) {
            $pathId = $globalKey >> self::DATE_BITS;
            $dateId = $globalKey & self::DATE_MASK;

            if (!isset($globalDates[$dateId])) {
                continue;
            }

            $perPathDates[$pathId][$globalDates[$dateId]] = $count;
        }

        $ordered = [];
        foreach ($globalPathFirst as $pathId => $_offset) {
            if (!isset($globalPaths[$pathId])) {
                continue;
            }

            $dates = $perPathDates[$pathId] ?? [];
            if ($dates !== []) {
                \ksort($dates, \SORT_STRING);
            }

            $ordered[$globalPaths[$pathId]] = $dates;
        }

        $json = \json_encode($ordered, \JSON_PRETTY_PRINT);
        if (!\is_string($json)) {
            return false;
        }

        $written = \file_put_contents($outputPath, $json);

        return \is_int($written);
    }

    private function loadFragment(string $fragmentPath): ?array
    {
        if (!\is_file($fragmentPath)) {
            return null;
        }

        $raw = \file_get_contents($fragmentPath);
        if (!\is_string($raw) || $raw === '') {
            return null;
        }

        $decoded = \unserialize($raw, ['allowed_classes' => false]);
        if (!\is_array($decoded) || \count($decoded) !== 4) {
            return null;
        }

        return $decoded;
    }

    private function cleanupTmpDir(string $tmpDir): void
    {
        if (!\is_dir($tmpDir)) {
            return;
        }

        $entries = \scandir($tmpDir);
        if ($entries === false) {
            return;
        }

        foreach ($entries as $entry) {
            if ($entry === '.' || $entry === '..') {
                continue;
            }

            $path = $tmpDir . '/' . $entry;
            if (\is_dir($path)) {
                $this->cleanupTmpDir($path);
                if (\is_dir($path)) {
                    \rmdir($path);
                }

                continue;
            }

            if (\is_file($path)) {
                \unlink($path);
            }
        }

        if (\is_dir($tmpDir)) {
            \rmdir($tmpDir);
        }
    }

    private function writeEmptyOutput(string $outputPath): void
    {
        \file_put_contents($outputPath, '{}');
    }
}
