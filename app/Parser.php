<?php

declare(strict_types=1);

namespace App;

use function array_fill;
use function array_keys;
use function asort;
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
use function getmypid;
use function igbinary_serialize;
use function igbinary_unserialize;
use function pcntl_fork;
use function pcntl_waitpid;
use function str_replace;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strrpos;
use function strpos;
use function substr;
use function sys_get_temp_dir;
use function unlink;

final class Parser
{
    private const int READ_CHUNK_SIZE = 1_048_576;
    private const int WRITE_BUFFER_SIZE = 1_048_576;
    private const int URI_PREFIX_LENGTH = 19;
    private const int DATE_LENGTH = 10;
    private const int DATE_OFFSET_FROM_NL = 25;
    private const int MIN_LINE_LENGTH = 45;
    private const int WORKERS = 4;

    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);

        $boundaries = $this->calculateBoundaries(
            $inputPath,
            $fileSize,
        );

        $tmpDir = sys_get_temp_dir();
        $myPid = getmypid();
        $tmpFiles = [];
        $pids = [];

        for ($i = 0; $i < self::WORKERS - 1; $i++) {
            $tmpFile = $tmpDir . '/parse_' . $myPid . '_' . $i;
            $tmpFiles[$i] = $tmpFile;

            $pid = pcntl_fork();

            if ($pid === 0) {
                $data = $this->parseRangeState(
                    $inputPath,
                    $boundaries[$i],
                    $boundaries[$i + 1],
                );

                file_put_contents($tmpFile, $this->encodeWorkerData($data));

                exit(0);
            }

            if ($pid < 0) {
                throw new \RuntimeException('Unable to fork parser worker');
            }

            $pids[$i] = $pid;
        }

        $parentData = $this->parseRangeState(
            $inputPath,
            $boundaries[self::WORKERS - 1],
            $boundaries[self::WORKERS],
        );

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $mergedPaths = [];
        $mergedPathIds = [];
        $mergedDates = [];
        $mergedDateIds = [];
        $mergedCounts = [];

        foreach ($tmpFiles as $tmpFile) {
            $data = $this->decodeWorkerData((string) file_get_contents($tmpFile));
            unlink($tmpFile);

            $this->mergeState(
                $mergedPaths,
                $mergedPathIds,
                $mergedDates,
                $mergedDateIds,
                $mergedCounts,
                $data,
            );
        }

        $this->mergeState(
            $mergedPaths,
            $mergedPathIds,
            $mergedDates,
            $mergedDateIds,
            $mergedCounts,
            $parentData,
        );
        unset($parentData);

        $this->writeOutputFromState($outputPath, $mergedPaths, $mergedDates, $mergedCounts);
    }

    private function encodeWorkerData(array $data): string
    {
        return (string) igbinary_serialize($data);
    }

    private function decodeWorkerData(string $payload): mixed
    {
        return igbinary_unserialize($payload);
    }

    private function calculateBoundaries(
        string $inputPath,
        int $fileSize,
    ): array {
        $chunkSize = (int) ($fileSize / self::WORKERS);
        $boundaries = [0];

        $handle = fopen($inputPath, 'rb');

        for ($i = 1; $i < self::WORKERS; $i++) {
            fseek($handle, $i * $chunkSize);
            fgets($handle);
            $boundaries[] = ftell($handle);
        }

        fclose($handle);

        $boundaries[] = $fileSize;

        return $boundaries;
    }

    private function parseRangeState(string $inputPath, int $start, int $end): array
    {
        $pathIds = [];
        $paths = [];
        $pathCount = 0;
        $dateIds = [];
        $dates = [];
        $dateCount = 0;
        $counts = [];

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);

        $toRead = $end - $start;
        $remainder = '';

        while ($toRead > 0) {
            $chunk = fread($handle, $toRead > self::READ_CHUNK_SIZE ? self::READ_CHUNK_SIZE : $toRead);

            $toRead -= strlen($chunk);

            if ($remainder !== '') {
                $chunk = $remainder . $chunk;
            }

            $lastNl = strrpos($chunk, "\n");
            $remainder = substr($chunk, $lastNl + 1);

            $pos = 0;

            while ($pos < $lastNl) {
                $nlPos = strpos($chunk, "\n", $pos);

                $path = substr(
                    $chunk,
                    $pos + self::URI_PREFIX_LENGTH,
                    $nlPos - $pos - self::MIN_LINE_LENGTH,
                );
                $date = substr(
                    $chunk,
                    $nlPos - self::DATE_OFFSET_FROM_NL,
                    self::DATE_LENGTH,
                );

                $pathId = $pathIds[$path] ?? $pathCount;

                if ($pathId === $pathCount) {
                    $pathIds[$path] = $pathId;
                    $paths[$pathCount] = $path;
                    $counts[$pathCount] = array_fill(0, $dateCount, 0);
                    $pathCount++;
                }

                $dateId = $dateIds[$date] ?? $dateCount;

                if ($dateId === $dateCount) {
                    $dateIds[$date] = $dateId;
                    $dates[$dateCount] = $date;

                    for ($i = 0; $i < $pathCount; $i++) {
                        $counts[$i][$dateCount] = 0;
                    }

                    $dateCount++;
                }

                $counts[$pathId][$dateId]++;

                $pos = $nlPos + 1;
            }
        }

        if ($remainder !== '') {
            $len = strlen($remainder);

            $path = substr(
                $remainder,
                self::URI_PREFIX_LENGTH,
                $len - self::MIN_LINE_LENGTH,
            );
            $date = substr(
                $remainder,
                $len - self::DATE_OFFSET_FROM_NL,
                self::DATE_LENGTH,
            );

            $pathId = $pathIds[$path] ?? $pathCount;

            if ($pathId === $pathCount) {
                $pathIds[$path] = $pathId;
                $paths[$pathCount] = $path;
                $counts[$pathCount] = array_fill(0, $dateCount, 0);
                $pathCount++;
            }

            $dateId = $dateIds[$date] ?? $dateCount;

            if ($dateId === $dateCount) {
                $dateIds[$date] = $dateId;
                $dates[$dateCount] = $date;

                for ($i = 0; $i < $pathCount; $i++) {
                    $counts[$i][$dateCount] = 0;
                }

                $dateCount++;
            }

            $counts[$pathId][$dateId]++;
        }

        fclose($handle);

        return [$paths, $dates, $counts];
    }

    private function mergeState(
        array &$mergedPaths,
        array &$mergedPathIds,
        array &$mergedDates,
        array &$mergedDateIds,
        array &$mergedCounts,
        array $state,
    ): void {
        [$paths, $dates, $counts] = $state;

        $dateMap = [];
        $mergedPathCount = count($mergedPaths);
        $mergedDateCount = count($mergedDates);

        foreach ($dates as $dateId => $date) {
            $mergedDateId = $mergedDateIds[$date] ?? $mergedDateCount;

            if ($mergedDateId === $mergedDateCount) {
                $mergedDateCount++;
                $mergedDateIds[$date] = $mergedDateId;
                $mergedDates[$mergedDateId] = $date;

                for ($i = 0; $i < $mergedPathCount; $i++) {
                    $mergedCounts[$i][$mergedDateId] = 0;
                }
            }

            $dateMap[$dateId] = $mergedDateId;
        }

        foreach ($paths as $pathId => $path) {
            $mergedPathId = $mergedPathIds[$path] ?? $mergedPathCount;

            if ($mergedPathId === $mergedPathCount) {
                $mergedPathCount++;
                $mergedPathIds[$path] = $mergedPathId;
                $mergedPaths[$mergedPathId] = $path;
                $mergedCounts[$mergedPathId] = array_fill(0, $mergedDateCount, 0);
            }

            foreach ($counts[$pathId] as $dateId => $count) {
                $mergedDateId = $dateMap[$dateId];
                $mergedCounts[$mergedPathId][$mergedDateId] += $count;
            }
        }
    }

    private function writeOutputFromState(
        string $outputPath,
        array $paths,
        array $dates,
        array $counts,
    ): void {
        $sortedDates = $dates;
        asort($sortedDates);
        $orderedDateIds = array_keys($sortedDates);

        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, self::WRITE_BUFFER_SIZE);
        fwrite($out, '{');

        $firstPath = true;

        foreach ($paths as $pathId => $path) {
            $pathBuffer = $firstPath ? '' : ',';
            $firstPath = false;
            $escapedPath = str_replace('/', '\\/', $path);
            $pathBuffer .= "\n    \"{$escapedPath}\": {";

            $firstDate = true;
            $pathCounts = $counts[$pathId];

            foreach ($orderedDateIds as $dateId) {
                $count = $pathCounts[$dateId];

                if ($count === 0) {
                    continue;
                }

                if ($firstDate) {
                    $pathBuffer .= "\n";
                    $firstDate = false;
                } else {
                    $pathBuffer .= ",\n";
                }

                $pathBuffer .= "        \"{$dates[$dateId]}\": {$count}";
            }

            $pathBuffer .= "\n    }";
            fwrite($out, $pathBuffer);
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
