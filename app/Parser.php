<?php

namespace App;

use App\Commands\Visit;

final class Parser
{
    private const READ_BUFFER_SIZE = 16 * 1024 * 1024;
    private const PARALLEL_THRESHOLD = 128 * 1024 * 1024;
    private const WORKER_COUNT = 2;
    private const URL_PATH_OFFSET = 19;
    private const URL_SUFFIX_LENGTH_LF = 26;
    private const DATE_OFFSET_LF = 25;
    private const URL_SUFFIX_LENGTH_CRLF = 27;
    private const DATE_OFFSET_CRLF = 26;
    private const DATE_LENGTH = 10;
    private const MAX_LINE_LENGTH = 128;

    public function parse(string $inputPath, string $outputPath): void
    {
        [$urlToId, $pathsById] = $this->buildUrlMap();

        [$countsById, $orderById] = $this->parseInput($inputPath, $urlToId, count($pathsById));

        $result = [];

        foreach ($orderById as $id) {
            $dateCounts = $countsById[$id];

            if ($dateCounts === []) {
                continue;
            }

            ksort($dateCounts, SORT_STRING);
            $result[$pathsById[$id]] = $dateCounts;
        }

        $json = json_encode($result, JSON_PRETTY_PRINT);

        if (PHP_EOL !== "\n") {
            $json = str_replace("\n", PHP_EOL, $json);
        }

        file_put_contents($outputPath, $json);
    }

    private function buildUrlMap(): array
    {
        $urlToId = [];
        $pathsById = [];

        foreach (Visit::all() as $visit) {
            $id = count($pathsById);
            $path = substr($visit->uri, self::URL_PATH_OFFSET);

            $urlToId[$visit->uri] = $id;
            $pathsById[$id] = $path;
        }

        return [$urlToId, $pathsById];
    }

    private function parseInput(string $inputPath, array $urlToId, int $urlCount): array
    {
        if (! function_exists('pcntl_fork') || ! function_exists('pcntl_waitpid')) {
            [$countsById, $orderById, $datesById] = $this->parseSegment($inputPath, 0, null, $urlToId, $urlCount);

            return [$this->materializeDateStrings($countsById, $datesById), $orderById];
        }

        $fileSize = filesize($inputPath);

        if ($fileSize === false || $fileSize < self::PARALLEL_THRESHOLD) {
            [$countsById, $orderById, $datesById] = $this->parseSegment($inputPath, 0, null, $urlToId, $urlCount);

            return [$this->materializeDateStrings($countsById, $datesById), $orderById];
        }

        return $this->parseParallel($inputPath, $fileSize, $urlToId, $urlCount);
    }

    private function parseParallel(string $inputPath, int $fileSize, array $urlToId, int $urlCount): array
    {
        $segments = $this->buildSegments($fileSize);
        return $this->parseParallelWithTempFiles($inputPath, $segments, $urlToId, $urlCount);
    }

    private function buildSegments(int $fileSize): array
    {
        $segmentSize = intdiv($fileSize, self::WORKER_COUNT);
        $segments = [];
        $start = 0;

        for ($i = 0; $i < self::WORKER_COUNT; $i++) {
            $end = $i === self::WORKER_COUNT - 1
                ? null
                : $start + $segmentSize;

            $segments[$i] = [$start, $end];
            $start += $segmentSize;
        }

        return $segments;
    }

    private function parseParallelWithTempFiles(string $inputPath, array $segments, array $urlToId, int $urlCount): array
    {
        $children = [];
        $tempBasePath = sys_get_temp_dir() . DIRECTORY_SEPARATOR . 'php-100m-' . getmypid() . '-';

        foreach ($segments as $index => [$segmentStart, $segmentEnd]) {
            $tempPath = $tempBasePath . $index . '.tmp';
            $pid = pcntl_fork();

            if ($pid === -1) {
                $this->cleanupTempFiles($children);

                [$countsById, $orderById, $datesById] = $this->parseSegment($inputPath, 0, null, $urlToId, $urlCount);

                return [$this->materializeDateStrings($countsById, $datesById), $orderById];
            }

            if ($pid === 0) {
                [$countsById, $orderById, $datesById] = $this->parseSegment($inputPath, $segmentStart, $segmentEnd, $urlToId, $urlCount);

                file_put_contents($tempPath, $this->encodePayload([$countsById, $orderById, $datesById]));

                exit(0);
            }

            $children[] = [
                'index' => $index,
                'pid' => $pid,
                'tempPath' => $tempPath,
            ];
        }

        if (! $this->waitForChildren($children)) {
            $this->cleanupTempFiles($children);

            [$countsById, $orderById, $datesById] = $this->parseSegment($inputPath, 0, null, $urlToId, $urlCount);

            return [$this->materializeDateStrings($countsById, $datesById), $orderById];
        }

        usort($children, static fn (array $a, array $b): int => $a['index'] <=> $b['index']);

        $mergedCountsById = array_fill(0, $urlCount, []);
        $mergedSeen = array_fill(0, $urlCount, false);
        $mergedOrderById = [];

        foreach ($children as $child) {
            $raw = file_get_contents($child['tempPath']);
            @unlink($child['tempPath']);

            if (! is_string($raw)) {
                $this->cleanupTempFiles($children);

                [$countsById, $orderById, $datesById] = $this->parseSegment($inputPath, 0, null, $urlToId, $urlCount);

                return [$this->materializeDateStrings($countsById, $datesById), $orderById];
            }

            $data = $this->decodePayload($raw);

            if ($data === null) {
                $this->cleanupTempFiles($children);

                [$countsById, $orderById, $datesById] = $this->parseSegment($inputPath, 0, null, $urlToId, $urlCount);

                return [$this->materializeDateStrings($countsById, $datesById), $orderById];
            }

            [$localCountsById, $localOrderById, $localDatesById] = $data;
            $this->mergePartialCounts(
                $mergedCountsById,
                $mergedSeen,
                $mergedOrderById,
                $localCountsById,
                $localOrderById,
                $localDatesById,
            );
        }

        return [$mergedCountsById, $mergedOrderById];
    }

    private function mergePartialCounts(
        array &$mergedCountsById,
        array &$mergedSeen,
        array &$mergedOrderById,
        array $localCountsById,
        array $localOrderById,
        array $localDatesById,
    ): void {
        foreach ($localOrderById as $id) {
            if (! $mergedSeen[$id]) {
                $mergedSeen[$id] = true;
                $mergedOrderById[] = $id;
            }
        }

        foreach ($localCountsById as $id => $dateCounts) {
            if ($dateCounts === []) {
                continue;
            }

            if ($mergedCountsById[$id] === []) {
                $materialized = [];

                foreach ($dateCounts as $localDateId => $count) {
                    $materialized[$localDatesById[$localDateId]] = $count;
                }

                $mergedCountsById[$id] = $materialized;

                continue;
            }

            foreach ($dateCounts as $localDateId => $count) {
                $date = $localDatesById[$localDateId];

                if (isset($mergedCountsById[$id][$date])) {
                    $mergedCountsById[$id][$date] += $count;
                } else {
                    $mergedCountsById[$id][$date] = $count;
                }
            }
        }
    }

    private function waitForChildren(array $children): bool
    {
        foreach ($children as $child) {
            $status = 0;
            pcntl_waitpid($child['pid'], $status);

            if (! pcntl_wifexited($status) || pcntl_wexitstatus($status) !== 0) {
                return false;
            }
        }

        return true;
    }

    private function encodePayload(array $payload): string
    {
        if (function_exists('igbinary_serialize')) {
            return call_user_func('igbinary_serialize', $payload);
        }

        return serialize($payload);
    }

    private function decodePayload(string $raw): ?array
    {
        if (function_exists('igbinary_unserialize')) {
            $data = call_user_func('igbinary_unserialize', $raw);
        } else {
            $data = unserialize($raw, ['allowed_classes' => false]);
        }

        if (! is_array($data) || count($data) !== 3) {
            return null;
        }

        return $data;
    }

    private function cleanupTempFiles(array $children): void
    {
        foreach ($children as $child) {
            if (is_string($child['tempPath'] ?? null) && is_file($child['tempPath'])) {
                @unlink($child['tempPath']);
            }
        }
    }

    private function materializeDateStrings(array $countsById, array $datesById): array
    {
        foreach ($countsById as $id => $dateCounts) {
            if ($dateCounts === []) {
                continue;
            }

            $materialized = [];

            foreach ($dateCounts as $dateId => $count) {
                $materialized[$datesById[$dateId]] = $count;
            }

            $countsById[$id] = $materialized;
        }

        return $countsById;
    }

    private function parseSegment(
        string $inputPath,
        int $start,
        ?int $end,
        array $urlToId,
        int $urlCount,
    ): array {
        $countsById = array_fill(0, $urlCount, []);
        $seen = array_fill(0, $urlCount, false);
        $orderById = [];
        $dateToId = [];
        $datesById = [];
        $nextDateId = 0;

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, self::READ_BUFFER_SIZE);

        if ($start > 0) {
            fseek($handle, $start - 1);
            $previousCharacter = fgetc($handle);

            fseek($handle, $start);

            if ($previousCharacter !== "\n") {
                stream_get_line($handle, self::MAX_LINE_LENGTH, "\n");
            }
        }

        if ($end === null) {
            $line = stream_get_line($handle, self::MAX_LINE_LENGTH, "\n");

            if ($line === false) {
                fclose($handle);

                return [$countsById, $orderById, $datesById];
            }

            [$urlSuffixLength, $dateOffset] = $this->resolveLineOffsets($line);
            $pathLength = -$urlSuffixLength;
            $dateStart = -$dateOffset;

            do {
                $path = substr($line, 0, $pathLength);
                $date = substr($line, $dateStart, self::DATE_LENGTH);

                if (isset($dateToId[$date])) {
                    $dateId = $dateToId[$date];
                } else {
                    $dateId = $nextDateId;
                    $dateToId[$date] = $dateId;
                    $datesById[$dateId] = $date;
                    ++$nextDateId;
                }

                $id = $urlToId[$path];

                if (! $seen[$id]) {
                    $seen[$id] = true;
                    $orderById[] = $id;
                }

                $bucket = &$countsById[$id];

                if (isset($bucket[$dateId])) {
                    ++$bucket[$dateId];
                } else {
                    $bucket[$dateId] = 1;
                }
            } while (($line = stream_get_line($handle, self::MAX_LINE_LENGTH, "\n")) !== false);

            fclose($handle);

            return [$countsById, $orderById, $datesById];
        }

        $remaining = $end - ftell($handle);

        if ($remaining > 0 && ($line = stream_get_line($handle, self::MAX_LINE_LENGTH, "\n")) !== false) {
            [$urlSuffixLength, $dateOffset] = $this->resolveLineOffsets($line);
            $pathLength = -$urlSuffixLength;
            $dateStart = -$dateOffset;

            do {
                $remaining -= strlen($line) + 1;

                $path = substr($line, 0, $pathLength);
                $date = substr($line, $dateStart, self::DATE_LENGTH);

                if (isset($dateToId[$date])) {
                    $dateId = $dateToId[$date];
                } else {
                    $dateId = $nextDateId;
                    $dateToId[$date] = $dateId;
                    $datesById[$dateId] = $date;
                    ++$nextDateId;
                }

                $id = $urlToId[$path];

                if (! $seen[$id]) {
                    $seen[$id] = true;
                    $orderById[] = $id;
                }

                $bucket = &$countsById[$id];

                if (isset($bucket[$dateId])) {
                    ++$bucket[$dateId];
                } else {
                    $bucket[$dateId] = 1;
                }

                if ($remaining <= 0) {
                    break;
                }

                $line = stream_get_line($handle, self::MAX_LINE_LENGTH, "\n");
            } while ($line !== false);
        }

        fclose($handle);

        return [$countsById, $orderById, $datesById];
    }

    private function resolveLineOffsets(string $line): array
    {
        if (str_ends_with($line, "\r")) {
            return [self::URL_SUFFIX_LENGTH_CRLF, self::DATE_OFFSET_CRLF];
        }

        return [self::URL_SUFFIX_LENGTH_LF, self::DATE_OFFSET_LF];
    }

}
