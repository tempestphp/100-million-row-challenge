<?php

namespace App;

\gc_disable();

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $workerCount = 14;
        $chunkBytes = 163840;
        $counterCount = 12;

        [$slugIndexByKey, $slugOrder] = $this->discoverSlugs($inputPath, 524288);
        [$dateToBinaryId, $dateJsonPrefix, $dateCount] = $this->buildDateMetadata();

        $fileSize = \filesize($inputPath);
        if ($fileSize === 0 || $slugOrder === []) {
            \file_put_contents($outputPath, "{}\n");
            return;
        }

        if ($fileSize < $chunkBytes * 4) {
            $workerCount = 1;
        }

        if ($counterCount > \count($slugOrder)) {
            $counterCount = \count($slugOrder);
        }

        $boundaries = $this->computeBoundaries($inputPath, $fileSize, $workerCount);

        $mergedBuckets = $this->collectBucketsFromWorkers(
            $inputPath,
            $boundaries,
            $workerCount,
            $chunkBytes,
            $slugOrder,
            $slugIndexByKey,
            $dateToBinaryId,
        );

        $this->writeOutputJson(
            $outputPath,
            $mergedBuckets,
            $slugOrder,
            $dateJsonPrefix,
            $counterCount,
        );
    }

    /**
     * @return array{0: array<string,int>, 1: list<string>}
     */
    private function discoverSlugs(string $inputPath, int $sampleBytes): array
    {
        $slugIndexByKey = [];
        $nextSlugIndex = 0;

        $handle = \fopen($inputPath, 'rb');
        $sample = \fread($handle, $sampleBytes);
        \fclose($handle);

        $sampleLength = \strlen($sample);
        $lineStart = 0;

        while ($lineStart + 29 < $sampleLength) {
            $commaPos = \strpos($sample, ',', $lineStart + 29);
            if ($commaPos === false) {
                break;
            }

            $slugKey = \substr($sample, $lineStart + 25, $commaPos - $lineStart - 25);

            if (!isset($slugIndexByKey[$slugKey])) {
                $slugIndexByKey[$slugKey] = $nextSlugIndex++;
            }

            $lineStart = $commaPos + 27;
        }

        return [$slugIndexByKey, \array_keys($slugIndexByKey)];
    }

    /**
     * @return array{0: array<string,string>, 1: array<int,string>, 2: int}
     */
    private function buildDateMetadata(): array
    {
        $dateToBinaryId = [];
        $dateJsonPrefix = [];
        $dateId = 0;

        $daysInMonth = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        $monthStrings = [];
        for ($month = 1; $month <= 12; $month++) {
            $monthStrings[$month] = $month < 10 ? '0' . $month : (string) $month;
        }

        $dayStrings = [];
        for ($day = 1; $day <= 31; $day++) {
            $dayStrings[$day] = $day < 10 ? '0' . $day : (string) $day;
        }

        for ($year = 2021; $year <= 2026; $year++) {
            $yy = $year - 2000;
            $yyString = $yy < 10 ? '0' . $yy : (string) $yy;
            $yearString = (string) $year;

            for ($month = 1; $month <= 12; $month++) {
                $days = $daysInMonth[$month];
                if ($month === 2 && $year === 2024) {
                    $days = 29;
                }

                $monthString = $monthStrings[$month];

                for ($day = 1; $day <= $days; $day++) {
                    $dayString = $dayStrings[$day];
                    $dateKey = $yyString . '-' . $monthString . '-' . $dayString;

                    $dateToBinaryId[$dateKey] = \chr($dateId & 0xFF) . \chr($dateId >> 8);
                    $dateJsonPrefix[$dateId] = '        "'
                        . $yearString . '-' . $monthString . '-' . $dayString
                        . '": ';

                    $dateId++;
                }
            }
        }

        return [$dateToBinaryId, $dateJsonPrefix, $dateId];
    }

    /**
     * @return list<int>
     */
    private function computeBoundaries(string $inputPath, int $fileSize, int $workerCount): array
    {
        if ($workerCount <= 1) {
            return [0, $fileSize];
        }

        $boundaries = [0];

        $handle = \fopen($inputPath, 'rb');

        for ($workerIndex = 1; $workerIndex < $workerCount; $workerIndex++) {
            \fseek($handle, (int) ($fileSize * $workerIndex / $workerCount));
            \fgets($handle);
            $boundaries[] = \ftell($handle);
        }

        \fclose($handle);
        $boundaries[] = $fileSize;

        return $boundaries;
    }

    /**
     * @param list<int> $boundaries
     * @param list<string> $slugOrder
     * @param array<string,int> $slugIndexByKey
     * @param array<string,string> $dateToBinaryId
     * @return array<string,string>
     */
    private function collectBucketsFromWorkers(
        string $inputPath,
        array $boundaries,
        int $workerCount,
        int $chunkBytes,
        array $slugOrder,
        array $slugIndexByKey,
        array $dateToBinaryId,
    ): array {
        if ($workerCount === 1) {
            return $this->parseWorkerSegment(
                $inputPath,
                $boundaries[0],
                $boundaries[1],
                $chunkBytes,
                $slugOrder,
                $dateToBinaryId,
            );
        }

        $tmpDir = \sys_get_temp_dir();
        $pidToWorker = [];

        for ($workerIndex = 0; $workerIndex < $workerCount; $workerIndex++) {
            $pid = \pcntl_fork();

            if ($pid === 0) {
                $buckets = $this->parseWorkerSegment(
                    $inputPath,
                    $boundaries[$workerIndex],
                    $boundaries[$workerIndex + 1],
                    $chunkBytes,
                    $slugOrder,
                    $dateToBinaryId,
                );

                $encoded = $this->encodeWorkerBuckets($buckets, $slugIndexByKey);
                if ($encoded !== '') {
                    \file_put_contents($tmpDir . '/parser_w' . $workerIndex, $encoded);
                }

                \posix_kill(\posix_getpid(), 9);
                exit(0);
            }

            $pidToWorker[$pid] = $workerIndex;
        }

        $mergedBuckets = \array_fill_keys($slugOrder, '');
        $completedWorkers = 0;

        do {
            $pid = \pcntl_waitpid(-1, $status);
            if ($pid <= 0) {
                continue;
            }

            if (!isset($pidToWorker[$pid])) {
                continue;
            }

            $workerIndex = $pidToWorker[$pid];
            $tmpFile = $tmpDir . '/parser_w' . $workerIndex;
            if (!\is_file($tmpFile)) {
                $completedWorkers++;
                continue;
            }

            $payload = \file_get_contents($tmpFile);
            \unlink($tmpFile);

            if ($payload === false || $payload === '') {
                $completedWorkers++;
                continue;
            }

            $this->mergeWorkerPayload($mergedBuckets, $slugOrder, $payload);
            $completedWorkers++;
        } while ($completedWorkers < $workerCount);

        return $mergedBuckets;
    }

    /**
     * @param list<string> $slugOrder
     * @param array<string,string> $dateToBinaryId
     * @return array<string,string>
     */
    private function parseWorkerSegment(
        string $inputPath,
        int $start,
        int $end,
        int $chunkBytes,
        array $slugOrder,
        array $dateToBinaryId,
    ): array {
        $buckets = \array_fill_keys($slugOrder, '');
        if ($start >= $end) {
            return $buckets;
        }

        $handle = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($handle, 0);
        \fseek($handle, $start);

        $remaining = $end - $start;

        do {
            $toRead = $remaining > $chunkBytes ? $chunkBytes : $remaining;
            $raw = \fread($handle, $toRead);

            if ($raw === false || $raw === '') {
                break;
            }

            $rawLength = \strlen($raw);
            $remaining -= $rawLength;

            $lastNewline = \strrpos($raw, "\n");
            if ($lastNewline === false) {
                break;
            }

            $tailLength = $rawLength - $lastNewline - 1;
            if ($tailLength > 0) {
                \fseek($handle, -$tailLength, SEEK_CUR);
                $remaining += $tailLength;
            }

            $slugPos = 25;
            $searchPos = 29;
            $lineFence = $lastNewline - 25;
            $unrolledFence = $lineFence - 600;

            if ($slugPos < $unrolledFence) {
                do {
                    $commaPos = \strpos($raw, ',', $searchPos);
                    $buckets[\substr($raw, $slugPos, $commaPos - $slugPos)] .= $dateToBinaryId[\substr($raw, $commaPos + 3, 8)];
                    $slugPos = $commaPos + 52;
                    $searchPos = $commaPos + 56;

                    $commaPos = \strpos($raw, ',', $searchPos);
                    $buckets[\substr($raw, $slugPos, $commaPos - $slugPos)] .= $dateToBinaryId[\substr($raw, $commaPos + 3, 8)];
                    $slugPos = $commaPos + 52;
                    $searchPos = $commaPos + 56;

                    $commaPos = \strpos($raw, ',', $searchPos);
                    $buckets[\substr($raw, $slugPos, $commaPos - $slugPos)] .= $dateToBinaryId[\substr($raw, $commaPos + 3, 8)];
                    $slugPos = $commaPos + 52;
                    $searchPos = $commaPos + 56;

                    $commaPos = \strpos($raw, ',', $searchPos);
                    $buckets[\substr($raw, $slugPos, $commaPos - $slugPos)] .= $dateToBinaryId[\substr($raw, $commaPos + 3, 8)];
                    $slugPos = $commaPos + 52;
                    $searchPos = $commaPos + 56;

                    $commaPos = \strpos($raw, ',', $searchPos);
                    $buckets[\substr($raw, $slugPos, $commaPos - $slugPos)] .= $dateToBinaryId[\substr($raw, $commaPos + 3, 8)];
                    $slugPos = $commaPos + 52;
                    $searchPos = $commaPos + 56;

                    $commaPos = \strpos($raw, ',', $searchPos);
                    $buckets[\substr($raw, $slugPos, $commaPos - $slugPos)] .= $dateToBinaryId[\substr($raw, $commaPos + 3, 8)];
                    $slugPos = $commaPos + 52;
                    $searchPos = $commaPos + 56;
                } while ($slugPos < $unrolledFence);
            }

            if ($slugPos < $lineFence) {
                do {
                    $commaPos = \strpos($raw, ',', $searchPos);
                    if ($commaPos === false || $commaPos > $lastNewline) {
                        break;
                    }

                    $buckets[\substr($raw, $slugPos, $commaPos - $slugPos)] .= $dateToBinaryId[\substr($raw, $commaPos + 3, 8)];
                    $slugPos = $commaPos + 52;
                    $searchPos = $commaPos + 56;
                } while ($slugPos < $lineFence);
            }
        } while ($remaining > 0);

        \fclose($handle);

        return $buckets;
    }

    /**
     * @param array<string,string> $buckets
     * @param array<string,int> $slugIndexByKey
     */
    private function encodeWorkerBuckets(array $buckets, array $slugIndexByKey): string
    {
        $payload = '';

        foreach ($buckets as $slugKey => $packedDates) {
            if ($packedDates === '') {
                continue;
            }

            $payload .= \pack('vV', $slugIndexByKey[$slugKey], \strlen($packedDates)) . $packedDates;
        }

        return $payload;
    }

    /**
     * @param array<string,string> $mergedBuckets
     * @param list<string> $slugOrder
     */
    private function mergeWorkerPayload(array &$mergedBuckets, array $slugOrder, string $payload): void
    {
        if ($payload === '') {
            return;
        }

        $offset = 0;
        $payloadLength = \strlen($payload);

        while ($offset < $payloadLength) {
            $slugIndex = \ord($payload[$offset]) | (\ord($payload[$offset + 1]) << 8);

            $bucketLength = \ord($payload[$offset + 2])
                | (\ord($payload[$offset + 3]) << 8)
                | (\ord($payload[$offset + 4]) << 16)
                | (\ord($payload[$offset + 5]) << 24);

            $offset += 6;
            $mergedBuckets[$slugOrder[$slugIndex]] .= \substr($payload, $offset, $bucketLength);
            $offset += $bucketLength;
        }
    }

    /**
     * @param array<string,string> $mergedBuckets
     * @param list<string> $slugOrder
     * @param array<int,string> $dateJsonPrefix
     */
    private function writeOutputJson(
        string $outputPath,
        array $mergedBuckets,
        array $slugOrder,
        array $dateJsonPrefix,
        int $counterCount,
    ): void {
        $nonEmptySlugOrder = [];
        foreach ($slugOrder as $slugKey) {
            if ($mergedBuckets[$slugKey] !== '') {
                $nonEmptySlugOrder[] = $slugKey;
            }
        }

        if ($nonEmptySlugOrder === []) {
            \file_put_contents($outputPath, "{}\n");
            return;
        }

        $slugOrder = $nonEmptySlugOrder;
        if ($counterCount > \count($slugOrder)) {
            $counterCount = \count($slugOrder);
        }

        $slugJsonHeaders = [];
        foreach ($slugOrder as $slugKey) {
            $slugJsonHeaders[$slugKey] = '    "\/blog\/' . $slugKey . '": {' . "\n";
        }

        if ($counterCount === 1) {
            $slugFragments = [];
            foreach ($slugOrder as $slugKey) {
                $packedDates = $mergedBuckets[$slugKey];
                $counts = \array_count_values(\unpack('v*', $packedDates));
                if (\count($counts) > 1) {
                    \ksort($counts, SORT_NUMERIC);
                }

                $jsonFragment = $slugJsonHeaders[$slugKey];
                $first = true;
                foreach ($counts as $dateId => $count) {
                    if (!$first) {
                        $jsonFragment .= ",\n";
                    }
                    $jsonFragment .= $dateJsonPrefix[$dateId] . $count;
                    $first = false;
                }

                $jsonFragment .= "\n    }";
                $slugFragments[] = $jsonFragment;
            }

            \file_put_contents($outputPath, "{\n" . \implode(",\n", $slugFragments) . "\n}");
            return;
        }

        $counterPipes = $this->createCounterPipes($counterCount);

        $slugCount = \count($slugOrder);
        $slugsPerCounter = intdiv($slugCount + $counterCount - 1, $counterCount);
        $counterPids = [];

        for ($counterIndex = 0; $counterIndex < $counterCount; $counterIndex++) {
            $pid = \pcntl_fork();

            if ($pid === 0) {
                for ($pipeIndex = 0; $pipeIndex < $counterCount; $pipeIndex++) {
                    \fclose($counterPipes[$pipeIndex][0]);
                    if ($pipeIndex !== $counterIndex) {
                        \fclose($counterPipes[$pipeIndex][1]);
                    }
                }

                $rangeStart = $counterIndex * $slugsPerCounter;
                $rangeEnd = \min(($counterIndex + 1) * $slugsPerCounter, $slugCount);
                $slugFragments = [];

                for ($slugPos = $rangeStart; $slugPos < $rangeEnd; $slugPos++) {
                    $slugKey = $slugOrder[$slugPos];
                    $packedDates = $mergedBuckets[$slugKey];
                    $counts = \array_count_values(\unpack('v*', $packedDates));
                    if (\count($counts) > 1) {
                        \ksort($counts, SORT_NUMERIC);
                    }
                    $jsonFragment = $slugJsonHeaders[$slugKey];
                    $first = true;
                    foreach ($counts as $dateId => $count) {
                        if (!$first) {
                            $jsonFragment .= ",\n";
                        }

                        $jsonFragment .= $dateJsonPrefix[$dateId] . $count;
                        $first = false;
                    }

                    $jsonFragment .= "\n    }";
                    $slugFragments[] = $jsonFragment;
                }

                if ($slugFragments !== []) {
                    $fragment = \implode(",\n", $slugFragments);
                    $this->writeSocketFully($counterPipes[$counterIndex][1], $fragment);
                }
                \fclose($counterPipes[$counterIndex][1]);

                \posix_kill(\posix_getpid(), 9);
                exit(0);
            }

            $counterPids[] = $pid;
        }

        for ($counterIndex = 0; $counterIndex < $counterCount; $counterIndex++) {
            \fclose($counterPipes[$counterIndex][1]);
        }

        unset($mergedBuckets);

        $outputHandle = \fopen($outputPath, 'wb');
        \fwrite($outputHandle, "{\n");

        $hasFragment = false;
        for ($counterIndex = 0; $counterIndex < $counterCount; $counterIndex++) {
            $fragment = \stream_get_contents($counterPipes[$counterIndex][0]);
            \fclose($counterPipes[$counterIndex][0]);

            if ($fragment === '') {
                continue;
            }

            if ($hasFragment) {
                \fwrite($outputHandle, ",\n");
            }

            \fwrite($outputHandle, $fragment);
            $hasFragment = true;
        }

        \fwrite($outputHandle, "\n}");
        \fclose($outputHandle);

        foreach ($counterPids as $pid) {
            \pcntl_waitpid($pid, $status);
        }
    }

    /**
     * @return array<int, array{0: resource, 1: resource}>
     */
    private function createCounterPipes(int $counterCount): array
    {
        $pipes = [];

        for ($counterIndex = 0; $counterIndex < $counterCount; $counterIndex++) {
            \socket_create_pair(AF_UNIX, SOCK_STREAM, 0, $rawPair);
            \socket_set_option($rawPair[0], SOL_SOCKET, SO_RCVBUF, 2097152);
            \socket_set_option($rawPair[1], SOL_SOCKET, SO_SNDBUF, 2097152);

            $pipes[$counterIndex] = [
                \socket_export_stream($rawPair[0]),
                \socket_export_stream($rawPair[1]),
            ];
        }

        return $pipes;
    }

    /**
     * @param resource $socket
     */
    private function writeSocketFully($socket, string $payload): void
    {
        $written = 0;
        $payloadLength = \strlen($payload);
        if ($payloadLength === 0) {
            return;
        }

        while ($written < $payloadLength) {
            $bytes = \fwrite($socket, \substr($payload, $written, 131072));
            if ($bytes === false) {
                break;
            }

            $written += $bytes;
        }
    }
}
