<?php declare(strict_types=1);

namespace App;

use App\Commands\Visit;

final class Parser
{
    private const int READ_CHUNK_SIZE = 524288;
    private const int WRITE_BUFFER_SIZE = 4_194_304;
    private const int DATE_LENGTH = 10;
    private const int TIMESTAMP_LENGTH = 25;
    private const int URI_PREFIX_LENGTH = 19;
    private const int MIN_LINE_LENGTH = 45;
    private const int DEFAULT_WORKERS = 8;

    /** @var array<string, int> */
    private array $pathToPathId = [];

    /** @var array<int, string> */
    private array $pathIdToPath = [];

    public function parse(string $inputPath, string $outputPath): void
    {
        $this->buildVisitDictionary();

        $fileSize = filesize($inputPath);

        if ($fileSize === 0) {
            file_put_contents($outputPath, "{}\n");

            return;
        }

        $workerCount = $this->resolveWorkerCount($fileSize);
        $ranges = $this->splitRanges($inputPath, $fileSize, $workerCount);
        $sessionId = getmypid();

        $segmentKeys = [];
        $childPids = [];
        $lastRangeIndex = count($ranges) - 1;

        for ($index = 0; $index < $lastRangeIndex; $index++) {
            $segmentKeys[$index] = ($sessionId << 8) + $index + 1;
            $pid = pcntl_fork();

            if ($pid === 0) {
                [$pathOrder, $dates, $counts] = $this->parseRange($inputPath, $ranges[$index][0], $ranges[$index][1]);
                $this->saveSnapshot($segmentKeys[$index], $pathOrder, $dates, $counts);
                exit(0);
            }

            $childPids[$index] = $pid;
        }

        [$parentPathOrder, $parentDates, $parentCounts] = $this->parseRange(
            $inputPath,
            $ranges[$lastRangeIndex][0],
            $ranges[$lastRangeIndex][1],
        );

        $mergedPathOrder = [];
        $mergedDates = [];
        $mergedCounts = [];

        foreach ($childPids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        foreach ($segmentKeys as $segmentKey) {
            [$pathOrder, $dates, $counts] = $this->loadSnapshot($segmentKey);
            $this->mergeState($mergedPathOrder, $mergedDates, $mergedCounts, $pathOrder, $dates, $counts);
        }

        $this->mergeState($mergedPathOrder, $mergedDates, $mergedCounts, $parentPathOrder, $parentDates, $parentCounts);

        $this->writeOutput($outputPath, $mergedPathOrder, $mergedDates, $mergedCounts);
    }

    private function buildVisitDictionary(): void
    {
        $pathId = 0;

        foreach (Visit::all() as $visit) {
            $path = substr($visit->uri, self::URI_PREFIX_LENGTH);

            if (isset($this->pathToPathId[$path])) {
                continue;
            }

            $this->pathToPathId[$path] = $pathId;
            $this->pathIdToPath[$pathId] = $path;
            $pathId++;
        }
    }

    private function resolveWorkerCount(int $fileSize): int
    {
        if ($fileSize < 8_000_000) {
            return 1;
        }

        return min(self::DEFAULT_WORKERS, max(1, intdiv($fileSize, 128_000_000)));
    }

    /** @return list<array{0:int,1:int}> */
    private function splitRanges(string $inputPath, int $fileSize, int $workerCount): array
    {
        if ($workerCount === 1) {
            return [[0, $fileSize]];
        }

        $handle = fopen($inputPath, 'rb');

        $chunkSize = intdiv($fileSize, $workerCount);
        $ranges = [];
        $start = 0;

        for ($index = 1; $index < $workerCount; $index++) {
            fseek($handle, $index * $chunkSize);
            fgets($handle);
            $end = ftell($handle);

            $ranges[] = [$start, $end];
            $start = $end;
        }

        fclose($handle);

        $ranges[] = [$start, $fileSize];

        return $ranges;
    }

    /** @return array{0: list<int>, 1: array<int, string>, 2: array<int, array<int, int>>} */
    private function parseRange(string $inputPath, int $start, int $end): array
    {
        $seenPathIds = [];
        $pathOrder = [];
        $dateToId = [];
        $dates = [];
        $nextDateId = 0;
        $counts = [];

        $handle = fopen($inputPath, 'rb');

        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);

        $remaining = $end - $start;
        $remainder = '';
        $pathToPathId = $this->pathToPathId;

        while ($remaining > 0) {
            $readSize = min($remaining, self::READ_CHUNK_SIZE);
            $chunk = fread($handle, $readSize);

            if ($chunk === '') {
                break;
            }

            $remaining -= strlen($chunk);

            if ($remainder !== '') {
                $chunk = $remainder . $chunk;
            }

            $lastNewline = strrpos($chunk, "\n");

            if ($lastNewline === false) {
                $remainder = $chunk;
                continue;
            }

            $remainder = substr($chunk, $lastNewline + 1);
            $this->accumulateChunk(
                substr($chunk, 0, $lastNewline + 1),
                $pathToPathId,
                $seenPathIds,
                $pathOrder,
                $dateToId,
                $dates,
                $nextDateId,
                $counts,
            );
        }

        if ($remainder !== '') {
            $this->accumulateChunk(
                $remainder,
                $pathToPathId,
                $seenPathIds,
                $pathOrder,
                $dateToId,
                $dates,
                $nextDateId,
                $counts,
            );
        }

        fclose($handle);

        return [$pathOrder, $dates, $counts];
    }

    /**
     * @param array<string, int> $pathToPathId
     * @param array<int, true> $seenPathIds
     * @param list<int> $pathOrder
     * @param array<string, int> $dateToId
     * @param array<int, string> $dates
     * @param array<int, array<int, int>> $counts
     */
    private function accumulateChunk(
        string $chunk,
        array $pathToPathId,
        array &$seenPathIds,
        array &$pathOrder,
        array &$dateToId,
        array &$dates,
        int &$nextDateId,
        array &$counts,
    ): void {
        $offset = 0;
        $length = strlen($chunk);

        while ($offset < $length) {
            $newlinePos = strpos($chunk, "\n", $offset);

            if ($newlinePos === false) {
                break;
            }

            if ($newlinePos - $offset < self::MIN_LINE_LENGTH) {
                $offset = $newlinePos + 1;
                continue;
            }

            $pathId = $pathToPathId[
                substr(
                    $chunk,
                    $offset + self::URI_PREFIX_LENGTH,
                    $newlinePos - $offset - self::MIN_LINE_LENGTH,
                )
            ] ?? null;

            if ($pathId === null) {
                $offset = $newlinePos + 1;
                continue;
            }

            if (!isset($seenPathIds[$pathId])) {
                $seenPathIds[$pathId] = true;
                $pathOrder[] = $pathId;
            }

            $date = substr($chunk, $newlinePos - self::TIMESTAMP_LENGTH, self::DATE_LENGTH);
            $dateId = $dateToId[$date] ?? $nextDateId;

            if ($dateId === $nextDateId) {
                $dateToId[$date] = $dateId;
                $dates[$dateId] = $date;
                $nextDateId++;
            }

            $counts[$pathId][$dateId] = ($counts[$pathId][$dateId] ?? 0) + 1;
            $offset = $newlinePos + 1;
        }
    }

    /**
     * @param list<int> $pathOrder
     * @param array<int, string> $dates
     * @param array<int, array<int, int>> $counts
     */
    private function saveSnapshot(int $segmentKey, array $pathOrder, array $dates, array $counts): void
    {
        $tripleCount = 0;

        foreach ($counts as $pathCounts) {
            $tripleCount += count($pathCounts);
        }

        $payload = pack('V', count($pathOrder));

        foreach ($pathOrder as $pathId) {
            $payload .= pack('V', $pathId);
        }

        $payload .= pack('V', count($dates));

        foreach ($dates as $date) {
            $payload .= $date;
        }

        $payload .= pack('V', $tripleCount);

        foreach ($counts as $pathId => $pathCounts) {
            foreach ($pathCounts as $dateId => $count) {
                $payload .= pack('V3', $pathId, $dateId, $count);
            }
        }

        $segment = shmop_open($segmentKey, 'n', 0644, strlen($payload));
        shmop_write($segment, $payload, 0);
    }

    /** @return array{0: list<int>, 1: array<int, string>, 2: array<int, array<int, int>>} */
    private function loadSnapshot(int $segmentKey): array
    {
        $segment = shmop_open($segmentKey, 'a', 0, 0);
        $payload = shmop_read($segment, 0, shmop_size($segment));
        shmop_delete($segment);

        $offset = 0;
        $pathCount = unpack('Vcount', substr($payload, $offset, 4))['count'];
        $offset += 4;
        $pathOrder = [];

        for ($index = 0; $index < $pathCount; $index++) {
            $pathOrder[] = unpack('VpathId', substr($payload, $offset, 4))['pathId'];
            $offset += 4;
        }

        $dateCount = unpack('Vcount', substr($payload, $offset, 4))['count'];
        $offset += 4;
        $dates = [];

        for ($index = 0; $index < $dateCount; $index++) {
            $dates[$index] = substr($payload, $offset, self::DATE_LENGTH);
            $offset += self::DATE_LENGTH;
        }

        $tripleCount = unpack('Vcount', substr($payload, $offset, 4))['count'];
        $offset += 4;
        $counts = [];

        for ($index = 0; $index < $tripleCount; $index++) {
            $data = unpack('VpathId/VdateId/Vcount', substr($payload, $offset, 12));
            $offset += 12;
            $counts[$data['pathId']][$data['dateId']] = $data['count'];
        }

        return [$pathOrder, $dates, $counts];
    }

    /**
     * @param list<int> $mergedPathOrder
     * @param array<int, string> $mergedDates
     * @param array<int, array<int, int>> $mergedCounts
     * @param list<int> $pathOrder
     * @param array<int, string> $dates
     * @param array<int, array<int, int>> $counts
     */
    private function mergeState(
        array &$mergedPathOrder,
        array &$mergedDates,
        array &$mergedCounts,
        array $pathOrder,
        array $dates,
        array $counts,
    ): void {
        $seenMergedPathIds = array_fill_keys($mergedPathOrder, true);

        foreach ($pathOrder as $pathId) {
            if (isset($seenMergedPathIds[$pathId])) {
                continue;
            }

            $seenMergedPathIds[$pathId] = true;
            $mergedPathOrder[] = $pathId;
        }

        $globalDateIds = array_flip($mergedDates);
        $nextGlobalDateId = count($mergedDates);
        $localToGlobalDateId = [];

        foreach ($dates as $localDateId => $date) {
            $globalDateId = $globalDateIds[$date] ?? $nextGlobalDateId;

            if ($globalDateId === $nextGlobalDateId) {
                $mergedDates[$globalDateId] = $date;
                $globalDateIds[$date] = $globalDateId;
                $nextGlobalDateId++;
            }

            $localToGlobalDateId[$localDateId] = $globalDateId;
        }

        foreach ($counts as $pathId => $pathCounts) {
            foreach ($pathCounts as $localDateId => $count) {
                $globalDateId = $localToGlobalDateId[$localDateId];
                $mergedCounts[$pathId][$globalDateId] = ($mergedCounts[$pathId][$globalDateId] ?? 0) + $count;
            }
        }
    }

    /**
     * @param list<int> $pathOrder
     * @param array<int, string> $dates
     * @param array<int, array<int, int>> $counts
     */
    private function writeOutput(string $outputPath, array $pathOrder, array $dates, array $counts): void
    {
        $orderedDates = $dates;
        asort($orderedDates, SORT_STRING);
        $orderedDateIds = array_keys($orderedDates);

        $handle = fopen($outputPath, 'wb');

        stream_set_write_buffer($handle, self::WRITE_BUFFER_SIZE);

        fwrite($handle, '{');

        $firstPath = true;

        foreach ($pathOrder as $pathId) {
            if (!isset($counts[$pathId])) {
                continue;
            }

            $path = $this->pathIdToPath[$pathId];
            $buffer = $firstPath ? '' : ',';
            $firstPath = false;
            $buffer .= "\n    \"" . str_replace('/', '\\/', $path) . "\": {";

            $firstDate = true;

            foreach ($orderedDateIds as $dateId) {
                if (!isset($counts[$pathId][$dateId])) {
                    continue;
                }

                $buffer .= $firstDate ? "\n" : ",\n";
                $firstDate = false;
                $buffer .= "        \"{$dates[$dateId]}\": {$counts[$pathId][$dateId]}";
            }

            $buffer .= "\n    }";
            fwrite($handle, $buffer);
        }

        fwrite($handle, "\n}");
        fclose($handle);
    }
}
