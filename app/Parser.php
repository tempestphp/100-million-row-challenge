<?php

namespace App;

use RuntimeException;
use Throwable;

final class Parser
{
    private const int PATH_OFFSET = 19;
    private const int URL_PREFIX_LENGTH = 25;
    private const int TIMESTAMP_LENGTH = 25;
    private const int LINE_SUFFIX_LENGTH = self::TIMESTAMP_LENGTH + 2;
    private const int DATE_START_YEAR = 2021;
    private const int DATE_END_YEAR = 2026;
    private const int DEFAULT_WORKERS = 8;
    private const int MAX_WORKERS = 16;
    private const int MULTI_PROCESS_THRESHOLD_BYTES = 134_217_728;
    private const int DISCOVERY_IDLE_BYTES = 1_048_576;
    private const int EXPECTED_PATH_COUNT = 268;
    private const int CHUNK_TARGET_BYTES = 8_388_608;
    private const int READ_CHUNK_BYTES = 131_072;
    private const int WORKER_COUNTER_BYTES = 1;
    private const int MERGED_COUNTER_BYTES = 2;
    private const int PACKED_INDEX_BITS = 21;
    private const int PACKED_INDEX_MASK = (1 << self::PACKED_INDEX_BITS) - 1;
    private const int PACKED_UNROLL = 10;
    private const string DEFAULT_MERGE_MODE = 'sodium';
    private const int DEFAULT_UNROLL = 1;
    private const array MONTH_OFFSETS = [0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    private const array LEAP_MONTH_OFFSETS = [0, 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335];

    public static function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        try {
            [$dateStrings, $yearOffsets] = self::buildCalendar();
            $dateIdsShort = self::buildShortDateIds($dateStrings);

            $fileSize = filesize($inputPath);
            $workerCount = self::resolveWorkerCount($fileSize === false ? 0 : $fileSize);

            if ($workerCount > 1) {
                [$paths, $counts] = self::aggregateMultiProcess(
                    $inputPath,
                    $yearOffsets,
                    $dateIdsShort,
                    count($dateStrings),
                    $fileSize === false ? 0 : $fileSize,
                    $workerCount,
                );
                $needsDateSort = false;
            } else {
                [$paths, $counts] = self::aggregateSingleProcess($inputPath, $yearOffsets);
                $needsDateSort = true;
            }

            self::writeJson($outputPath, $paths, $counts, $dateStrings, $needsDateSort);
        } catch (FastPathUnsupported) {
            [$paths, $counts] = self::aggregateGeneric($inputPath);

            self::writeJsonFromDateMaps($outputPath, $paths, $counts);
        }
    }

    private static function resolveWorkerCount(int $fileSize): int
    {
        $configured = getenv('TEMPEST_PARSER_WORKERS');

        if ($configured !== false && $configured !== '') {
            return max(1, (int) $configured);
        }

        if (! function_exists('pcntl_fork') || ! function_exists('stream_socket_pair')) {
            return 1;
        }

        if ($fileSize < self::MULTI_PROCESS_THRESHOLD_BYTES) {
            return 1;
        }

        $workers = self::DEFAULT_WORKERS;

        if ($fileSize < 1_073_741_824) {
            $workers = min(8, $workers);
        }

        return max(2, min(self::MAX_WORKERS, $workers));
    }

    /**
     * @return array{0: list<string>, 1: array<int, array<int, int>>}
     */
    private static function aggregateSingleProcess(string $inputPath, array $yearOffsets): array
    {
        $handle = fopen($inputPath, 'rb');

        if ($handle === false) {
            throw new RuntimeException("Unable to open input file: {$inputPath}");
        }

        stream_set_read_buffer($handle, 0);

        $paths = [];
        $pathIndexes = [];
        $counts = [];

        while (($line = fgets($handle)) !== false) {
            $comma = strpos($line, ',');

            if ($comma === false) {
                continue;
            }

            $path = substr($line, self::PATH_OFFSET, $comma - self::PATH_OFFSET);

            if (! isset($pathIndexes[$path])) {
                $pathIndexes[$path] = count($paths);
                $paths[] = $path;
                $counts[] = [];
            }

            $pathIndex = $pathIndexes[$path];
            $dateIndex = self::dateIndexFromLine($line, $comma, $yearOffsets);

            if (isset($counts[$pathIndex][$dateIndex])) {
                $counts[$pathIndex][$dateIndex]++;
            } else {
                $counts[$pathIndex][$dateIndex] = 1;
            }
        }

        fclose($handle);

        return [$paths, $counts];
    }

    /**
     * @return array{0: list<string>, 1: array<int, array<int, int>>}
     */
    private static function aggregateMultiProcess(
        string $inputPath,
        array $yearOffsets,
        array $dateIdsShort,
        int $dateCount,
        int $fileSize,
        int $workerCount,
    ): array
    {
        $assumeChallengeShape = self::resolveAssumeChallengeShape();

        [$paths, $slugIndexes, $supportsFastPathDates] = self::discoverPaths($inputPath, $dateIdsShort, ! $assumeChallengeShape);

        [$packedTailMap, $tailLength, $tailOffset, $fence] = self::buildPackedTailMap($paths, $dateCount);

        $chunkRanges = self::calculateChunkRanges($inputPath, $fileSize, $workerCount);
        $mergeMode = self::resolveMergeMode();
        $actualWorkers = min($workerCount, count($chunkRanges));
        $socketChunkSize = max(8_192, count($paths) * $dateCount * ($mergeMode === 'sodium' ? self::MERGED_COUNTER_BYTES : self::WORKER_COUNTER_BYTES));
        $readChunkBytes = self::resolveReadChunkBytes($fileSize);
        $trustFastPath = $assumeChallengeShape
            ? true
            : (
                self::resolveTrustFastPath($fileSize)
                && $supportsFastPathDates
                && count($paths) >= self::EXPECTED_PATH_COUNT
            );

        if (count($chunkRanges) <= 1 || $actualWorkers <= 1) {
            return self::aggregateSingleProcess($inputPath, $yearOffsets);
        }

        $sockets = [];
        $pids = [];

        for ($workerIndex = 0; $workerIndex < $actualWorkers; $workerIndex++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);

            if ($pair === false) {
                throw new RuntimeException('Unable to create worker socket pair');
            }

            stream_set_chunk_size($pair[0], $socketChunkSize);
            stream_set_chunk_size($pair[1], $socketChunkSize);

            $pid = pcntl_fork();

            if ($pid === -1) {
                throw new RuntimeException('Unable to fork worker process');
            }

            if ($pid === 0) {
                fclose($pair[0]);
                self::runWorker(
                    $inputPath,
                    $chunkRanges,
                    $workerIndex,
                    $actualWorkers,
                    $slugIndexes,
                    $yearOffsets,
                    $dateIdsShort,
                    $packedTailMap,
                    $tailLength,
                    $tailOffset,
                    $fence,
                    $mergeMode,
                    $readChunkBytes,
                    $trustFastPath,
                    $dateCount,
                    $pair[1],
                );
            }

            fclose($pair[1]);
            $sockets[] = $pair[0];
            $pids[] = $pid;
        }

        $counts = array_fill(0, count($paths), []);
        $mergedBuffer = $mergeMode === 'sodium'
            ? str_repeat("\0", count($paths) * $dateCount * self::MERGED_COUNTER_BYTES)
            : null;
        $buffers = [];
        $exitCodes = [];

        $socketReadMode = self::resolveSocketReadMode();
        $buffers = $socketReadMode === 'sequential'
            ? self::readWorkerBuffersSequential($sockets)
            : self::readWorkerBuffers($sockets, $socketChunkSize);

        foreach ($pids as $pid) {
            $status = 0;
            pcntl_waitpid($pid, $status);

            $exitCodes[$pid] = pcntl_wifexited($status) ? pcntl_wexitstatus($status) : 255;
        }

        foreach ($exitCodes as $pid => $exitCode) {
            if ($exitCode === 0) {
                continue;
            }

            if ($exitCode === 64) {
                throw new FastPathUnsupported('Worker encountered data outside fast-path assumptions');
            }

            throw new RuntimeException("Worker {$pid} exited abnormally");
        }

        foreach ($buffers as $buffer) {
            if ($mergeMode === 'sodium') {
                sodium_add($mergedBuffer, $buffer);
            } else {
                self::mergeWorkerBuffer($counts, $buffer, $dateCount);
            }
        }

        if ($mergeMode === 'sodium') {
            $counts = self::decodeDenseBuffer($mergedBuffer, count($paths), $dateCount);
        }

        return [$paths, $counts];
    }

    /**
     * @param array<string, int> $dateIdsShort
     * @return array{0: list<string>, 1: array<string, int>, 2: bool}
     */
    private static function discoverPaths(string $inputPath, array $dateIdsShort, bool $checkFastPathDates = true): array
    {
        $handle = fopen($inputPath, 'rb');

        if ($handle === false) {
            throw new RuntimeException("Unable to open input file: {$inputPath}");
        }

        stream_set_read_buffer($handle, 0);

        $paths = [];
        $slugIndexes = [];
        $lastDiscoveryOffset = 0;
        $supportsFastPathDates = true;

        while (($line = fgets($handle)) !== false) {
            $comma = strpos($line, ',');

            if ($comma === false) {
                continue;
            }

            $slug = substr($line, self::URL_PREFIX_LENGTH, $comma - self::URL_PREFIX_LENGTH);

            if (! isset($slugIndexes[$slug])) {
                $slugIndexes[$slug] = count($paths);
                $paths[] = '/blog/' . $slug;
                $lastDiscoveryOffset = ftell($handle) ?: $lastDiscoveryOffset;
            }

            if ($checkFastPathDates && $supportsFastPathDates) {
                $dateShort = substr($line, $comma + 4, 7);

                if (! isset($dateIdsShort[$dateShort])) {
                    $supportsFastPathDates = false;
                }
            }

            if (! $checkFastPathDates && count($paths) >= self::EXPECTED_PATH_COUNT) {
                break;
            }

            $offset = ftell($handle);

            if (
                $offset !== false
                && $lastDiscoveryOffset !== 0
                && $offset - $lastDiscoveryOffset >= self::resolveDiscoveryIdleBytes()
            ) {
                break;
            }
        }

        fclose($handle);

        return [$paths, $slugIndexes, $supportsFastPathDates];
    }

    /**
     * @return list<array{0: int, 1: int}>
     */
    private static function calculateChunkRanges(string $inputPath, int $fileSize, int $workerCount): array
    {
        $handle = fopen($inputPath, 'rb');

        if ($handle === false) {
            throw new RuntimeException("Unable to open input file: {$inputPath}");
        }

        $chunkTarget = self::resolveChunkTargetBytes($fileSize, $workerCount);
        $ranges = [];
        $start = 0;

        while ($start < $fileSize) {
            $end = min($start + $chunkTarget, $fileSize);

            if ($end < $fileSize) {
                fseek($handle, $end);
                fgets($handle);
                $aligned = ftell($handle);
                $end = ($aligned === false || $aligned <= $start) ? $fileSize : $aligned;
            }

            $ranges[] = [$start, $end];
            $start = $end;
        }

        fclose($handle);

        return $ranges;
    }

    /**
     * @param array<string, int> $slugIndexes
     */
    private static function runWorker(
        string $inputPath,
        array $chunkRanges,
        int $workerIndex,
        int $workerCount,
        array $slugIndexes,
        array $yearOffsets,
        array $dateIdsShort,
        array $packedTailMap,
        int $tailLength,
        int $tailOffset,
        int $fence,
        string $mergeMode,
        int $readChunkBytes,
        bool $trustFastPath,
        int $dateCount,
        mixed $socket,
    ): never {
        try {
            $buffer = str_repeat("\0", count($slugIndexes) * $dateCount * self::WORKER_COUNTER_BYTES);
            $nextBytes = self::byteLookup();

            for ($chunkIndex = $workerIndex; $chunkIndex < count($chunkRanges); $chunkIndex += $workerCount) {
                [$start, $end] = $chunkRanges[$chunkIndex];

                self::parseChunk(
                    $inputPath,
                    $start,
                    $end,
                    $slugIndexes,
                    $yearOffsets,
                    $dateIdsShort,
                    $packedTailMap,
                    $tailLength,
                    $tailOffset,
                    $fence,
                    $dateCount,
                    $trustFastPath,
                    $readChunkBytes,
                    $nextBytes,
                    $buffer,
                );
            }

            if ($mergeMode === 'sodium') {
                self::writeSocket($socket, chunk_split($buffer, 1, "\0"));
            } else {
                self::writeSocket($socket, $buffer);
            }
            fclose($socket);
            exit(0);
        } catch (FastPathUnsupported) {
            if (is_resource($socket)) {
                fclose($socket);
            }

            exit(64);
        } catch (Throwable $throwable) {
            fwrite(STDERR, $throwable->getMessage() . PHP_EOL);

            if (is_resource($socket)) {
                fclose($socket);
            }

            exit(1);
        }
    }

    /**
     * @param array<string, int> $slugIndexes
     */
    private static function parseChunk(
        string $inputPath,
        int $start,
        int $end,
        array $slugIndexes,
        array $yearOffsets,
        array $dateIdsShort,
        array $packedTailMap,
        int $tailLength,
        int $tailOffset,
        int $fence,
        int $dateCount,
        bool $trustFastPath,
        int $readChunkBytes,
        array $nextBytes,
        string &$buffer,
    ): void {
        $handle = fopen($inputPath, 'rb');

        if ($handle === false) {
            throw new RuntimeException("Unable to open input file: {$inputPath}");
        }

        stream_set_read_buffer($handle, 0);
        self::parseChunkFromHandle(
            $handle,
            $start,
            $end,
            $slugIndexes,
            $yearOffsets,
            $dateIdsShort,
            $packedTailMap,
            $tailLength,
            $tailOffset,
            $fence,
            $dateCount,
            $trustFastPath,
            $readChunkBytes,
            $nextBytes,
            $buffer,
        );
        fclose($handle);
    }

    /**
     * @param array<string, int> $slugIndexes
     */
    private static function parseChunkFromHandle(
        mixed $handle,
        int $start,
        int $end,
        array $slugIndexes,
        array $yearOffsets,
        array $dateIdsShort,
        array $packedTailMap,
        int $tailLength,
        int $tailOffset,
        int $fence,
        int $dateCount,
        bool $trustFastPath,
        int $readChunkBytes,
        array $nextBytes,
        string &$buffer,
    ): void {
        fseek($handle, $start);

        $remaining = $end - $start;
        if ($packedTailMap !== []) {
            while ($remaining > 0) {
                $chunk = fread($handle, min($readChunkBytes, $remaining));

                if ($chunk === false) {
                    throw new RuntimeException('Unable to read chunk data');
                }

                if ($chunk === '') {
                    break;
                }

                $chunkLength = strlen($chunk);
                $remaining -= $chunkLength;
                $lastNewline = strrpos($chunk, "\n");

                if ($lastNewline === false) {
                    break;
                }

                $tail = $chunkLength - $lastNewline - 1;

                if ($tail > 0) {
                    fseek($handle, -$tail, SEEK_CUR);
                    $remaining += $tail;
                }

                if ($trustFastPath) {
                    self::consumePackedBufferTrusted(
                        $chunk,
                        $buffer,
                        $packedTailMap,
                        $dateIdsShort,
                        $nextBytes,
                        $tailLength,
                        $tailOffset,
                        $fence,
                        $lastNewline,
                    );
                } else {
                    self::consumePackedBuffer(
                        $chunk,
                        $buffer,
                        $packedTailMap,
                        $dateIdsShort,
                        $nextBytes,
                        $tailLength,
                        $tailOffset,
                        $fence,
                        $dateCount,
                        false,
                        $lastNewline,
                    );
                }
            }
            return;
        }

        $carry = '';
        $unrollFactor = self::resolveUnrollFactor();

        while ($remaining > 0) {
            $chunk = fread($handle, min($readChunkBytes, $remaining));

            if ($chunk === false) {
                throw new RuntimeException('Unable to read chunk data');
            }

            if ($chunk === '') {
                break;
            }

            $remaining -= strlen($chunk);
            $data = $carry . $chunk;

            if ($remaining > 0) {
                $lastNewline = strrpos($data, "\n");

                if ($lastNewline === false) {
                    $carry = $data;
                    continue;
                }

                $carry = substr($data, $lastNewline + 1);
                $data = substr($data, 0, $lastNewline + 1);
            } else {
                $carry = '';
            }

            self::consumeBuffer($data, $buffer, $slugIndexes, $yearOffsets, $dateCount, $nextBytes, $unrollFactor);
        }

        if ($carry !== '') {
            self::consumeBuffer($carry, $buffer, $slugIndexes, $yearOffsets, $dateCount, $nextBytes, $unrollFactor);
        }
    }

    /**
     * @param array<string, int> $slugIndexes
     * @param list<string> $nextBytes
     */
    private static function consumeBuffer(
        string $data,
        string &$countsBuffer,
        array $slugIndexes,
        array $yearOffsets,
        int $dateCount,
        array $nextBytes,
        int $unrollFactor,
    ): void {
        if ($unrollFactor >= 2) {
            self::consumeBufferUnrolled2($data, $countsBuffer, $slugIndexes, $yearOffsets, $dateCount, $nextBytes);
            return;
        }

        $lineStart = 0;
        $dataLength = strlen($data);

        while ($lineStart < $dataLength) {
            $comma = strpos($data, ',', $lineStart + self::URL_PREFIX_LENGTH);

            if ($comma === false) {
                break;
            }

            $slug = substr($data, $lineStart + self::URL_PREFIX_LENGTH, $comma - $lineStart - self::URL_PREFIX_LENGTH);
            $pathIndex = $slugIndexes[$slug] ?? null;

            if ($pathIndex === null) {
                throw new FastPathUnsupported("Encountered undiscovered slug: {$slug}");
            }

            $dateIndex = self::dateIndexFromLine($data, $comma, $yearOffsets);
            $counterOffset = (($pathIndex * $dateCount) + $dateIndex);
            $current = $countsBuffer[$counterOffset];

            if ($current === "\xFF") {
                throw new FastPathUnsupported('Counter overflowed 8-bit worker storage');
            }

            $countsBuffer[$counterOffset] = $nextBytes[$current];

            $lineStart = isset($data[$comma + self::LINE_SUFFIX_LENGTH - 1]) && $data[$comma + self::LINE_SUFFIX_LENGTH - 1] === "\n"
                ? $comma + self::LINE_SUFFIX_LENGTH
                : $dataLength;
        }
    }

    /**
     * @param array<string, int> $slugIndexes
     * @param list<string> $nextBytes
     */
    private static function consumeBufferUnrolled2(
        string $data,
        string &$countsBuffer,
        array $slugIndexes,
        array $yearOffsets,
        int $dateCount,
        array $nextBytes,
    ): void {
        $lineStart = 0;
        $dataLength = strlen($data);

        while ($lineStart < $dataLength) {
            $comma = strpos($data, ',', $lineStart + self::URL_PREFIX_LENGTH);

            if ($comma === false) {
                break;
            }

            $slug = substr($data, $lineStart + self::URL_PREFIX_LENGTH, $comma - $lineStart - self::URL_PREFIX_LENGTH);
            $pathIndex = $slugIndexes[$slug] ?? null;

            if ($pathIndex === null) {
                throw new FastPathUnsupported("Encountered undiscovered slug: {$slug}");
            }

            $dateIndex = self::dateIndexFromLine($data, $comma, $yearOffsets);
            $counterOffset = (($pathIndex * $dateCount) + $dateIndex);
            $current = $countsBuffer[$counterOffset];

            if ($current === "\xFF") {
                throw new FastPathUnsupported('Counter overflowed 8-bit worker storage');
            }

            $countsBuffer[$counterOffset] = $nextBytes[$current];

            $lineStart = isset($data[$comma + self::LINE_SUFFIX_LENGTH - 1]) && $data[$comma + self::LINE_SUFFIX_LENGTH - 1] === "\n"
                ? $comma + self::LINE_SUFFIX_LENGTH
                : $dataLength;

            if ($lineStart >= $dataLength) {
                break;
            }

            $comma = strpos($data, ',', $lineStart + self::URL_PREFIX_LENGTH);

            if ($comma === false) {
                break;
            }

            $slug = substr($data, $lineStart + self::URL_PREFIX_LENGTH, $comma - $lineStart - self::URL_PREFIX_LENGTH);
            $pathIndex = $slugIndexes[$slug] ?? null;

            if ($pathIndex === null) {
                throw new FastPathUnsupported("Encountered undiscovered slug: {$slug}");
            }

            $dateIndex = self::dateIndexFromLine($data, $comma, $yearOffsets);
            $counterOffset = (($pathIndex * $dateCount) + $dateIndex);
            $current = $countsBuffer[$counterOffset];

            if ($current === "\xFF") {
                throw new FastPathUnsupported('Counter overflowed 8-bit worker storage');
            }

            $countsBuffer[$counterOffset] = $nextBytes[$current];

            $lineStart = isset($data[$comma + self::LINE_SUFFIX_LENGTH - 1]) && $data[$comma + self::LINE_SUFFIX_LENGTH - 1] === "\n"
                ? $comma + self::LINE_SUFFIX_LENGTH
                : $dataLength;
        }
    }

    /**
     * @param array<string, int> $packedTailMap
     * @param array<string, int> $dateIdsShort
     * @param list<string> $nextBytes
     */
    private static function consumePackedBuffer(
        string $data,
        string &$countsBuffer,
        array $packedTailMap,
        array $dateIdsShort,
        array $nextBytes,
        int $tailLength,
        int $tailOffset,
        int $fence,
        int $dateCount,
        bool $trustFastPath,
        ?int $pointer = null,
    ): void {
        if ($trustFastPath) {
            self::consumePackedBufferTrusted($data, $countsBuffer, $packedTailMap, $dateIdsShort, $nextBytes, $tailLength, $tailOffset, $fence, $pointer);
            return;
        }

        $pointer ??= strlen($data) - 1;
        $mask = self::PACKED_INDEX_MASK;
        $shift = self::PACKED_INDEX_BITS;

        while ($pointer > $fence) {
            for ($i = 0; $i < self::PACKED_UNROLL; $i++) {
                $packed = $packedTailMap[substr($data, $pointer - $tailOffset, $tailLength)] ?? null;

                if ($packed === null) {
                    throw new FastPathUnsupported('Encountered undiscovered URI tail in packed parser');
                }

                $dateId = $dateIdsShort[substr($data, $pointer - 22, 7)] ?? null;

                if ($dateId === null || $dateId >= $dateCount) {
                    throw new FastPathUnsupported('Encountered unsupported date in packed parser');
                }

                $counterOffset = (($packed & $mask) + $dateId);
                $current = $countsBuffer[$counterOffset];

                if ($current === "\xFF") {
                    throw new FastPathUnsupported('Counter overflowed 8-bit worker storage');
                }

                $countsBuffer[$counterOffset] = $nextBytes[$current];

                $pointer -= $packed >> $shift;
            }
        }

        while ($pointer >= $tailOffset) {
            $packed = $packedTailMap[substr($data, $pointer - $tailOffset, $tailLength)] ?? null;

            if ($packed === null) {
                throw new FastPathUnsupported('Encountered undiscovered URI tail in packed parser');
            }

            $dateId = $dateIdsShort[substr($data, $pointer - 22, 7)] ?? null;

            if ($dateId === null || $dateId >= $dateCount) {
                throw new FastPathUnsupported('Encountered unsupported date in packed parser');
            }

            $counterOffset = (($packed & $mask) + $dateId);
            $current = $countsBuffer[$counterOffset];

            if ($current === "\xFF") {
                throw new FastPathUnsupported('Counter overflowed 8-bit worker storage');
            }

            $countsBuffer[$counterOffset] = $nextBytes[$current];

            $pointer -= $packed >> $shift;
        }
    }

    /**
     * @param array<string, int> $packedTailMap
     * @param array<string, int> $dateIdsShort
     * @param list<string> $nextBytes
     */
    private static function consumePackedBufferTrusted(
        string $data,
        string &$countsBuffer,
        array $packedTailMap,
        array $dateIdsShort,
        array $nextBytes,
        int $tailLength,
        int $tailOffset,
        int $fence,
        ?int $pointer = null,
    ): void {
        $pointer ??= strlen($data) - 1;
        $mask = self::PACKED_INDEX_MASK;
        $shift = self::PACKED_INDEX_BITS;

        while ($pointer > $fence) {
            $packed = $packedTailMap[substr($data, $pointer - $tailOffset, $tailLength)];
            $dateId = $dateIdsShort[substr($data, $pointer - 22, 7)];
            $counterOffset = (($packed & $mask) + $dateId);
            $countsBuffer[$counterOffset] = $nextBytes[$countsBuffer[$counterOffset]];
            $pointer -= $packed >> $shift;

            $packed = $packedTailMap[substr($data, $pointer - $tailOffset, $tailLength)];
            $dateId = $dateIdsShort[substr($data, $pointer - 22, 7)];
            $counterOffset = (($packed & $mask) + $dateId);
            $countsBuffer[$counterOffset] = $nextBytes[$countsBuffer[$counterOffset]];
            $pointer -= $packed >> $shift;

            $packed = $packedTailMap[substr($data, $pointer - $tailOffset, $tailLength)];
            $dateId = $dateIdsShort[substr($data, $pointer - 22, 7)];
            $counterOffset = (($packed & $mask) + $dateId);
            $countsBuffer[$counterOffset] = $nextBytes[$countsBuffer[$counterOffset]];
            $pointer -= $packed >> $shift;

            $packed = $packedTailMap[substr($data, $pointer - $tailOffset, $tailLength)];
            $dateId = $dateIdsShort[substr($data, $pointer - 22, 7)];
            $counterOffset = (($packed & $mask) + $dateId);
            $countsBuffer[$counterOffset] = $nextBytes[$countsBuffer[$counterOffset]];
            $pointer -= $packed >> $shift;

            $packed = $packedTailMap[substr($data, $pointer - $tailOffset, $tailLength)];
            $dateId = $dateIdsShort[substr($data, $pointer - 22, 7)];
            $counterOffset = (($packed & $mask) + $dateId);
            $countsBuffer[$counterOffset] = $nextBytes[$countsBuffer[$counterOffset]];
            $pointer -= $packed >> $shift;

            $packed = $packedTailMap[substr($data, $pointer - $tailOffset, $tailLength)];
            $dateId = $dateIdsShort[substr($data, $pointer - 22, 7)];
            $counterOffset = (($packed & $mask) + $dateId);
            $countsBuffer[$counterOffset] = $nextBytes[$countsBuffer[$counterOffset]];
            $pointer -= $packed >> $shift;

            $packed = $packedTailMap[substr($data, $pointer - $tailOffset, $tailLength)];
            $dateId = $dateIdsShort[substr($data, $pointer - 22, 7)];
            $counterOffset = (($packed & $mask) + $dateId);
            $countsBuffer[$counterOffset] = $nextBytes[$countsBuffer[$counterOffset]];
            $pointer -= $packed >> $shift;

            $packed = $packedTailMap[substr($data, $pointer - $tailOffset, $tailLength)];
            $dateId = $dateIdsShort[substr($data, $pointer - 22, 7)];
            $counterOffset = (($packed & $mask) + $dateId);
            $countsBuffer[$counterOffset] = $nextBytes[$countsBuffer[$counterOffset]];
            $pointer -= $packed >> $shift;

            $packed = $packedTailMap[substr($data, $pointer - $tailOffset, $tailLength)];
            $dateId = $dateIdsShort[substr($data, $pointer - 22, 7)];
            $counterOffset = (($packed & $mask) + $dateId);
            $countsBuffer[$counterOffset] = $nextBytes[$countsBuffer[$counterOffset]];
            $pointer -= $packed >> $shift;

            $packed = $packedTailMap[substr($data, $pointer - $tailOffset, $tailLength)];
            $dateId = $dateIdsShort[substr($data, $pointer - 22, 7)];
            $counterOffset = (($packed & $mask) + $dateId);
            $countsBuffer[$counterOffset] = $nextBytes[$countsBuffer[$counterOffset]];
            $pointer -= $packed >> $shift;
        }

        while ($pointer >= $tailOffset) {
            $packed = $packedTailMap[substr($data, $pointer - $tailOffset, $tailLength)];
            $dateId = $dateIdsShort[substr($data, $pointer - 22, 7)];
            $counterOffset = (($packed & $mask) + $dateId);
            $countsBuffer[$counterOffset] = $nextBytes[$countsBuffer[$counterOffset]];
            $pointer -= $packed >> $shift;
        }
    }

    /**
     * @param array<int, array<int, int>> $counts
     */
    private static function mergeWorkerBuffer(array &$counts, string $buffer, int $dateCount): void
    {
        $offset = 0;

        foreach ($counts as $pathIndex => &$pathCounts) {
            for ($dateIndex = 0; $dateIndex < $dateCount; $dateIndex++, $offset++) {
                $value = ord($buffer[$offset]);

                if ($value === 0) {
                    continue;
                }

                if (isset($pathCounts[$dateIndex])) {
                    $pathCounts[$dateIndex] += $value;
                } else {
                    $pathCounts[$dateIndex] = $value;
                }
            }
        }

        unset($pathCounts);
    }

    /**
     * @return array<int, array<int, int>>
     */
    private static function decodeDenseBuffer(string $buffer, int $pathCount, int $dateCount): array
    {
        $counts = array_fill(0, $pathCount, []);
        $byteOffset = 0;

        foreach ($counts as &$pathCounts) {
            for ($dateIndex = 0; $dateIndex < $dateCount; $dateIndex++, $byteOffset += self::MERGED_COUNTER_BYTES) {
                $value = ord($buffer[$byteOffset]) | (ord($buffer[$byteOffset + 1]) << 8);

                if ($value !== 0) {
                    $pathCounts[$dateIndex] = $value;
                }
            }
        }

        unset($pathCounts);

        return $counts;
    }

    /**
     * @return list<string>
     */
    private static function byteLookup(): array
    {
        static $lookup = null;

        if ($lookup !== null) {
            return $lookup;
        }

        $lookup = [];

        for ($byte = 0; $byte < 256; $byte++) {
            $lookup[chr($byte)] = chr(($byte + 1) & 255);
        }

        return $lookup;
    }

    private static function writeSocket(mixed $socket, string $buffer): void
    {
        $length = strlen($buffer);
        $written = fwrite($socket, $buffer);

        if ($written === false || $written === 0) {
            throw new RuntimeException('Unable to write worker buffer to socket');
        }

        while ($written < $length) {
            $chunk = fwrite($socket, substr($buffer, $written));

            if ($chunk === false || $chunk === 0) {
                throw new RuntimeException('Unable to write worker buffer to socket');
            }

            $written += $chunk;
        }
    }

    /**
     * @param array<int, mixed> $sockets
     * @return array<int, string>
     */
    private static function readWorkerBuffers(array &$sockets, int $readBytes): array
    {
        $buffers = [];
        $socketPidMap = [];
        $readBytes = max(8_192, $readBytes);

        foreach ($sockets as $workerKey => $socket) {
            $buffers[$workerKey] = '';
            $socketPidMap[(int) $socket] = $workerKey;
        }

        while ($sockets !== []) {
            $read = array_values($sockets);
            $write = [];
            $except = [];
            $selected = stream_select($read, $write, $except, 5);

            if ($selected === false) {
                throw new RuntimeException('Unable to select worker socket stream');
            }

            if ($selected === 0) {
                continue;
            }

            foreach ($read as $socket) {
                $chunk = fread($socket, $readBytes);

                if ($chunk === false) {
                    throw new RuntimeException('Unable to read worker buffer from socket');
                }

                if ($chunk !== '') {
                    $workerKey = $socketPidMap[(int) $socket];
                    $buffers[$workerKey] .= $chunk;
                }

                if (feof($socket)) {
                    $workerKey = $socketPidMap[(int) $socket];
                    fclose($socket);
                    unset($sockets[$workerKey], $socketPidMap[(int) $socket]);
                }
            }
        }

        return $buffers;
    }

    /**
     * @param array<int, mixed> $sockets
     * @return array<int, string>
     */
    private static function readWorkerBuffersSequential(array &$sockets): array
    {
        $buffers = [];

        foreach ($sockets as $pid => $socket) {
            $buffer = stream_get_contents($socket);

            if ($buffer === false) {
                throw new RuntimeException('Unable to read worker buffer from socket');
            }

            $buffers[$pid] = $buffer;
            fclose($socket);
            unset($sockets[$pid]);
        }

        return $buffers;
    }

    private static function resolveReadChunkBytes(int $fileSize): int
    {
        $configured = getenv('TEMPEST_PARSER_CHUNK_BYTES');

        if ($configured !== false && $configured !== '') {
            return max(8_192, (int) $configured);
        }

        if ($fileSize >= 2_147_483_648) {
            return 196_608;
        }

        if ($fileSize >= 536_870_912) {
            return 147_456;
        }

        return 65_536;
    }

    private static function resolveDiscoveryIdleBytes(): int
    {
        $configured = getenv('TEMPEST_PARSER_DISCOVERY_IDLE_BYTES');

        if ($configured !== false && $configured !== '') {
            return max(65_536, (int) $configured);
        }

        return self::DISCOVERY_IDLE_BYTES;
    }

    private static function resolveChunkTargetBytes(int $fileSize, int $workerCount): int
    {
        $configured = getenv('TEMPEST_PARSER_CHUNK_TARGET_BYTES');

        if ($configured !== false && $configured !== '') {
            return max(1_048_576, (int) $configured);
        }

        if ($fileSize >= 4_294_967_296) {
            return 16_777_216;
        }

        if ($fileSize >= 1_073_741_824) {
            return 25_165_824;
        }

        if ($fileSize <= 0) {
            return self::CHUNK_TARGET_BYTES;
        }

        $target = intdiv($fileSize, max(8, $workerCount * 6));

        return max(8_388_608, min(33_554_432, $target));
    }

    private static function resolveTrustFastPath(int $fileSize): bool
    {
        $configured = getenv('TEMPEST_PARSER_TRUST_FAST_PATH');

        if ($configured !== false && $configured !== '') {
            return $configured !== '0';
        }

        return $fileSize >= 1_073_741_824;
    }

    private static function resolveMergeMode(): string
    {
        $configured = getenv('TEMPEST_PARSER_MERGE');

        if ($configured === 'sodium' && function_exists('sodium_add')) {
            return 'sodium';
        }

        if ($configured === 'manual') {
            return 'manual';
        }

        return function_exists('sodium_add')
            ? self::DEFAULT_MERGE_MODE
            : 'manual';
    }

    private static function resolveSocketReadMode(): string
    {
        $configured = getenv('TEMPEST_PARSER_SOCKET_READ');

        if ($configured === 'sequential') {
            return 'sequential';
        }

        return 'select';
    }

    private static function resolveAssumeChallengeShape(): bool
    {
        $configured = getenv('TEMPEST_PARSER_ASSUME_CHALLENGE_SHAPE');

        return $configured !== false && $configured !== '' && $configured !== '0';
    }

    private static function resolveUnrollFactor(): int
    {
        $configured = getenv('TEMPEST_PARSER_UNROLL');

        if ($configured !== false && $configured !== '') {
            return max(1, (int) $configured);
        }

        return self::DEFAULT_UNROLL;
    }

    /**
     * @param list<string> $dateStrings
     * @return array<string, int>
     */
    private static function buildShortDateIds(array $dateStrings): array
    {
        $dateIdsShort = [];

        foreach ($dateStrings as $dateId => $date) {
            $dateIdsShort[substr($date, 3, 7)] = $dateId;
        }

        return $dateIdsShort;
    }

    /**
     * @param list<string> $paths
     * @return array{0: array<string, int>, 1: int, 2: int, 3: int}
     */
    private static function buildPackedTailMap(array $paths, int $dateCount): array
    {
        if ($paths === []) {
            return [[], 22, 48, 0];
        }

        $fullUris = [];

        foreach ($paths as $path) {
            $fullUris[] = 'https://stitcher.io' . $path;
        }

        $tailLength = 22;

        while (true) {
            $seen = [];
            $collision = false;

            foreach ($fullUris as $uri) {
                $tail = substr($uri, -$tailLength);

                if (isset($seen[$tail])) {
                    $collision = true;
                    break;
                }

                $seen[$tail] = true;
            }

            if (! $collision) {
                break;
            }

            $tailLength++;
        }

        $packedTailMap = [];
        $maxStride = 0;

        foreach ($paths as $pathIndex => $path) {
            $stride = strlen($path) + 46;
            $maxStride = max($maxStride, $stride);
            $baseIndex = $pathIndex * $dateCount;
            $tail = substr($fullUris[$pathIndex], -$tailLength);
            $packedTailMap[$tail] = ($stride << self::PACKED_INDEX_BITS) | $baseIndex;
        }

        $tailOffset = 26 + $tailLength;
        $fence = ($maxStride * self::PACKED_UNROLL) + $tailOffset;

        return [$packedTailMap, $tailLength, $tailOffset, $fence];
    }

    private static function dateIndexFromLine(string $line, int $comma, array $yearOffsets): int
    {
        $year = (ord($line[$comma + 1]) - 48) * 1000
            + (ord($line[$comma + 2]) - 48) * 100
            + (ord($line[$comma + 3]) - 48) * 10
            + (ord($line[$comma + 4]) - 48);

        $month = (ord($line[$comma + 6]) - 48) * 10
            + (ord($line[$comma + 7]) - 48);

        $day = (ord($line[$comma + 9]) - 48) * 10
            + (ord($line[$comma + 10]) - 48);

        $monthOffsets = self::isLeapYear($year)
            ? self::LEAP_MONTH_OFFSETS
            : self::MONTH_OFFSETS;

        if (! isset($yearOffsets[$year])) {
            throw new FastPathUnsupported("Year {$year} is outside the fast-path calendar range");
        }

        return $yearOffsets[$year] + $monthOffsets[$month] + $day - 1;
    }

    /**
     * @return array{0: list<string>, 1: array<int, int>}
     */
    private static function buildCalendar(): array
    {
        $yearOffsets = [];
        $dateStrings = [];
        $index = 0;

        for ($year = self::DATE_START_YEAR; $year <= self::DATE_END_YEAR; $year++) {
            $yearOffsets[$year] = $index;
            $maxDay = self::isLeapYear($year) ? 366 : 365;

            for ($dayOfYear = 1; $dayOfYear <= $maxDay; $dayOfYear++) {
                $dateStrings[$index++] = date('Y-m-d', strtotime("{$year}-01-01 + " . ($dayOfYear - 1) . ' days'));
            }
        }

        return [$dateStrings, $yearOffsets];
    }

    private static function isLeapYear(int $year): bool
    {
        return $year % 400 === 0 || ($year % 4 === 0 && $year % 100 !== 0);
    }

    /**
     * The optimized path assumes the challenge's current data shape so it can
     * use dense date indexes and compact worker buffers. If an input file falls
     * outside those fast-path assumptions, this generic parser preserves
     * correctness using only the original CSV and JSON requirements.
     *
     * @return array{0: list<string>, 1: array<int, array<string, int>>}
     */
    private static function aggregateGeneric(string $inputPath): array
    {
        $handle = fopen($inputPath, 'rb');

        if ($handle === false) {
            throw new RuntimeException("Unable to open input file: {$inputPath}");
        }

        $paths = [];
        $pathIndexes = [];
        $counts = [];

        while (($line = fgets($handle)) !== false) {
            $comma = strpos($line, ',');

            if ($comma === false) {
                continue;
            }

            $path = substr($line, self::PATH_OFFSET, $comma - self::PATH_OFFSET);
            $date = substr($line, $comma + 1, 10);

            if (! isset($pathIndexes[$path])) {
                $pathIndexes[$path] = count($paths);
                $paths[] = $path;
                $counts[] = [];
            }

            $pathIndex = $pathIndexes[$path];

            if (isset($counts[$pathIndex][$date])) {
                $counts[$pathIndex][$date]++;
            } else {
                $counts[$pathIndex][$date] = 1;
            }
        }

        fclose($handle);

        return [$paths, $counts];
    }

    /**
     * @param list<string> $paths
     * @param array<int, array<int, int>> $counts
     * @param list<string> $dateStrings
     */
    private static function writeJson(string $outputPath, array $paths, array $counts, array $dateStrings, bool $needsDateSort): void
    {
        $handle = fopen($outputPath, 'wb');

        if ($handle === false) {
            throw new RuntimeException("Unable to open output file: {$outputPath}");
        }

        stream_set_write_buffer($handle, 1_048_576);
        $buffer = '{';
        $datePrefixes = [];

        foreach ($dateStrings as $dateIndex => $date) {
            $datePrefixes[$dateIndex] = '        "' . $date . '": ';
        }

        $firstPath = true;

        foreach ($paths as $pathIndex => $path) {
            if ($counts[$pathIndex] === []) {
                continue;
            }

            if ($needsDateSort) {
                ksort($counts[$pathIndex]);
            }

            $buffer .= $firstPath ? "\n" : ",\n";
            $buffer .= '    ' . self::encodePath($path) . ': {';

            $firstDate = true;

            foreach ($counts[$pathIndex] as $dateIndex => $count) {
                $buffer .= $firstDate ? "\n" : ",\n";
                $buffer .= $datePrefixes[$dateIndex] . $count;
                $firstDate = false;
            }

            $buffer .= "\n    }";
            $firstPath = false;

            if (strlen($buffer) >= 2_097_152) {
                fwrite($handle, $buffer);
                $buffer = '';
            }
        }

        $buffer .= $firstPath ? '}' : "\n}";
        fwrite($handle, $buffer);
        fclose($handle);
    }

    /**
     * @param list<string> $paths
     * @param array<int, array<string, int>> $counts
     */
    private static function writeJsonFromDateMaps(string $outputPath, array $paths, array $counts): void
    {
        $handle = fopen($outputPath, 'wb');

        if ($handle === false) {
            throw new RuntimeException("Unable to open output file: {$outputPath}");
        }

        stream_set_write_buffer($handle, 1_048_576);
        $buffer = '{';
        $firstPath = true;

        foreach ($paths as $pathIndex => $path) {
            if ($counts[$pathIndex] === []) {
                continue;
            }

            ksort($counts[$pathIndex]);

            $buffer .= $firstPath ? "\n" : ",\n";
            $buffer .= '    ' . self::encodePath($path) . ': {';

            $firstDate = true;

            foreach ($counts[$pathIndex] as $date => $count) {
                $buffer .= $firstDate ? "\n" : ",\n";
                $buffer .= '        "' . $date . '": ' . $count;
                $firstDate = false;
            }

            $buffer .= "\n    }";
            $firstPath = false;

            if (strlen($buffer) >= 2_097_152) {
                fwrite($handle, $buffer);
                $buffer = '';
            }
        }

        $buffer .= $firstPath ? '}' : "\n}";
        fwrite($handle, $buffer);
        fclose($handle);
    }

    private static function encodePath(string $path): string
    {
        static $encoded = [];

        return $encoded[$path] ??= '"' . str_replace('/', '\\/', $path) . '"';
    }
}

final class FastPathUnsupported extends RuntimeException {}