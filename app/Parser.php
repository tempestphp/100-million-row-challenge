<?php

namespace App;

use App\Commands\Visit;

use function array_fill;
use function chr;
use function count;
use function fclose;
use function fopen;
use function fread;
use function fseek;
use function fwrite;
use function gc_disable;
use function implode;
use function pcntl_fork;
use function sem_acquire;
use function sem_get;
use function sem_release;
use function sem_remove;
use function shmop_delete;
use function shmop_open;
use function shmop_read;
use function shmop_write;
use function str_repeat;
use function str_replace;
use function stream_set_read_buffer;
use function stream_socket_pair;
use function stream_set_blocking;
use function stream_select;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function unpack;
use function microtime;

final class Parser
{
    private const int WORKER_COUNT = 8;
    private const int READ_BUFFER_SIZE = 320*1024;// used to read the file
    private const int SMALL_CHUNK_SIZE = 64*1024;
    private const int CHUNKS_COUNT = self::WORKER_COUNT; // how many chunks will be used (try to find a balance between overhead and full cores utilization)
    private const int SAMPLE_SIZE = 2*1024*1024; //initial piece of logs that contains all unique URIs
    private const int URI_PREFIX_LENGTH = 19; //how long the URI before path
    private const int REMAINING_SIZE = 27; //the size of log row without URI
    private const int URI_TAIL_LENGTH = 22; //the minimal possible length that can identify URI
    private const int BIT_SHIFT = 24; // the part to encode main index (total amount of days * number of unique URIs)
    private const int INDEX_MASK = (1 << self::BIT_SHIFT) - 1; // the part to encode URI length

    public static function parse(string $inputPath, string $outputPath): void
    {
        $startTotal = microtime(true);
        gc_disable();

        $dateToId = [];
        $idToDate = [];
        $totalDates = 0;

        for ($year = 1; $year <= 6; $year++) {
            $monthStart = ($year === 1) ? 2 : 1;
            $monthEnd = ($year === 6) ? 3 : 12;
            for ($month = $monthStart; $month <= $monthEnd; $month++) {
                $daysInMonth = match ($month) {
                    2 => $year === 4 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31
                };
                $prefix = "$year-" . ($month < 10 ? '0' : '') . $month . "-";

                for ($day = 1; $day <= $daysInMonth; $day++) {
                    $dateString = $prefix . ($day < 10 ? '0' : '') . $day;
                    $dateToId[$dateString] = $totalDates;
                    $idToDate[$totalDates] = $dateString;
                    $totalDates++;
                }
            }
        }

        $allVisits = Visit::all();
        $maxUriLength = 0;
        $uriTailsData = [];
        foreach ($allVisits as $visit) {
            $length = strlen($visit->uri);
            if ($length > $maxUriLength) {
                $maxUriLength = $length;
            }
            $uriTailsData[substr($visit->uri, -self::URI_TAIL_LENGTH)] = $length + self::REMAINING_SIZE;
        }
        $chunkBackstep = $maxUriLength + self::REMAINING_SIZE;

        $tailToId = [];
        $idToTail = [];
        $tailToPathMap = [];

        $fileHandle = fopen($inputPath, 'rb');
        $sampleData = fread($fileHandle, self::SAMPLE_SIZE);
        $fileSize = fstat($fileHandle)['size'];
        $offset = 0;
        $tailCount = count($idToTail);

        while (($pos = strpos($sampleData, "\n", $offset)) !== false) {
            $separatorPos = strpos($sampleData, ',', $offset);
            if ($separatorPos !== false && $separatorPos < $pos) {
                $tailKey = substr($sampleData, $separatorPos - self::URI_TAIL_LENGTH, self::URI_TAIL_LENGTH);
                if (!isset($tailToId[$tailKey])) {
                    $tailToId[$tailKey] = $tailCount;
                    $idToTail[] = $tailKey;
                    $tailCount++;
                }
            }
            $offset = $pos + 1;
        }
        unset($sampleData);

        foreach ($allVisits as $visit) {
            $tailKey = substr($visit->uri, -self::URI_TAIL_LENGTH);
            $tailToPathMap[$tailKey] = substr($visit->uri, self::URI_PREFIX_LENGTH);
        }
        unset($allVisits);

        $totalOutputSize = count($idToTail) * $totalDates;

        $packedMap = [];
        foreach ($tailToId as $tailKey => $id) {
            $baseIndex = $id * $totalDates;
            $uriFullLength = $uriTailsData[$tailKey] ?? 0;
            $packedMap[$tailKey] = ($uriFullLength << self::BIT_SHIFT) | $baseIndex;
        }
        unset($uriTailsData);

        $chunksConfig = self::calculateChunks($fileHandle, $fileSize, $chunkBackstep);
        fclose($fileHandle);

        $shmId = shmop_open(ftok(__FILE__, 'm'), "c", 0644, 4);
        shmop_write($shmId, pack('V', 0), 0);
        $semId = sem_get(ftok(__FILE__, 's'));

        $incrementMap = [];
        for ($i = 0; $i < 255; $i++) {
            $incrementMap[chr($i)] = chr($i + 1);
        }

        // --- PHASE 3: Parallel Processing ---
        $t3_start = microtime(true);
        $workerSockets = [];
        for ($w = 0; $w < self::WORKER_COUNT; $w++) {
            $socketPair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            $pid = pcntl_fork();

            if ($pid === 0) {
                fclose($socketPair[0]);
                $workerBuffer = str_repeat(chr(0), $totalOutputSize);
                $workerFileHandle = fopen($inputPath, 'rb');
                stream_set_read_buffer($workerFileHandle, 0);

                while (true) {
                    sem_acquire($semId);
                    $currentIndexRaw = shmop_read($shmId, 0, 4);
                    $chunkIndex = unpack('V', $currentIndexRaw)[1];
                    if ($chunkIndex >= self::CHUNKS_COUNT) {
                        sem_release($semId);
                        break;
                    }
                    shmop_write($shmId, pack('V', $chunkIndex + 1), 0);
                    sem_release($semId);

                    [$chunkStart, $chunkEnd] = $chunksConfig[$chunkIndex];
                    self::processChunk($workerFileHandle, $chunkStart, $chunkEnd, $packedMap, $dateToId, $incrementMap, $workerBuffer);
                }

                fwrite($socketPair[1], $workerBuffer);
                exit(0);
            }
            fclose($socketPair[1]);
            stream_set_blocking($socketPair[0], false);
            $workerSockets[] = $socketPair[0];
        }

        $t3_io_start = microtime(true);
        $workerDataBuffers = array_fill(0, self::WORKER_COUNT, '');
        $socketsToRead = $workerSockets;
        $resultsCollected = 0;

        echo "Collecting results (Asynchronous):\n";
        while ($resultsCollected < self::WORKER_COUNT) {
            $read = $socketsToRead;
            $write = $except = null;
            if (stream_select($read, $write, $except, 1) > 0) {
                foreach ($read as $socket) {
                    $idx = array_search($socket, $workerSockets);
                    $data = fread($socket, 1024 * 1024);
                    if ($data !== false) {
                        $workerDataBuffers[$idx] .= $data;
                    }
                    if (feof($socket)) {
                        $doneTime = round(microtime(true) - $t3_io_start, 4);
                        echo "  - Worker $idx finished and data collected at: {$doneTime}s\n";

                        fclose($socket);
                        unset($socketsToRead[array_search($socket, $socketsToRead)]);
                        $resultsCollected++;
                    }
                }
            }
        }
        $t3_io_time = microtime(true) - $t3_io_start;

        $t3_agg_start = microtime(true);
        $aggregatedCounts = array_fill(0, $totalOutputSize, 0);
        foreach ($workerDataBuffers as $workerData) {
            if (strlen($workerData) === $totalOutputSize) {
                $unpackedData = unpack('C*', $workerData);
                foreach ($unpackedData as $index => $value) {
                    $aggregatedCounts[$index - 1] += $value;
                }
            }
        }
        unset($workerDataBuffers);
        $t3_agg_time = microtime(true) - $t3_agg_start;

        echo "Phase 3 Summary:\n";
        echo "  - Total I/O time (Async): " . round($t3_io_time, 4) . "s\n";
        echo "  - Aggregation (Sum):      " . round($t3_agg_time, 4) . "s\n";

        shmop_delete($shmId);
        sem_remove($semId);

        self::saveResults($outputPath, $aggregatedCounts, $idToTail, $idToDate, $totalDates, $tailToPathMap);
        echo "Full Execution: " . round(microtime(true) - $startTotal, 4) . "s\n";
    }

    private static function calculateChunks($handle, int $fileSize, int $backstep): array
    {
        $chunks = [];
        $stepSize = (int)($fileSize / self::CHUNKS_COUNT);
        $currentStart = 0;

        for ($i = 1; $i < self::CHUNKS_COUNT; $i++) {
            $targetEnd = $i * $stepSize;
            fseek($handle, max(0, $targetEnd - $backstep));
            $boundaryBuffer = fread($handle, $backstep);
            $lastNewLinePos = strrpos($boundaryBuffer, "\n");

            $actualEnd = ($lastNewLinePos === false)
                ? $targetEnd
                : ($targetEnd - $backstep) + $lastNewLinePos + 1;

            $chunks[] = [$currentStart, $actualEnd];
            $currentStart = $actualEnd;
        }

        $chunks[] = [$currentStart, $fileSize];
        return $chunks;
    }

    private static function processChunk($handle, int $start, int $end, array $packedMap, array $dateToId, array $incrementMap, string &$outputBuffer): void
    {
        fseek($handle, $start);

        $remainingBytes = $end - $start;
        $readBufferSize = self::READ_BUFFER_SIZE;
        $chunkLimit = self::SMALL_CHUNK_SIZE;

        $leftover = '';

        while ($remainingBytes > 0) {
            $bytesToRead = min($remainingBytes, $readBufferSize);
            $data = fread($handle, $bytesToRead);
            if ($data === false || $data === '') break;

            $remainingBytes -= strlen($data);
            $currentOffset = 0;
            $dataLength = strlen($data);

            while ($currentOffset < $dataLength) {
                $targetEnd = $currentOffset + ($chunkLimit - strlen($leftover));

                $lastNL = strrpos($data, "\n", $targetEnd > $dataLength ? 0 : ($targetEnd - $dataLength));

                if ($lastNL !== false && $lastNL >= $currentOffset) {
                    $chunk = $leftover . substr($data, $currentOffset, $lastNL - $currentOffset + 1);
                    self::processL2Chunk($chunk, $packedMap, $dateToId, $incrementMap, $outputBuffer);

                    $leftover = '';
                    $currentOffset = $lastNL + 1;
                } else {
                    $leftover .= substr($data, $currentOffset);
                    break;
                }
            }
        }
        if ($leftover !== '') {
            if (substr($leftover, -1) !== "\n") {
                $leftover .= "\n";
            }
            self::processL2Chunk($leftover, $packedMap, $dateToId, $incrementMap, $outputBuffer);
        }
    }

    private static function processL2Chunk(string $data, array &$packedMap, array &$dateToId, array &$incrementMap, string &$outputBuffer): void
    {
        $pointer = strlen($data) - 1;
        $mask = self::INDEX_MASK;
        $shift = self::BIT_SHIFT;
        $threshold = 1050;

        while ($pointer > $threshold) {
            $packedVal = $packedMap[substr($data, $pointer - 48, 22)];
            $targetIdx = ($packedVal & $mask) + $dateToId[substr($data, $pointer - 22, 7)];
            $outputBuffer[$targetIdx] = $incrementMap[$outputBuffer[$targetIdx]];
            $pointer -= ($packedVal >> $shift);

            $packedVal = $packedMap[substr($data, $pointer - 48, 22)];
            $targetIdx = ($packedVal & $mask) + $dateToId[substr($data, $pointer - 22, 7)];
            $outputBuffer[$targetIdx] = $incrementMap[$outputBuffer[$targetIdx]];
            $pointer -= ($packedVal >> $shift);

            $packedVal = $packedMap[substr($data, $pointer - 48, 22)];
            $targetIdx = ($packedVal & $mask) + $dateToId[substr($data, $pointer - 22, 7)];
            $outputBuffer[$targetIdx] = $incrementMap[$outputBuffer[$targetIdx]];
            $pointer -= ($packedVal >> $shift);

            $packedVal = $packedMap[substr($data, $pointer - 48, 22)];
            $targetIdx = ($packedVal & $mask) + $dateToId[substr($data, $pointer - 22, 7)];
            $outputBuffer[$targetIdx] = $incrementMap[$outputBuffer[$targetIdx]];
            $pointer -= ($packedVal >> $shift);

            $packedVal = $packedMap[substr($data, $pointer - 48, 22)];
            $targetIdx = ($packedVal & $mask) + $dateToId[substr($data, $pointer - 22, 7)];
            $outputBuffer[$targetIdx] = $incrementMap[$outputBuffer[$targetIdx]];
            $pointer -= ($packedVal >> $shift);

            $packedVal = $packedMap[substr($data, $pointer - 48, 22)];
            $targetIdx = ($packedVal & $mask) + $dateToId[substr($data, $pointer - 22, 7)];
            $outputBuffer[$targetIdx] = $incrementMap[$outputBuffer[$targetIdx]];
            $pointer -= ($packedVal >> $shift);

            $packedVal = $packedMap[substr($data, $pointer - 48, 22)];
            $targetIdx = ($packedVal & $mask) + $dateToId[substr($data, $pointer - 22, 7)];
            $outputBuffer[$targetIdx] = $incrementMap[$outputBuffer[$targetIdx]];
            $pointer -= ($packedVal >> $shift);

            $packedVal = $packedMap[substr($data, $pointer - 48, 22)];
            $targetIdx = ($packedVal & $mask) + $dateToId[substr($data, $pointer - 22, 7)];
            $outputBuffer[$targetIdx] = $incrementMap[$outputBuffer[$targetIdx]];
            $pointer -= ($packedVal >> $shift);

            $packedVal = $packedMap[substr($data, $pointer - 48, 22)];
            $targetIdx = ($packedVal & $mask) + $dateToId[substr($data, $pointer - 22, 7)];
            $outputBuffer[$targetIdx] = $incrementMap[$outputBuffer[$targetIdx]];
            $pointer -= ($packedVal >> $shift);

            $packedVal = $packedMap[substr($data, $pointer - 48, 22)];
            $targetIdx = ($packedVal & $mask) + $dateToId[substr($data, $pointer - 22, 7)];
            $outputBuffer[$targetIdx] = $incrementMap[$outputBuffer[$targetIdx]];
            $pointer -= ($packedVal >> $shift);
        }

        while ($pointer >= 48) {
            $packedVal = $packedMap[substr($data, $pointer - 48, 22)];
            $targetIdx = ($packedVal & $mask) + $dateToId[substr($data, $pointer - 22, 7)];
            $outputBuffer[$targetIdx] = $incrementMap[$outputBuffer[$targetIdx]];
            $pointer -= ($packedVal >> $shift);
        }
    }

    private static function saveResults(string $path, array $counts, array $idToTail, array $idToDate, int $dateCount, array $tailToPathMap): void
    {
        $outHandle = fopen($path, 'wb');
        fwrite($outHandle, "{\n");
        $isFirstElement = true;

        foreach ($idToTail as $pathIndex => $tailKey) {
            $jsonEntries = [];
            $baseIndex = $pathIndex * $dateCount;

            for ($d = 0; $d < $dateCount; $d++) {
                if ($counts[$baseIndex + $d] > 0) {
                    $jsonEntries[] = '        "202' . $idToDate[$d] . '": ' . $counts[$baseIndex + $d];
                }
            }

            if (!empty($jsonEntries)) {
                if (!$isFirstElement) {
                    fwrite($outHandle, ",\n");
                }
                $formattedUrl = '"' . str_replace('/', '\/', $tailToPathMap[$tailKey]) . '"';
                fwrite($outHandle, "    $formattedUrl: {\n" . implode(",\n", $jsonEntries) . "\n    }");
                $isFirstElement = false;
            }
        }
        fwrite($outHandle, "\n}");
        fclose($outHandle);
    }
}
