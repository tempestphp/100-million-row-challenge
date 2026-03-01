<?php

namespace App;

use Exception;

final class Parser
{
    private const NUM_WORKERS = 8;
    private const MAX_PATHS = 300;
    private const CHUNK_SIZE = 8_388_608; // 8MB
    private const SLUG_SCAN_CHUNK_BYTES = 2_097_152; // 2MB
    private const URL_SLUG_OFFSET = 25; // strlen('https://stitcher.io/blog/')
    private const NEXT_SLUG_OFFSET = 52; // strlen(',2026-01-24T01:16:58+00:00\nhttps://stitcher.io/blog/')
    private const DATE_SLOTS_PER_PATH = 2560;

    public function parse(string $inputPath, string $outputPath): void
    {
        ini_set('memory_limit', -1);

        gc_disable();

        $fileSize = filesize($inputPath);

        if ($fileSize === false) {
            throw new Exception("Unable to get file size: {$inputPath}");
        }

        // Build the global path IDs once in the parent.
        [$slugBaseByStr, $pathStrById] = $this->scanPathBases($inputPath);
        $pathCount = count($pathStrById);

        if ($pathCount === 0) {
            file_put_contents($outputPath, json_encode([], JSON_PRETTY_PRINT));

            return;
        }

        // Global date IDs are fixed to 2020..2026.
        $dateStrById = $this->buildDateStrings();
        $dateIdByShort = $this->buildDateIdsByShort($dateStrById);
        $numDates = count($dateStrById);

        if (self::DATE_SLOTS_PER_PATH < $numDates) {
            throw new Exception(sprintf(
                'DATE_SLOTS_PER_PATH (%d) must be >= number of fixed dates (%d).',
                self::DATE_SLOTS_PER_PATH,
                $numDates,
            ));
        }

        $flatSize = $pathCount * self::DATE_SLOTS_PER_PATH;

        // Calculate chunk boundaries aligned to newlines.
        $boundaries = $this->calculateBoundaries($inputPath, $fileSize);

        // Unique prefix for temp files.
        $tmpPrefix = './parser_' . getmypid() . '_';

        $pids = [];

        for ($i = 1; $i < self::NUM_WORKERS; ++$i) {
            $pid = pcntl_fork();

            if ($pid === -1) {
                throw new Exception("Unable to fork worker {$i}");
            }

            if ($pid === 0) {
                $workerFlat = $this->processChunk(
                    $inputPath,
                    $boundaries[$i],
                    $boundaries[$i + 1],
                    $slugBaseByStr,
                    $dateIdByShort,
                    $flatSize,
                );

                $this->writeFlatFile($tmpPrefix . $i, $workerFlat, $flatSize);
                exit(0);
            }

            $pids[$i] = $pid;
        }

        // Parent processes the first chunk directly.
        $flat = $this->processChunk(
            $inputPath,
            $boundaries[0],
            $boundaries[1],
            $slugBaseByStr,
            $dateIdByShort,
            $flatSize,
        );

        // Wait for all children to finish writing.
        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        // Read child flat arrays and merge directly.
        for ($i = 1; $i < self::NUM_WORKERS; ++$i) {
            $tmpFile = $tmpPrefix . $i;
            $raw = file_get_contents($tmpFile);

            if ($raw === false) {
                throw new Exception("Unable to read temp file: {$tmpFile}");
            }

            unlink($tmpFile);

            $workerFlat = unpack('V*', $raw);

            if ($workerFlat === false) {
                throw new Exception("Unable to unpack worker data: {$tmpFile}");
            }

            $this->mergeFlat($flat, $workerFlat, $flatSize);
        }

        unset($slugBaseByStr);

        // Build final array for json_encode.
        $visits = [];

        for ($pathId = 0; $pathId < $pathCount; ++$pathId) {
            $sorted = [];
            $base = $pathId * self::DATE_SLOTS_PER_PATH;

            for ($dateId = 0; $dateId < $numDates; ++$dateId) {
                $count = $flat[$base + $dateId];

                if ($count > 0) {
                    $sorted[$dateStrById[$dateId]] = $count;
                }
            }

            $visits[$pathStrById[$pathId]] = $sorted;
        }

        file_put_contents($outputPath, json_encode($visits, JSON_PRETTY_PRINT));
    }

    private function scanPathBases(string $inputPath): array
    {
        $slugBaseByStr = [];
        $pathStrById = [];

        $fp = fopen($inputPath, 'rb');

        if ($fp === false) {
            throw new Exception("Unable to open input file: {$inputPath}");
        }

        $buffer = fread($fp, self::SLUG_SCAN_CHUNK_BYTES);
        fclose($fp);

        if ($buffer === false) {
            throw new Exception("Unable to read input file: {$inputPath}");
        }

        if ($buffer === '') {
            return [$slugBaseByStr, $pathStrById];
        }

        $lastNewlinePosition = strrpos($buffer, "\n");

        if ($lastNewlinePosition === false) {
            return [$slugBaseByStr, $pathStrById];
        }

        $limit = $lastNewlinePosition + 1;
        $pos = self::URL_SLUG_OFFSET;

        while ($pos < $limit && count($pathStrById) < self::MAX_PATHS) {
            $commaPos = strpos($buffer, ',', $pos);

            if ($commaPos === false || $commaPos >= $limit) {
                break;
            }

            $slug = substr($buffer, $pos, $commaPos - $pos);

            if (! isset($slugBaseByStr[$slug])) {
                $pathId = count($pathStrById);
                $pathStrById[$pathId] = '/blog/' . $slug;
                $slugBaseByStr[$slug] = $pathId * self::DATE_SLOTS_PER_PATH;
            }

            $pos = $commaPos + self::NEXT_SLUG_OFFSET;
        }

        return [$slugBaseByStr, $pathStrById];
    }

    private function buildDateStrings(): array
    {
        $dateStrById = [];
        $daysInMonthsCommon = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        $daysInMonthsLeap = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

        for ($year = 2020; $year <= 2026; ++$year) {
            $daysInMonths = ($year === 2020 || $year === 2024) ? $daysInMonthsLeap : $daysInMonthsCommon;

            for ($month = 1; $month <= 12; ++$month) {
                $daysInMonth = $daysInMonths[$month - 1];

                for ($day = 1; $day <= $daysInMonth; ++$day) {
                    $dateStrById[] = sprintf('%04d-%02d-%02d', $year, $month, $day);
                }
            }
        }

        return $dateStrById;
    }

    private function buildDateIdsByShort(array $dateStrById): array
    {
        $dateIdByShort = [];

        foreach ($dateStrById as $dateId => $date) {
            $dateIdByShort[substr($date, 2, 8)] = $dateId;
        }

        return $dateIdByShort;
    }

    private function writeFlatFile(string $tmpFile, array $flat, int $flatSize): void
    {
        $fp = fopen($tmpFile, 'wb');

        if ($fp === false) {
            throw new Exception("Unable to open temp file for writing: {$tmpFile}");
        }

        $chunkSize = 8192;

        for ($offset = 0; $offset < $flatSize; $offset += $chunkSize) {
            $length = min($chunkSize, $flatSize - $offset);
            $slice = array_slice($flat, $offset, $length);
            fwrite($fp, pack('V*', ...$slice));
        }

        fclose($fp);
    }

    private function mergeFlat(array &$targetFlat, array $sourceFlat, int $flatSize): void
    {
        $idx = 0;
        $sourceIdx = 1; // unpack('V*') is 1-based
        $limit = $flatSize - 3;

        while ($idx < $limit) {
            $value0 = $sourceFlat[$sourceIdx];
            if ($value0 !== 0) {
                $targetFlat[$idx] += $value0;
            }

            $value1 = $sourceFlat[$sourceIdx + 1];
            if ($value1 !== 0) {
                $targetFlat[$idx + 1] += $value1;
            }

            $value2 = $sourceFlat[$sourceIdx + 2];
            if ($value2 !== 0) {
                $targetFlat[$idx + 2] += $value2;
            }

            $value3 = $sourceFlat[$sourceIdx + 3];
            if ($value3 !== 0) {
                $targetFlat[$idx + 3] += $value3;
            }

            $idx += 4;
            $sourceIdx += 4;
        }

        for (; $idx < $flatSize; ++$idx, ++$sourceIdx) {
            $value = $sourceFlat[$sourceIdx];

            if ($value !== 0) {
                $targetFlat[$idx] += $value;
            }
        }
    }

    private function calculateBoundaries(string $inputPath, int $fileSize): array
    {
        $boundaries = [0];

        $fp = fopen($inputPath, 'rb');

        if ($fp === false) {
            throw new Exception("Unable to open input file: {$inputPath}");
        }

        for ($i = 1; $i < self::NUM_WORKERS; ++$i) {
            $approxPos = (int) ($i * $fileSize / self::NUM_WORKERS);
            fseek($fp, $approxPos);

            // Scan forward to next newline
            $line = fgets($fp);

            if ($line === false) {
                // Past EOF, just use fileSize
                $boundaries[] = $fileSize;
            } else {
                $boundaries[] = ftell($fp);
            }
        }

        $boundaries[] = $fileSize;
        fclose($fp);

        return $boundaries;
    }

    private function processChunk(
        string $inputPath,
        int $startOffset,
        int $endOffset,
        array $slugBaseByStr,
        array $dateIdByShort,
        int $flatSize,
    ): array
    {
        $flat = array_fill(0, $flatSize, 0);
        $fileHandle = fopen($inputPath, 'rb');

        if ($fileHandle === false) {
            throw new Exception("Unable to open input file: {$inputPath}");
        }

        if (fseek($fileHandle, $startOffset, SEEK_SET) !== 0) {
            fclose($fileHandle);
            throw new Exception("Unable to seek to offset {$startOffset} in {$inputPath}");
        }

        $remaining = $endOffset - $startOffset;

        while ($remaining > 0) {
            $chunk = fread($fileHandle, $remaining > self::CHUNK_SIZE ? self::CHUNK_SIZE : $remaining);

            $chunkLength = strlen($chunk);

            if ($chunkLength === 0) {
                break;
            }

            $remaining -= $chunkLength;
            $lastNl = strrpos($chunk, "\n");

            if ($lastNl === false) {
                break;
            }

            $over = $chunkLength - $lastNl - 1;

            if ($over > 0) {
                if (fseek($fileHandle, -$over, SEEK_CUR) !== 0) {
                    fclose($fileHandle);
                    throw new Exception("Unable to rewind chunk overlap in {$inputPath}");
                }

                $remaining += $over;
            }

            $lineCount = substr_count($chunk, "\n", 0, $lastNl + 1);
            $mainIters = $lineCount >> 3;
            $tailLines = $lineCount & 7;

            $pos = self::URL_SLUG_OFFSET;

            for ($i = 0; $i < $mainIters; ++$i) {
                $sep = strpos($chunk, ',', $pos);
                ++$flat[$slugBaseByStr[substr($chunk, $pos, $sep - $pos)] + $dateIdByShort[substr($chunk, $sep + 3, 8)]];
                $pos = $sep + self::NEXT_SLUG_OFFSET;

                $sep = strpos($chunk, ',', $pos);
                ++$flat[$slugBaseByStr[substr($chunk, $pos, $sep - $pos)] + $dateIdByShort[substr($chunk, $sep + 3, 8)]];
                $pos = $sep + self::NEXT_SLUG_OFFSET;

                $sep = strpos($chunk, ',', $pos);
                ++$flat[$slugBaseByStr[substr($chunk, $pos, $sep - $pos)] + $dateIdByShort[substr($chunk, $sep + 3, 8)]];
                $pos = $sep + self::NEXT_SLUG_OFFSET;

                $sep = strpos($chunk, ',', $pos);
                ++$flat[$slugBaseByStr[substr($chunk, $pos, $sep - $pos)] + $dateIdByShort[substr($chunk, $sep + 3, 8)]];
                $pos = $sep + self::NEXT_SLUG_OFFSET;

                $sep = strpos($chunk, ',', $pos);
                ++$flat[$slugBaseByStr[substr($chunk, $pos, $sep - $pos)] + $dateIdByShort[substr($chunk, $sep + 3, 8)]];
                $pos = $sep + self::NEXT_SLUG_OFFSET;

                $sep = strpos($chunk, ',', $pos);
                ++$flat[$slugBaseByStr[substr($chunk, $pos, $sep - $pos)] + $dateIdByShort[substr($chunk, $sep + 3, 8)]];
                $pos = $sep + self::NEXT_SLUG_OFFSET;

                $sep = strpos($chunk, ',', $pos);
                ++$flat[$slugBaseByStr[substr($chunk, $pos, $sep - $pos)] + $dateIdByShort[substr($chunk, $sep + 3, 8)]];
                $pos = $sep + self::NEXT_SLUG_OFFSET;

                $sep = strpos($chunk, ',', $pos);
                ++$flat[$slugBaseByStr[substr($chunk, $pos, $sep - $pos)] + $dateIdByShort[substr($chunk, $sep + 3, 8)]];
                $pos = $sep + self::NEXT_SLUG_OFFSET;
            }

            for ($i = 0; $i < $tailLines; ++$i) {
                $sep = strpos($chunk, ',', $pos);
                ++$flat[$slugBaseByStr[substr($chunk, $pos, $sep - $pos)] + $dateIdByShort[substr($chunk, $sep + 3, 8)]];
                $pos = $sep + self::NEXT_SLUG_OFFSET;
            }
        }

        fclose($fileHandle);

        return $flat;
    }
}
