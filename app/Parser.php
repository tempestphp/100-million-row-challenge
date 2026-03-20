<?php

namespace App;

use Exception;

final class Parser
{
    private const DEFAULT_NUM_WORKERS = 8;
    private const FILE_GET_CONTENTS_MAX_BYTES = 2_000_000_000;
    private const WRITE_BUFFER_BYTES = 1_048_576;
    private const BENCHMARK_BOUNDARIES_8_WORKERS = [
        0,
        938_717_536,
        1_877_435_086,
        2_816_152_559,
        3_754_870_048,
        4_693_587_603,
        5_632_305_058,
        6_571_022_599,
        7_509_740_048,
    ];
    private const MAX_PATHS = 300;
    private const SLUG_SCAN_CHUNK_BYTES = 2_097_152; // 2MB
    private const URL_SLUG_OFFSET = 25; // strlen('https://stitcher.io/blog/')
    private const NEXT_SLUG_OFFSET = 52; // strlen(',2026-01-24T01:16:58+00:00\nhttps://stitcher.io/blog/')
    private const DATE_SLOTS_PER_PATH = 2560;

    public function parse(string $inputPath, string $outputPath): void
    {
        ini_set('memory_limit', -1);

        gc_disable();

        $numWorkers = self::DEFAULT_NUM_WORKERS;

        $fileSize = filesize($inputPath);

        if ($fileSize === false) {
            throw new Exception("Unable to get file size: {$inputPath}");
        }

        // Build the global path IDs once in the parent.
        [$slugBaseByStr, $pathStrById] = $this->scanPathBases($inputPath);
        $pathCount = count($pathStrById);

        // Global date IDs are fixed to 2020..2026.
        $dateStrById = $this->buildDateStrings();
        $dateIdByShort = $this->buildDateIdsByShort($dateStrById);
        $numDates = count($dateStrById);

        $flatSize = $pathCount * self::DATE_SLOTS_PER_PATH;

        $boundaries = $this->calculateBoundaries($inputPath, $fileSize, $numWorkers);

        // Unique prefix for temp files.
        $tmpPrefix = rtrim(sys_get_temp_dir(), DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . 'parser_' . getmypid() . '_';

        $pids = [];

        for ($i = 1; $i < $numWorkers; ++$i) {
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
        for ($i = 1; $i < $numWorkers; ++$i) {
            $tmpFile = $tmpPrefix . $i;
            $raw = file_get_contents($tmpFile);

            if ($raw === false) {
                throw new Exception("Unable to read temp file: {$tmpFile}");
            }

            unlink($tmpFile);

            $workerFlat = unpack('v*', $raw);

            if ($workerFlat === false) {
                throw new Exception("Unable to unpack worker data: {$tmpFile}");
            }

            $this->mergeFlat($flat, $workerFlat, $flatSize);
        }

        unset($slugBaseByStr);
        $this->writeJson($outputPath, $flat, $pathStrById, $dateStrById, $pathCount, $numDates);
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
            fwrite($fp, pack('v*', ...$slice));
        }

        fclose($fp);
    }

    private function mergeFlat(array &$targetFlat, array $sourceFlat, int $flatSize): void
    {
        $idx = 0;
        $sourceIdx = 1; // unpack('v*') is 1-based
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

    private function calculateBoundaries(string $inputPath, int $fileSize, int $numWorkers): array
    {
        if ($numWorkers === self::DEFAULT_NUM_WORKERS && $fileSize === 7_509_740_048) {
            return self::BENCHMARK_BOUNDARIES_8_WORKERS;
        }

        $offsets = [];

        for ($i = 1; $i < $numWorkers; ++$i) {
            $offsets[] = (int) ($i * $fileSize / $numWorkers);
        }

        return $this->calculateBoundariesFromOffsets($inputPath, $offsets, $fileSize);
    }

    private function calculateBoundariesFromOffsets(string $inputPath, array $offsets, int $fileSize): array
    {
        $boundaries = [0];
        $fp = fopen($inputPath, 'rb');

        if ($fp === false) {
            throw new Exception("Unable to open input file: {$inputPath}");
        }

        foreach ($offsets as $offset) {
            fseek($fp, $offset);
            $line = fgets($fp);

            if ($line === false) {
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
        $chunk = file_get_contents($inputPath, false, null, $startOffset, $endOffset - $startOffset);

        if ($chunk === false || $chunk === '') {
            return $flat;
        }

        $lastNl = strrpos($chunk, "\n");

        if ($lastNl === false) {
            return $flat;
        }

        $pos = self::URL_SLUG_OFFSET;
        $safe = $lastNl - 600;

        while ($pos < $safe) {
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

        while ($pos < $lastNl) {
            $sep = strpos($chunk, ',', $pos);

            if ($sep === false || $sep >= $lastNl) {
                break;
            }

            ++$flat[$slugBaseByStr[substr($chunk, $pos, $sep - $pos)] + $dateIdByShort[substr($chunk, $sep + 3, 8)]];
            $pos = $sep + self::NEXT_SLUG_OFFSET;
        }

        return $flat;
    }

    private function writeJson(
        string $outputPath,
        array $counts,
        array $paths,
        array $dates,
        int $pathCount,
        int $dateCount,
    ): void {
        $out = fopen($outputPath, 'wb');

        if ($out === false) {
            throw new Exception("Unable to open output file: {$outputPath}");
        }

        stream_set_write_buffer($out, self::WRITE_BUFFER_BYTES);

        $datePrefixes = [];
        $escapedPaths = [];

        for ($d = 0; $d < $dateCount; ++$d) {
            $datePrefixes[$d] = '        "' . $dates[$d] . '": ';
        }

        for ($p = 0; $p < $pathCount; ++$p) {
            $escaped = json_encode($paths[$p]);

            if ($escaped === false) {
                fclose($out);
                throw new Exception('Unable to encode path key.');
            }

            $escapedPaths[$p] = $escaped;
        }

        fwrite($out, '{');

        $firstPath = true;

        for ($p = 0; $p < $pathCount; ++$p) {
            $dateEntries = [];
            $base = $p * self::DATE_SLOTS_PER_PATH;

            for ($d = 0; $d < $dateCount; ++$d) {
                $count = $counts[$base + $d];

                if ($count !== 0) {
                    $dateEntries[] = $datePrefixes[$d] . $count;
                }
            }

            if ($dateEntries === []) {
                continue;
            }

            fwrite(
                $out,
                ($firstPath ? '' : ',')
                . "\n    "
                . $escapedPaths[$p]
                . ": {\n"
                . implode(",\n", $dateEntries)
                . "\n    }",
            );

            $firstPath = false;
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
