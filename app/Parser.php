<?php

namespace App;

use const SEEK_CUR;

final class Parser
{
    private const WORKERS = 2;
    private const DISCOVER_BYTES = 8_388_608;
    private const CHUNK_BYTES = 524_288;
    private const PREFIX_LENGTH = 25;
    private const LINE_OVERHEAD = 51;
    private const DATE_OFFSET_FROM_NL = 25;
    private const DATE_LENGTH = 10;
    private const WRITE_BUFFER = 1_048_576;

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $fileSize = (int) filesize($inputPath);
        [$pathToId, $pathById, $dateToId, $dateById] = $this->discoverDictionary($inputPath, $fileSize);

        $pathCount = count($pathById);
        $dateCount = count($dateById);
        $workers = self::WORKERS;

        $boundaries = $this->calculateBoundaries($inputPath, $fileSize, $workers);
        $tmpFiles = [];
        $pids = [];
        $tmpDir = sys_get_temp_dir();
        $myPid = getmypid();

        for ($i = 0; $i < $workers - 1; $i++) {
            $tmpFile = $tmpDir . '/parse_' . $myPid . '_' . $i;
            $tmpFiles[] = $tmpFile;
            $pid = pcntl_fork();

            if ($pid === 0) {
                $data = $this->processRange(
                    $inputPath,
                    $boundaries[$i],
                    $boundaries[$i + 1],
                    $pathToId,
                    $dateToId,
                    $pathCount,
                    $dateCount,
                );

                file_put_contents($tmpFile, pack('V*', ...$data));
                exit(0);
            }

            $pids[] = $pid;
        }

        $counts = $this->processRange(
            $inputPath,
            $boundaries[$workers - 1],
            $boundaries[$workers],
            $pathToId,
            $dateToId,
            $pathCount,
            $dateCount,
        );

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        foreach ($tmpFiles as $tmpFile) {
            $child = unpack('V*', (string) file_get_contents($tmpFile)) ?: [];
            unlink($tmpFile);

            $j = 0;
            foreach ($child as $value) {
                $counts[$j++] += $value;
            }
        }

        $this->writeOutput($outputPath, $counts, $pathById, $dateById, $dateCount);
    }

    private function calculateBoundaries(string $inputPath, int $fileSize, int $workers): array
    {
        $chunkSize = (int) ($fileSize / $workers);
        $boundaries = [0];
        $handle = fopen($inputPath, 'rb');

        for ($i = 1; $i < $workers; $i++) {
            fseek($handle, $i * $chunkSize);
            fgets($handle);
            $boundaries[] = (int) ftell($handle);
        }

        fclose($handle);
        $boundaries[] = $fileSize;

        return $boundaries;
    }

    private function processRange(
        string $inputPath,
        int $start,
        int $end,
        array $pathToId,
        array $dateToId,
        int $pathCount,
        int $dateCount,
    ): array {
        $counts = array_fill(0, $pathCount * $dateCount, 0);
        if ($start >= $end) {
            return $counts;
        }

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);

        $remaining = $end - $start;

        while ($remaining > 0) {
            $chunk = fread($handle, $remaining > self::CHUNK_BYTES ? self::CHUNK_BYTES : $remaining);
            if ($chunk === '') {
                break;
            }

            $chunkLen = strlen($chunk);
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) {
                continue;
            }

            if ($lastNl < $chunkLen - 1) {
                $excess = $chunkLen - $lastNl - 1;
                fseek($handle, -$excess, SEEK_CUR);
                $remaining += $excess;
            }

            $pos = 0;
            while ($pos < $lastNl) {
                $nlPos = strpos($chunk, "\n", $pos + 55);
                if ($nlPos === false) {
                    break;
                }

                $path = substr($chunk, $pos + self::PREFIX_LENGTH, $nlPos - $pos - self::LINE_OVERHEAD);
                $pathId = $pathToId[$path] ?? -1;

                if ($pathId !== -1) {
                    $date = substr($chunk, $nlPos - self::DATE_OFFSET_FROM_NL, self::DATE_LENGTH);
                    $dateId = $dateToId[$date] ?? -1;
                    if ($dateId !== -1) {
                        $counts[($pathId * $dateCount) + $dateId]++;
                    }
                }

                $pos = $nlPos + 1;
            }
        }

        fclose($handle);

        return $counts;
    }

    private function discoverDictionary(string $inputPath, int $fileSize): array
    {
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);

        $discoverBytes = $fileSize > self::DISCOVER_BYTES ? self::DISCOVER_BYTES : $fileSize;
        $chunk = (string) fread($handle, $discoverBytes);
        fclose($handle);

        $lastNl = strrpos($chunk, "\n");
        if ($lastNl === false) {
            return [[], [], [], []];
        }

        $pathToId = [];
        $pathById = [];
        $dateToId = [];
        $dateById = [];
        $pathCount = 0;
        $dateCount = 0;
        $pos = 0;

        while ($pos < $lastNl) {
            $nlPos = strpos($chunk, "\n", $pos + 55);
            if ($nlPos === false) {
                break;
            }

            $path = substr($chunk, $pos + self::PREFIX_LENGTH, $nlPos - $pos - self::LINE_OVERHEAD);
            if (! isset($pathToId[$path])) {
                $pathToId[$path] = $pathCount;
                $pathById[$pathCount] = $path;
                $pathCount++;
            }

            $date = substr($chunk, $nlPos - self::DATE_OFFSET_FROM_NL, self::DATE_LENGTH);
            if (! isset($dateToId[$date])) {
                $dateToId[$date] = $dateCount;
                $dateById[$dateCount] = $date;
                $dateCount++;
            }

            $pos = $nlPos + 1;
        }

        return [$pathToId, $pathById, $dateToId, $dateById];
    }

    private function writeOutput(string $outputPath, array $counts, array $pathById, array $dateById, int $dateCount): void
    {
        $out = fopen($outputPath, 'wb');
        $sortedDates = $dateById;
        asort($sortedDates, SORT_STRING);

        stream_set_write_buffer($out, self::WRITE_BUFFER);
        fwrite($out, '{');
        $firstPath = true;

        foreach ($pathById as $pathId => $path) {
            $base = $pathId * $dateCount;
            $entries = [];

            foreach ($sortedDates as $dateId => $date) {
                $count = $counts[$base + $dateId];
                if ($count !== 0) {
                    $entries[] = "        \"{$date}\": {$count}";
                }
            }

            $pathBuffer = $firstPath ? '' : ',';
            $firstPath = false;
            $pathBuffer .= "\n    \"\\/blog\\/{$path}\": {\n";
            $pathBuffer .= implode(",\n", $entries);
            $pathBuffer .= "\n    }";
            fwrite($out, $pathBuffer);
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
