<?php

namespace App;

use Exception;

final class Parser
{
    const int PREFIX_LENGTH = 25;

    const int TIME_LENGTH = -16;

    const int FULL_DATE_LENGTH = 25;

    const int CHUNK_SIZE = 1024 * 1024;

    public function parse(string $inputPath, string $outputPath): void
    {
        ini_set('memory_limit', '512M');
        gc_disable();

        $fileSize = filesize($inputPath);
        $file = fopen($inputPath, 'rb');

        [$pathSet, $numPaths, $dateSet, $dateMap, $numDates] = $this->scanFile($fileSize, $file);

        fseek($file, 0);

        $map = array_fill(0, $numPaths * $numDates, 0);

        $this->parseFile($fileSize, $file, $pathSet, $dateMap, $numDates, $map);

        fclose($file);

        $this->output($outputPath, $map, $pathSet, $dateSet, $numDates);
    }

    protected function output(string $outputPath, array $map, &$pathSet, &$dateSet, $numDates): void
    {
        $file = fopen($outputPath, 'w');
        stream_set_write_buffer($file, self::CHUNK_SIZE);
        $buffer = "{";

        $pathSeparator = '';
        foreach ($pathSet as $path => $pathId) {
            $buffer .= $pathSeparator;
            $pathSeparator = ',';

            $buffer .= "\n    \"\/blog\/$path\": {";

            $dateSeparator = '';
            for ($i = 0; $i < $numDates; $i++) {
                $count = $map[$pathId * $numDates + $i];
                if ($count === 0) {
                    continue;
                }
                $buffer .= $dateSeparator;
                $dateSeparator = ',';

                $date = $dateSet[$i];
                $buffer .= "\n        \"20$date\": $count";
            }

            $buffer .= "\n    }";
        }

        fwrite($file, $buffer . "\n}");
        fclose($file);
    }

    protected function parseFile(int $fileSize, $file, &$pathSet, &$dateMap, $numDates, &$map): void
    {
        $bytesProcessed = 0;
        while ($bytesProcessed < $fileSize) {
            $bytesRemaining = $fileSize - $bytesProcessed;
            $estimatedChunkSize = min(self::CHUNK_SIZE, $bytesRemaining);

            $data = fread($file, $estimatedChunkSize);
            if ($data === false) {
                break;
            }

            $chunkLengthBytes = strlen($data);
            $bytesProcessed += $chunkLengthBytes;

            $lastNewlineIndex = strrpos($data, "\n");
            if ($lastNewlineIndex !== false) {
                $overhangBytes = $chunkLengthBytes - $lastNewlineIndex - 1;
                fseek($file, -$overhangBytes, SEEK_CUR);
                $bytesProcessed -= $overhangBytes;
            }

            $processedIndex = 0;
            while ($processedIndex < $lastNewlineIndex) {
                $newlineIndex = strpos($data, "\n", $processedIndex);
                $path = substr(
                    $data,
                    $processedIndex + self::PREFIX_LENGTH,
                    $newlineIndex - $processedIndex - self::PREFIX_LENGTH - self::FULL_DATE_LENGTH - 1,
                );
                $date = substr(
                    $data,
                    $newlineIndex - self::FULL_DATE_LENGTH + 2,
                    self::FULL_DATE_LENGTH + self::TIME_LENGTH - 1,
                );

                $pathIndex = $pathSet[$path];
                $dateIndex = $dateMap[$date];

                $map[$pathIndex * $numDates + $dateIndex]++;

                $processedIndex = $newlineIndex + 1;
            }
        }
    }

    protected function scanFile(int $fileSize, $file): array
    {
        $numPaths = 0;
        $pathsSet = [];
        $minDate = "99-99-99";
        $maxDate = "00-00-00";

        $bytesProcessed = 0;
        while ($bytesProcessed < $fileSize) {
            $bytesRemaining = $fileSize - $bytesProcessed;
            $estimatedChunkSize = min(self::CHUNK_SIZE, $bytesRemaining);

            $data = fread($file, $estimatedChunkSize);
            if ($data === false) {
                break;
            }

            $chunkLengthBytes = strlen($data);
            $bytesProcessed += $chunkLengthBytes;

            $lastNewlineIndex = strrpos($data, "\n");
            if ($lastNewlineIndex !== false) {
                $overhangBytes = $chunkLengthBytes - $lastNewlineIndex - 1;
                fseek($file, -$overhangBytes, SEEK_CUR);
                $bytesProcessed -= $overhangBytes;
            }

            $processedIndex = 0;
            while ($processedIndex < $lastNewlineIndex) {
                $newlineIndex = strpos($data, "\n", $processedIndex);
                $dateStr = substr(
                    $data,
                    $newlineIndex - self::FULL_DATE_LENGTH + 2,
                    self::FULL_DATE_LENGTH + self::TIME_LENGTH - 1,
                );

                if ($dateStr < $minDate) {
                    $minDate = $dateStr;
                }
                if ($dateStr > $maxDate) {
                    $maxDate = $dateStr;
                }

                $path = substr(
                    $data,
                    $processedIndex + self::PREFIX_LENGTH,
                    $newlineIndex - $processedIndex - self::PREFIX_LENGTH - self::FULL_DATE_LENGTH - 1,
                );

                if (! isset($pathsSet[$path])) {
                    $pathsSet[$path] = $numPaths++;
                }

                $processedIndex = $newlineIndex + 1;
            }
        }

        $minDateTs = strtotime($minDate);
        $numDates = intdiv((strtotime($maxDate) - $minDateTs), 86400) + 1;

        $dateSet = [];
        $dateMap = [];
        for ($i = 0; $i < $numDates; $i++) {
            $dateStr = date('y-m-d', $i * 86400 + $minDateTs);
            $dateSet[$i] = $dateStr;
            $dateMap[$dateStr] = $i;
        }

        return [$pathsSet, $numPaths, $dateSet, $dateMap, $numDates];
    }
}