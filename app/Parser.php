<?php

namespace App;

use App\Commands\Visit;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $fh = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($fh, 0);

        $result = [];
        $prefixLength = 25; // strlen('https://stitcher.io/blog/')
        $timestampLength = 25; // strlen('2026-02-25T12:00:00+00:00')
        $commaOffsetFromNewline = $timestampLength + 1;
        $chunkSize = 1024 * 1024;
        $buffer = '';
        $parsedDateCache = [];
        $formattedDatesByInt = [];
        $pathCache = [];
        $pathByIds = [];

        $pathIdOffset = 0;
        $pathIdCounter = 0;

        $dateIdCounter = 0;
        for ($year = 2021; $year <= 2026; $year++) {
            for ($month = 1; $month <= 12; $month++) {
                $yearMonth = \sprintf('%d-%02d-', $year, $month);

                $days = match ($month) {
                    1, 3, 5, 7, 8, 10, 12 => 31,
                    4, 6, 9, 11 => 30,
                    2 => $year === 2024 ? 29 : 28,
                };

                for ($day = 1; $day <= $days; $day++) {
                    $rawDate = $yearMonth . \sprintf('%02d', $day);
                    $parsedDateCache[$rawDate] = $dateIdCounter++;
                    $formattedDatesByInt[$parsedDateCache[$rawDate]] = $rawDate;
                    $pathIdOffset++;
                }
            }
        }

        $totalUniquePaths = count(Visit::all());

        $sample = \fread($fh, $chunkSize / 2);
        $offset = 0;
        while (true) {
            $newlinePos = \strpos($sample, "\n", $offset);

            if ($newlinePos === false) {
                break;
            }

            $commaPos = $newlinePos - $commaOffsetFromNewline;
            $pathStart = $offset + $prefixLength;
            $path = \substr($sample, $pathStart, $commaPos - $pathStart);

            if (! isset($pathCache[$path])) {
                $pathCache[$path] = $pathId = $pathIdCounter++ * $pathIdOffset;
                $pathByIds[$pathId] = $path;
                foreach ($formattedDatesByInt as $dateId => $_) {
                    $result[$pathId + $dateId] = 0;
                }

                if ($pathIdCounter === $totalUniquePaths) {
                    break;
                }
            }

            $offset = $newlinePos + 1;
        }

        fseek($fh, 0);

        while (! \feof($fh)) {
            $chunk = \fread($fh, $chunkSize);

            if ($chunk === '') {
                continue;
            }

            $buffer .= $chunk;
            $offset = 0;

            while (true) {
                $newlinePos = \strpos($buffer, "\n", $offset);

                if ($newlinePos === false) {
                    break;
                }

                $commaPos = $newlinePos - $commaOffsetFromNewline;
                $pathStart = $offset + $prefixLength;
                $path = \substr($buffer, $pathStart, $commaPos - $pathStart);
                $rawDate = \substr($buffer, $commaPos + 1, 10);

                $pathId = $pathCache[$path];
                $dateId = $parsedDateCache[$rawDate];

                $result[$pathId + $dateId]++;

                $offset = $newlinePos + 1;
            }

            if ($offset > 0) {
                $buffer = \substr($buffer, $offset);
            }
        }

        if ($buffer !== '') {
            $commaPos = \strlen($buffer) - $timestampLength - 1;
            $path = \substr($buffer, $prefixLength, $commaPos - $prefixLength);
            $rawDate = \substr($buffer, $commaPos + 1, 10);

            $pathId = $pathCache[$path];
            $dateId = $parsedDateCache[$rawDate];

            $result[$pathId + $dateId]++;
        }

        $out = \fopen($outputPath, 'wb');

        $outBuffer = "{\n";
        $outBufferFlushSize = 1024 * 1024;

        $lastPathId = null;

        foreach ($result as $key => $count) {
            if ($count === 0) {
                continue;
            }

            $dateId = $key % $pathIdOffset;
            $pathId = $key - $dateId;

            $path = $pathByIds[$pathId];

            if ($lastPathId === null) {
                // 1st path
                $outBuffer .= '    "\/blog\/' . $path . "\": {\n";
                $lastPathId = $pathId;
            } elseif ($lastPathId !== $pathId) {
                // Nth path
                $outBuffer .= "\n    },\n" . '    "\/blog\/' . $path . "\": {\n";
                $lastPathId = $pathId;
            } else {
                // Trailing comma for count
                $outBuffer .= ",\n";
            }

            $outBuffer .= '        "' . $formattedDatesByInt[$dateId] . '": ' . $count;
            if (\strlen($outBuffer) >= $outBufferFlushSize) {
                \fwrite($out, $outBuffer);
                $outBuffer = '';
            }
        }

        \fwrite($out, $outBuffer . "\n    }\n}");
    }
}
