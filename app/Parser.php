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

        foreach (Visit::all() as $pathId => $visit) {
            $path = \substr($visit->uri, $prefixLength);
            $pathCache[$path] = $pathId;
            $pathByIds[$pathId] = $path;
        }

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
                    $parsedDateCache[$rawDate] = (int) ($year . $month . $day);
                    $formattedDatesByInt[$parsedDateCache[$rawDate]] = $rawDate;
                }
            }
        }

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

                $date = $parsedDateCache[$rawDate];

                $pathDates = &$result[$pathId];

                if (isset($pathDates[$date])) {
                    $pathDates[$date]++;
                } else {
                    $pathDates[$date] = 1;
                }

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

            $date = $parsedDateCache[$rawDate];

            $pathDates = &$result[$pathId];

            if (isset($pathDates[$date])) {
                $pathDates[$date]++;
            } else {
                $pathDates[$date] = 1;
            }
        }

        $out = \fopen($outputPath, 'wb');

        $outBuffer = "{\n";
        $outBufferFlushSize = 1024 * 1024;
        $isFirstPath = true;

        foreach ($result as $pathId => $dates) {
            $path = $pathByIds[$pathId];

            if ($isFirstPath) {
                $isFirstPath = false;
            } else {
                $outBuffer .= ",\n";
            }

            if (\count($dates) === 1) {
                $outBuffer .= '    "\/blog\/' . $path . "\": {\n";

                foreach ($dates as $date => $count) {
                    $outBuffer .= '        "' . $formattedDatesByInt[$date] . '": ' . $count;
                }

                $outBuffer .= "\n    }";
            } else {
                \ksort($dates, \SORT_NUMERIC);

                $outBuffer .= '    "\/blog\/' . $path . "\": {\n";

                $isFirstDate = true;

                foreach ($dates as $date => $count) {
                    if ($isFirstDate) {
                        $isFirstDate = false;
                    } else {
                        $outBuffer .= ",\n";
                    }

                    $outBuffer .= '        "' . $formattedDatesByInt[$date] . '": ' . $count;
                }

                $outBuffer .= "\n    }";
            }

            if (\strlen($outBuffer) >= $outBufferFlushSize) {
                \fwrite($out, $outBuffer);
                $outBuffer = '';
            }
        }

        $outBuffer .= "\n}";

        \fwrite($out, $outBuffer);
    }
}
