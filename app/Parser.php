<?php

namespace App;

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
        $lastPathId = 0;
        $pathCache = [];
        $pathByIds = [];

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

                if (isset($pathCache[$path])) {
                    $pathId = $pathCache[$path];
                } else {
                    $pathId = $lastPathId++;
                    $pathCache[$path] = $pathId;
                    $pathByIds[$pathId] = $path;
                }

                if (isset($parsedDateCache[$rawDate])) {
                    $date = $parsedDateCache[$rawDate];
                } else {
                    $parsedDateCache[$rawDate] = $date = (int) (
                        \substr($rawDate, 0, 4)
                        . \substr($rawDate, 5, 2)
                        . \substr($rawDate, 8, 2)
                    );
                    $formattedDatesByInt[$date] = $rawDate;
                }

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

            if (isset($pathCache[$path])) {
                $pathId = $pathCache[$path];
            } else {
                $pathId = $lastPathId++;
                $pathCache[$path] = $pathId;
                $pathByIds[$pathId] = $path;
            }

            if (isset($parsedDateCache[$rawDate])) {
                $date = $parsedDateCache[$rawDate];
            } else {
                $parsedDateCache[$rawDate] = $date = (int) (
                    \substr($rawDate, 0, 4)
                    . \substr($rawDate, 5, 2)
                    . \substr($rawDate, 8, 2)
                );
                $formattedDatesByInt[$date] = $rawDate;
            }

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

            if (\count($dates) === 1) {
                foreach ($dates as $date => $count) {
                    $formattedDates = [$formattedDatesByInt[$date] => $count];
                }
            } else {
                \ksort($dates, \SORT_NUMERIC);

                $formattedDates = [];

                foreach ($dates as $date => $count) {
                    $formattedDates[$formattedDatesByInt[$date]] = $count;
                }
            }

            if ($isFirstPath) {
                $isFirstPath = false;
            } else {
                $outBuffer .= ",\n";
            }

            $outBuffer .= '    "\/blog\/' . $path . "\": {\n";

            $isFirstDate = true;

            foreach ($formattedDates as $date => $count) {
                if ($isFirstDate) {
                    $isFirstDate = false;
                } else {
                    $outBuffer .= ",\n";
                }

                $outBuffer .= '        "' . $date . '": ' . $count;
            }

            $outBuffer .= "\n    }";

            if (\strlen($outBuffer) >= $outBufferFlushSize) {
                \fwrite($out, $outBuffer);
                $outBuffer = '';
            }
        }

        $outBuffer .= "\n}";

        \fwrite($out, $outBuffer);
    }
}
