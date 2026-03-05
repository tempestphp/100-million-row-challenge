<?php

namespace App;

use App\Commands\Visit;
use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $firstUrl = Visit::all()[0]->uri;
        $baseUrlLen = strpos($firstUrl, '/', 8) + 1; // 8 is the length of 'https://'
        $timestampLen = 25; // e.g. 2024-09-13T06:26:07+00:00

        // With 1,000,000 visits, it is likely each URL will receive some visits on each date.
        // So we can precompute the date indices to save the sorting later.
        $dd = [];
        for ($i = 1; $i < 10; ++$i) {
            $dd[$i] = '-0' . $i;
        }
        for ($i = 10; $i <= 31; ++$i) {
            $dd[$i] = '-' . $i;
        }
        $i = 0;
        $dateToIndices = [];
        $initialDateCounts = [];
        for ($year = 1; $year <= 6; $year++) {
            $y = (string)$year;
            $minMonth = match ($year) {
                1 => 2,
                default => 1,
            };
            $maxMonth = match ($year) {
                6 => 3,
                default => 12,
            };
            for ($month = $minMonth; $month <= $maxMonth; $month++) {
                $mm = $y . $dd[$month];
                $maxDay = match ($month) {
                    2 => 29, // not bother to check 28 or 29
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                for ($day = 1; $day <= $maxDay; $day++) {
                    $date = $mm . $dd[$day];
                    $dateToIndices[$date] = $i;
                    $initialDateCounts[$i] = 0;
                    ++$i;
                }
            }
        }

        $visitStats = []; // this will hold all the visit counts in the format [url => [dateIndex => count]]

        // open the input file and read line by line
        $readLimit = 512 * 1024; // 512KB
        $writeLimit = 512 * 1024; // 512KB
        $inputRes = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($inputRes, 0);
        $raw = '';
        while (true) {
            $raw .= \fread($inputRes, $readLimit);
            if ($raw === '' || $raw === false) {
                break;
            }
            $from = 0;
            while (true) {
                $newlinePos = \strpos($raw, "\n", $from);
                if ($newlinePos === false) {
                    $raw = \substr($raw, $from);
                    break;
                }
                $comma = $newlinePos - $timestampLen - 1;
                $from += $baseUrlLen;
                $url = \substr($raw, $from, $comma - $from);
                // first three year digits are always 202, so we can skip them
                $date = \substr($raw, $comma + 4, 7);
                $dateIndex = $dateToIndices[$date];
                if (!isset($visitStats[$url])) {
                    $visitStats[$url] = $initialDateCounts;
                }
                ++$visitStats[$url][$dateIndex];
                $from = $newlinePos + 1;
            }
        }
        \fclose($inputRes);

        // speed up printing dates by precomputing the date strings
        $indexToDates = [];
        foreach ($dateToIndices as $date => $i) {
            $indexToDates[$i] = ",\n        \"202" . $date . '": ';
        }

        // write the result to the output file
        $outputRes = \fopen($outputPath, 'wb');
        \stream_set_write_buffer($outputRes, $writeLimit);
        $firstUrlWrittern = false;
        foreach ($visitStats as $url => $data) {
            if ($firstUrlWrittern) {
                \fwrite($outputRes, "\n    },\n    \"\\/");
            } else {
                \fwrite($outputRes, "{\n    \"\\/");
                $firstUrlWrittern = true;
            }
            \fwrite($outputRes, str_replace('/', '\\/', $url));

            $firstCountWritten = false;
            foreach ($data as $i => $count) {
                if ($count === 0) {
                    continue;
                }
                if ($firstCountWritten) {
                    \fwrite($outputRes, $indexToDates[$i]);
                } else {
                    \fwrite($outputRes, "\": {\n");
                    \fwrite($outputRes, substr($indexToDates[$i], 2));
                    $firstCountWritten = true;
                }
                \fwrite($outputRes, (string)$count);
            }
        }
        \fwrite($outputRes, "\n    }\n}");
        \fclose($outputRes);
    }
}
