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
        for ($year = 1; $year <= 6; ++$year) {
            $y = (string)$year;
            $minMonth = $year === 1 ? 2 : 1;
            $maxMonth = $year === 6 ? 3 : 12;
            for ($month = $minMonth; $month <= $maxMonth; ++$month) {
                $mm = $y . $dd[$month];
                for ($day = 1; $day <= 31; ++$day) {
                    $date = $mm . $dd[$day];
                    $dateToIndices[$date] = $i;
                    ++$i;
                }
            }
        }

        $visitStats = []; // sparse: [url => [dateIndex => count]] - only non-zero counts stored

        // open the input file and read line by line
        $readLimit = 8 * 1024 * 1024; // 8MB - larger chunks = fewer syscalls
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
                    $visitStats[$url] = [];
                }
                $visitStats[$url][$dateIndex] = ($visitStats[$url][$dateIndex] ?? 0) + 1;
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
        $writeLimit = 4 * 1024 * 1024; // 4MB buffer
        $blockSize = 48 * 1024;
        $iterationLimit = (int)($writeLimit / $blockSize);
        $outputRes = \fopen($outputPath, 'wb');
        \stream_set_write_buffer($outputRes, 0);
        $buffer = '';
        $firstUrlWritten = false;
        $urlCount = 0;
        foreach ($visitStats as $url => $data) {
            if ($firstUrlWritten) {
                $buffer .= "\n    },\n    \"\\/";
            } else {
                $buffer .= "{\n    \"\\/";
                $firstUrlWritten = true;
            }
            $buffer .= \str_replace('/', '\\/', $url);

            $firstCountWritten = false;
            foreach ($indexToDates as $idx => $dateStr) {
                if (!isset($data[$idx])) {
                    continue;
                }
                $count = $data[$idx];
                if ($firstCountWritten) {
                    $buffer .= $dateStr;
                } else {
                    $buffer .= "\": {\n";
                    $buffer .= \substr($dateStr, 2);
                    $firstCountWritten = true;
                }
                $buffer .= (string)$count;
            }

            ++$urlCount;
            if ($urlCount >= $iterationLimit) {
                \fwrite($outputRes, $buffer);
                $buffer = '';
                $urlCount = 0;
            }
        }

        $buffer .= "\n    }\n}";
        \fwrite($outputRes, $buffer);
        \fclose($outputRes);
    }
}
