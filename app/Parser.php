<?php

namespace App;

use App\Commands\Visit;
use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $commonUrlPrefixLen = 25; // https://stitcher.io/blog/
        $timestampLen = 25; // e.g. 2024-09-13T06:26:07+00:00
        $tsPlus1 = $timestampLen + 1;

        // With 1,000,000 visits, it is likely each URL will receive some visits on each date.
        // So we can precompute the date indices to save the sorting later.
        $dd = [];
        for ($i = 1; $i < 10; ++$i) {
            $dd[$i] = '-0' . $i;
        }
        for ($i = 10; $i <= 31; ++$i) {
            $dd[$i] = '-' . $i;
        }
        $possibleDates = [];
        for ($year = 1; $year <= 6; ++$year) {
            $y = (string)$year;
            $minMonth = $year === 1 ? 2 : 1;
            $maxMonth = $year === 6 ? 3 : 12;
            for ($month = $minMonth; $month <= $maxMonth; ++$month) {
                $mm = $y . $dd[$month];
                for ($day = 1; $day <= 31; ++$day) {
                    $date = $mm . $dd[$day];
                    $possibleDates[] = $date;
                }
            }
        }

        $visitStats = []; // sparse: [url => [date => count]] - only non-zero counts stored

        // open the input file
        $readLimit = 8 * 1024 * 1024; // 8MB
        $inputRes = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($inputRes, 0);
        $raw = '';
        $from = 0;
        while (true) {
            if ($raw !== '') {
                $remainderLen = \strlen($raw) - $from;
                \fseek($inputRes, -$remainderLen, \SEEK_CUR);
            }
            $raw = \fread($inputRes, $readLimit);
            if ($raw === '' || $raw === false) {
                break;
            }
            $from = 0;
            while (true) {
                $newlinePos = \strpos($raw, "\n", $from);
                if ($newlinePos === false) {
                    break;
                }
                $from += $commonUrlPrefixLen;
                $comma = $newlinePos - $tsPlus1;
                $url = \substr($raw, $from, $comma - $from);
                // first three year digits are always 202, so we can skip them
                $date = \substr($raw, $comma + 4, 7);
                if (!isset($visitStats[$url])) {
                    $visitStats[$url] = [];
                }
                $visitStats[$url][$date] = ($visitStats[$url][$date] ?? 0) + 1;
                $from = $newlinePos + 1;
            }
        }
        \fclose($inputRes);

        // speed up printing dates by precomputing the date strings
        $dateJsonParts1 = [];
        foreach ($possibleDates as $date) {
            $dateJsonParts1[] = ",\n        \"202" . $date . '": ';
        }
        $dateJsonParts2 = [];

        // write the result to the output file
        $writeLimit = 4 * 1024 * 1024; // 4MB buffer
        $blockSize = 48 * 1024;
        $iterationLimit = (int)($writeLimit / $blockSize);
        $outputRes = \fopen($outputPath, 'wb');
        \stream_set_write_buffer($outputRes, 0);
        $buffer = '';
        $firstUrlWritten = false;
        $urlCount = 0;
        foreach ($visitStats as $url => $stat) {
            if ($firstUrlWritten) {
                $buffer .= "\n    },\n    \"\\/blog\\/";
            } else {
                $buffer .= "{\n    \"\\/blog\\/";
                $firstUrlWritten = true;
            }
            $buffer .= $url;

            $firstCountWritten = false;
            foreach ($possibleDates as $i => $date) {
                if (!isset($stat[$date])) {
                    continue;
                }
                if ($firstCountWritten) {
                    $buffer .= $dateJsonParts1[$i];
                } else {
                    if (!isset($dateJsonParts2[$i])) {
                        $dateJsonParts2[$i] = "\": {\n" . \substr($dateJsonParts1[$i], 2);
                    }
                    $buffer .= $dateJsonParts2[$i];
                    $firstCountWritten = true;
                }
                $buffer .= (string)$stat[$date];
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
