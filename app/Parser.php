<?php

namespace App;

use function count;
use function fgets;
use function file_put_contents;
use function fopen;
use function gc_disable;
use function str_replace;
use function stream_set_read_buffer;
use function strpos;
use function substr;

final class Parser
{
    /**
     * @throws \DateMalformedStringException
     */
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        // pre-compute dates
        $dateIds = [];
        $dates = [];
        $dateCount = 0;
        for ($y = 21; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => $y === 24 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $mStr = ($m < 10 ? '0' : '') . $m;
                $ymStr = "{$y}-{$mStr}-";
                for ($d = 1; $d <= $maxD; $d++) {
                    $key = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $dateIds[$key] = $dateCount;
                    $dates[$dateCount] = $key;
                    $dateCount++;
                }
            }
        }

        $outputData = [];

        // parse input
        $input = fopen($inputPath, 'r');
        stream_set_read_buffer($input, 4_194_304);

        while ($line = fgets($input)) {
            $commaPos = strpos($line, ',');

            // we know url path is 19 chars from left, because host and protocol stay the same
            $path = substr($line, 19, $commaPos - 19);
            // we know the date is the 10 chars after the comma
            $date = substr($line, $commaPos + 3, 8);

            $outputData[$path] ??= [];

            // use dateIds for insertion because those are correctly ordered
            $dateId = $dateIds[$date];

            $outputData[$path][$dateId] = ($outputData[$path][$dateId] ?? 0) + 1;
        }

        // write output
        $outputJson = "{" . PHP_EOL;

        $totalPathsCount = count($outputData);
        $pathIndex = 0;
        foreach ($outputData as $path => $pathCounts) {
            $escapedPath = str_replace('/', '\/', $path);
            $outputJson .= "    \"$escapedPath\": {" . PHP_EOL;

            $totalDatesCount = count($pathCounts);
            $dateIndex = 0;

            foreach ($dateIds as $dateId) {
                $count = $pathCounts[$dateId] ?? null;
                if ($count === null) {
                    continue;
                }

                // reconstruct date from id
                $date = "20" . $dates[$dateId];

                $outputJson .= "        \"$date\": $count";
                if ($dateIndex < $totalDatesCount - 1) {
                    $outputJson .= ",";
                }
                $outputJson .= PHP_EOL;

                $dateIndex++;
            }

            $outputJson .= "    }";
            if ($pathIndex < $totalPathsCount - 1) {
                $outputJson .= ",";
            }
            $outputJson .= PHP_EOL;

            $pathIndex++;
        }

        $outputJson .= "}";

        file_put_contents($outputPath, $outputJson);
    }
}
