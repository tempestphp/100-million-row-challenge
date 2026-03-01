<?php

namespace App;

use function array_fill;
use function fgets;
use function file_put_contents;
use function fopen;
use function gc_disable;
use function str_replace;
use function stream_set_read_buffer;
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
                $fullYmStr = "20{$y}-{$mStr}-";
                for ($d = 1; $d <= $maxD; $d++) {
                    $dStr = ($d < 10 ? '0' : '') . $d;
                    $dateIds[$ymStr . $dStr] = $dateCount;
                    $dates[$dateCount] = $fullYmStr . $dStr;
                    $dateCount++;
                }
            }
        }

        $pathIds = [];
        $paths = [];
        $escapedPaths = [];
        $pathCount = 0;

        $outputData = [];

        // parse input
        $input = fopen($inputPath, 'r');
        stream_set_read_buffer($input, 4_194_304);

        while ($line = fgets($input)) {
            // we know url path is 19 chars from left, because host and protocol stay the same
            // and each line ends with ",YYYY-MM-DDTHH:MM:SS+00:00"
            $path = substr($line, 19, -27);
            $date = substr($line, -24, 8);

            // use dateIds for insertion because those are correctly ordered
            $dateId = $dateIds[$date];

            $pathId = $pathIds[$path] ?? null;
            if ($pathId === null) {
                $pathId = $pathCount;
                $pathIds[$path] = $pathId;
                $paths[$pathId] = $path;
                $escapedPaths[$pathId] = str_replace('/', '\/', $path);
                $outputData[$pathId] = array_fill(0, $dateCount, 0);
                $pathCount++;
            }

            $outputData[$pathId][$dateId]++;
        }

        // write output
        $outputJson = "{" . PHP_EOL;

        $totalPathsCount = $pathCount;
        $pathIndex = 0;
        foreach ($paths as $pathId => $_path) {
            $pathCounts = $outputData[$pathId];
            $escapedPath = $escapedPaths[$pathId];
            $outputJson .= "    \"$escapedPath\": {" . PHP_EOL;

            $firstDate = true;

            for ($dateId = 0; $dateId < $dateCount; $dateId++) {
                $count = $pathCounts[$dateId];
                if ($count === 0) {
                    continue;
                }

                if (! $firstDate) {
                    $outputJson .= "," . PHP_EOL;
                }
                $date = $dates[$dateId];
                $outputJson .= "        \"$date\": $count";
                $firstDate = false;
            }

            $outputJson .= PHP_EOL;

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
