<?php

namespace App;

final class Parser
{
    /**
     * @throws \DateMalformedStringException
     */
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

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
                $ymStr = "20{$y}-{$mStr}-";
                for ($d = 1; $d <= $maxD; $d++) {
                    $key = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $dateIds[$key] = $dateCount;
                    $dates[$dateCount] = $key;
                    $dateCount++;
                }
            }
        }

        $outputData = [];

        $input = fopen($inputPath, 'r');
        stream_set_read_buffer($input, 4_194_304);

        while ($line = fgets($input)) {
            $commaPos = strpos($line, ',');

            $path = substr($line, 19, $commaPos - 19);

            $date = substr($line, $commaPos + 1, 10);

            $outputData[$path] ??= [];

            $dateId = $dateIds[$date];

            $outputData[$path][$dateId] ??= 0;
            $outputData[$path][$dateId]++;
        }

        $output = fopen('php://memory', 'w');

        fwrite($output, "{" . PHP_EOL);

        $totalPathsCount = count($outputData);
        $pathIndex = 0;
        foreach ($outputData as $path => $pathCounts) {
            $escapedPath = str_replace('/', '\/', $path);
            fwrite($output, "    \"$escapedPath\": {" . PHP_EOL);

            $totalDatesCount = count($pathCounts);
            $dateIndex = 0;

            foreach ($dateIds as $dateId) {
                $count = $pathCounts[$dateId] ?? null;
                if ($count === null) {
                    continue;
                }

                $date = $dates[$dateId];

                fwrite($output, "        \"$date\": $count");
                if ($dateIndex < $totalDatesCount - 1) {
                    fwrite($output, ",");
                }
                fwrite($output, PHP_EOL);

                $dateIndex++;
            }

            fwrite($output, "    }");
            if ($pathIndex < $totalPathsCount - 1) {
                fwrite($output, ",");
            }
            fwrite($output, PHP_EOL);

            $pathIndex++;
        }

        fwrite($output, "}");

        rewind($output);

        stream_copy_to_stream($output, fopen($outputPath, 'w'));
    }
}
