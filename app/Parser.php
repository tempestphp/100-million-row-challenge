<?php

namespace App;

final class Parser
{
    /**
     * @throws \DateMalformedStringException
     */
    public function parse(string $inputPath, string $outputPath): void
    {
        $outputData = [];

        $input = fopen($inputPath, 'r');
        stream_set_read_buffer($input, 4 * 2 ^ 10 * 2 ^ 10);

        while ($line = fgets($input)) {
            $commaPos = strpos($line, ',');

            $path = substr($line, 19, $commaPos - 19);

            $date = substr($line, $commaPos + 1, 10);

            $outputData[$path] ??= [];

            $outputData[$path][$date] ??= 0;
            $outputData[$path][$date]++;
        }

        foreach ($outputData as &$data) {
            ksort($data);
        }

        $output = fopen('php://memory', 'w');
        stream_set_read_buffer($output, 4 * 2 ^ 10 * 2 ^ 10);

        fwrite($output, "{" . PHP_EOL);

        $totalPathsCount = count($outputData);
        $pathIndex = 0;
        foreach ($outputData as $path => $pathCounts) {
            $escapedPath = str_replace('/', '\/', $path);
            fwrite($output, "    \"$escapedPath\": {" . PHP_EOL);

            $totalDatesCount = count($pathCounts);
            $dateIndex = 0;
            foreach ($pathCounts as $date => $count) {
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
