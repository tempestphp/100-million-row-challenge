<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        // probably not very competitive, but not really sure what micro-optimisation tricks are available in PHP!

        $handle = fopen($inputPath, 'r');
        $outputData = [];

        // tried various alternatives to reading from the file, but none were any faster
        while ($row = fgets($handle)) {
            $rowLen = strlen($row);

            $site = substr($row, 19, $rowLen - 46);
            $date = $row[$rowLen - 26] . $row[$rowLen - 25] . $row[$rowLen - 24] . $row[$rowLen - 23] . $row[$rowLen - 22] . $row[$rowLen - 21] . $row[$rowLen - 20] . $row[$rowLen - 19] . $row[$rowLen - 18] . $row[$rowLen - 17];

            $outputData[$site][$date] ??= 0; // faster or similar to isset with assignment
            $outputData[$site][$date] += 1;
        }
        fclose($handle);

        foreach ($outputData as &$data) {
            // variously attempted to pre-sort on insertion but not worth the time spent when the arrays are small, and ksort has some clear optimisations built in I can't improve on
            ksort($data);
        }

//        $mem = round(memory_get_peak_usage() / (1024 * 1024), 1);
//        echo "mem used: {$mem} MiB\n";

        file_put_contents($outputPath, json_encode($outputData, JSON_PRETTY_PRINT));
    }
}
