<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $handle = fopen($inputPath, 'r');
        $outputData = [];
        while ($row = fgets($handle)) {
            $rowLen = strlen($row);

            $site = substr($row, 19, $rowLen - 46);
            $date = substr($row, $rowLen - 26, 10);

            $outputData[$site][$date] ??= 0;
            $outputData[$site][$date] += 1;
        }

        foreach ($outputData as &$data) {
            ksort($data);
        }

        file_put_contents($outputPath, json_encode($outputData, JSON_PRETTY_PRINT));
    }
}
