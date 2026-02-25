<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $data = [];

        // Read and iterate over the CSV file
        foreach ($this->readCsv($inputPath) as [$url, $date]) {
            $url = \substr($url, 19 /* 'https://stitcher.io' */);
            $date = \substr($date, 0, 10 /* '2024-06-01' */);

            $data[$url][$date] ??= 0;
            $data[$url][$date]++;
        }

        // Sort the data for each URL (SORT_STRING is faster for ISO dates)
        foreach ($data as &$urlData) {
            \ksort($urlData, SORT_STRING);
        }

        // Write data
        \file_put_contents($outputPath, \json_encode($data, JSON_PRETTY_PRINT));
    }

    private function readCsv(string $inputPath): iterable
    {
        $handle = \fopen($inputPath, "r");
        while (($line = \fgets($handle)) !== false) {
            yield \explode(',', $line, 2);
        }
        \fclose($handle);
    }
}
