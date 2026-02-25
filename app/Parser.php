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
            $url = \str_replace('https://stitcher.io', '', $url);
            $date = \substr($date, 0, 10);

            $data[$url][$date] ??= 0;
            $data[$url][$date]++;
        }

        // Sort the data for each URL
        foreach ($data as &$urlData) {
            \ksort($urlData);
        }

        // Write data
        \file_put_contents($outputPath, \json_encode($data, JSON_PRETTY_PRINT));
    }

    private function readCsv(string $inputPath): iterable
    {
        $handle = \fopen($inputPath, "r");
        while (($row = \fgetcsv($handle, escape: '\\')) !== false) {
            yield $row;
        }
        \fclose($handle);
    }
}
