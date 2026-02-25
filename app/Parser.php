<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $output = [];

        if (($handle = fopen($inputPath, 'r')) !== false) {
            while (($row = fgetcsv($handle, 0, ',', '"', '\\')) !== false) {
                $url = substr($row[0], 19);
                $date = substr($row[1], 0, 10);
                $output[$url][$date] = ($output[$url][$date] ?? 0) + 1;
            }
            fclose($handle);
        }

        foreach ($output as &$dates) {
            ksort($dates, SORT_STRING);
        }

        $output = json_encode($output, JSON_PRETTY_PRINT);

        file_put_contents($outputPath, $output);
    }
}