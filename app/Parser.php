<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $output = [];

        if (($file = fopen($inputPath, 'r')) !== false) {
            while (($line = fgets($file)) !== false) {
                $row = explode(',', $line);
                $url = substr($row[0], 19);
                $date = substr($row[1], 0, 10);
                $output[$url][$date] = ($output[$url][$date] ?? 0) + 1;
            }
            fclose($file);
        }

        foreach ($output as &$dates) {
            ksort($dates, SORT_STRING);
        }

        file_put_contents($outputPath, json_encode($output, JSON_PRETTY_PRINT));
    }
}