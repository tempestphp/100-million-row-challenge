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
                $end = strpos($line, ',');
                $url = substr($line, 19, $end - 19);
                $date = substr($line, -26, 10);

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