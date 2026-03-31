<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        // Read the input file line by line
        $inputFile = fopen($inputPath, 'r');
        if (!$inputFile) {
            throw new Exception("Could not open the input file: $inputPath");
        }

        $urls = [];

        while (($line = fgets($inputFile)) !== false) {
            $pos = strpos($line, ',');
            $url = substr($line, 19, $pos - 19);
            $date = substr($line, $pos + 1, 10);
            if(isset($urls[$url][$date])) {
                $urls[$url][$date]++;
            }
            else{
                $urls[$url][$date] = 1;
            }
        }

        // Sort dates for each URL by lexicographical order (which works for YYYY-M-D format)
        foreach ($urls as &$dates) {
            ksort($dates);
        }

        file_put_contents($outputPath,
            json_encode($urls, JSON_PRETTY_PRINT )
        );
    }
}