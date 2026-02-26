<?php

namespace App;

use Exception;

final class Parser
{
    /**
     *
     * Each entry in the generated JSON file must be a key-value pair with the page's URL path as the key
     * and an array with the number of visits per day as the value.
     *
     * Visits must be sorted by date in ascending order.
     * The output must be encoded as a pretty JSON string.
     *
     * i.e. {
     *          "\/blog\/shorthand-comparisons-in-php": {
     *          "2022-09-10": 1,
     *          "2025-12-07": 1
     * }
     *
     * @param string $inputPath
     * @param string $outputPath
     * @return void
     * @throws Exception
     */
    public function parse(string $inputPath, string $outputPath): void
    {
        // Disable garbage collection for better performance during bulk processing
        gc_disable();

        $handle = fopen($inputPath, 'r');
        $results = [];

        // Process file line by line to minimise memory footprint
        while (($line = fgets($handle)) !== false) {
            $commaPos = strrpos($line, ',');

            $url = substr($line, 19, $commaPos - 19);
            $date = substr($line, $commaPos + 1, 10);

            // Increment visit count for this URL and date combination
            $results[$url][$date] = ($results[$url][$date] ?? 0) + 1;
        }

        fclose($handle);

        // Sort visits by date for each URL
        foreach ($results as &$dates) {
            ksort($dates);
        }

        file_put_contents($outputPath, json_encode($results, JSON_PRETTY_PRINT));
    }
}