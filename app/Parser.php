<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        // Open input file for reading
        $inputFile = fopen($inputPath, 'r');
        if ($inputFile === false) {
            exit();
        }

        // Initialize results array to store URL path -> date -> count mappings
        // Use an associative array to maintain insertion order
        $results = [];
        $urlOrder = []; // Track order of first appearance

        // Process each line of the CSV file
        while (($line = fgets($inputFile)) !== false) {
            // Trim whitespace and skip empty lines
            $line = trim($line);
            if (empty($line)) {
                continue;
            }

            // Parse CSV line (URL,timestamp)
            $url = strtok($line, ',');
            $timestamp = strtok('');

            // Extract URL path from full URL
            $urlPath = substr($url, 19);

            // Extract date (YYYY-MM-DD) from timestamp
            $date = substr($timestamp, 0, 10);

            // Track order of first appearance
            if (!isset($results[$urlPath])) {
                $results[$urlPath] = [];
                $urlOrder[] = $urlPath;
            }
            
            if (!isset($results[$urlPath][$date])) {
                $results[$urlPath][$date] = 0;
            }
            
            $results[$urlPath][$date]++;
        }

        fclose($inputFile);

        // Sort dates within each URL path for consistent output
        foreach ($results as &$urlPath) {
            ksort($urlPath);
        }

        unset($urlPath);

        // Reorder results to match the order URLs first appeared in the file
        $orderedResults = [];
        foreach ($urlOrder as $urlPath) {
            if (isset($results[$urlPath])) {
                $orderedResults[$urlPath] = $results[$urlPath];
            }
        }
        $results = $orderedResults;

        // Convert to JSON with pretty formatting
        $jsonOutput = json_encode($results, JSON_PRETTY_PRINT);
        
        if ($jsonOutput === false) {
            exit();
        }

        // Write to output file
        $bytesWritten = file_put_contents($outputPath, $jsonOutput);
        
        if ($bytesWritten === false) {
            exit();
        }
    }
}