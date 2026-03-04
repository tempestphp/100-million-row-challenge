<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        ini_set('memory_limit', -1);
        gc_disable();

        $bufferSize = 64 * 1024; // Use 64KB buffer 

        // Open input file for reading
        $inputFile = fopen($inputPath, 'r');
        stream_set_read_buffer($inputFile, $bufferSize);

        $buffer = '';

        // Initialize results array to store URL path -> date -> count mappings
        // Use an associative array to maintain insertion order
        $results = [];
        $urlOrder = []; // Track order of first appearance

        // Read file in chunks
        printf("Reading input file in chunks...\n");
        while (!feof($inputFile)) {
            $chunk = fread($inputFile, $bufferSize);
            if ($chunk === false) break;
          
            $buffer .= $chunk;
          
            // Process complete lines from buffer
            $this->processBuffer($buffer, $results, $urlOrder);
        }
        printf("Finished reading input file.\n");

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