<?php

namespace App;

final class Parser
{
    private function processBuffer(string &$buffer, array &$results, array &$urlOrder): void
    {
        $offset = 0;
        $bufferLength = strlen($buffer);
        
        while ($offset < $bufferLength) {
            // Find next delimiter efficiently
            $commaPos = strpos($buffer, ',', $offset);
            if ($commaPos === false) {
                // No comma found, skip to next line
                $newlinePos = strpos($buffer, "\n", $offset);
                if ($newlinePos === false) {
                    // Partial line at end
                    $buffer = substr($buffer, $offset);
                    return;
                }
                $offset = $newlinePos + 1;
                continue;
            }
            
            $newlinePos = strpos($buffer, "\n", $commaPos);
            if ($newlinePos === false) {
                // Incomplete line at end
                $buffer = substr($buffer, $offset);
                return;
            }
            
            // Extract URL path and date efficiently
            $urlPath = substr($buffer, $offset + 19, $commaPos - $offset - 19);
            $date = substr($buffer, $commaPos + 1, $newlinePos - $commaPos - 16);
            
            // Process the data
            // Track order of first appearance
            if (!isset($results[$urlPath])) {
                $results[$urlPath] = [];
                $urlOrder[] = $urlPath;
            }
            
            if (!isset($results[$urlPath][$date])) {
                $results[$urlPath][$date] = 0;
            }
            
            $results[$urlPath][$date]++;

            $offset = $newlinePos + 1;
        }
        
        $buffer = '';
    }

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