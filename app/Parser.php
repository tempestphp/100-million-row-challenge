<?php

namespace App;

final class Parser
{
    private float $startTime;
    private ?float $previousTime = null;

    public function parse(string $inputPath, string $outputPath): void
    {
        $this->startTime = \microtime(true);

        ini_set('memory_limit', -1);
        gc_disable();

        $bufferSize = 64 * 1024; // Use 64KB buffer 

        // Open input file for reading
        $inputFile = fopen($inputPath, 'r');
        stream_set_read_buffer($inputFile, 0); // Disable buffering for real-time processing

        $buffer = '';

        // Initialize results array to store key -> count mappings
        $results = [];
        $visits = [];

        $this->logTime('Initialized');

        // Read file in chunks
        while (!feof($inputFile)) {
            $chunk = fread($inputFile, $bufferSize);
            if ($chunk === false) break;
          
            $buffer .= $chunk;
          
            // Process complete lines from buffer
            $this->processBuffer($buffer, $results);
        }
        
        $this->logTime('Parsed');

        fclose($inputFile);
        
        // Convert results to nested array format for output
        foreach ($results as $key => $count) {
            $split = strpos($key, '|');
            $path = substr($key, 0, $split);
            $date = substr($key, $split + 1);
            $visits[$path][$date] = $count;
        }

        // Sort dates within each URL path for consistent output
        foreach ($visits as &$urlPath) {
            ksort($urlPath);
        }

        unset($urlPath);

        // Write to output file
        file_put_contents($outputPath, json_encode($visits, JSON_PRETTY_PRINT));
        
        $this->logTime('Written');
    }

    private function processBuffer(string &$buffer, array &$results): void
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
            
            $key = $urlPath . '|' . $date; // Combine URL path and date for unique key

            $results[$key] ??= 0;
            ++$results[$key];

            $offset = $newlinePos + 1;
        }
        
        $buffer = '';
    }

    /**
     * Use the start time to calculate the elapsed time and output a message with the elapsed,
     * formatted time in seconds.
     */
    private function logTime(string $message): void
    {
        $elapsedTotal = number_format(\microtime(true) - $this->startTime, 4);
        $elapsedPrevious = number_format(\microtime(true) - ($this->previousTime ?? $this->startTime), 4);

        $this->previousTime = \microtime(true);

        printf("$message in $elapsedPrevious seconds, $elapsedTotal seconds in total.\n");
    }
}