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

        // Initialize results array to store key -> count mappings
        $results = [];

        $this->logTime('Initialized');

        // Open input file for reading
        $inputFile = fopen($inputPath, 'r');
        while (($line = fgets($inputFile)) !== false) {
            $path = substr($line, 19, -27);
            $date = substr($line, -26, 10);

            // Convert date to integer format YYYYMMDD for easier sorting and comparison
            $date = (int) str_replace('-', '', $date);

            $results[$path][$date] = ($results[$path][$date] ?? 0) + 1;
        }

        fclose($inputFile);
        
        $this->logTime('Parsed');
        
        // Sort dates within each URL path for consistent output
        foreach ($results as &$dates) {
            ksort($dates, SORT_NUMERIC);

            $modified = [];
            foreach ($dates as $key => $value) {
                $modified[
                    substr($key, 0, 4) . '-' .
                    substr($key, 4, 2) . '-' .
                    substr($key, 6, 2)
                ] = $value;
            }
            $dates = $modified;
        }

        unset($dates);

        // Write to output file
        file_put_contents($outputPath, json_encode($results, JSON_PRETTY_PRINT));
        
        $this->logTime('Written');
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