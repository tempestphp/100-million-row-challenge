<?php

namespace App;

use RuntimeException;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        // Track path order as we encounter them in input (insertion order)
        $pathToOrder  = [];
        $orderToPath  = [];
        $orderCounter = 0;

        // Aggregation array: [path_order][date] = count
        // Maximum size: 280 paths × ~1825 dates = ~511k entries
        $data = [];

        // Open input file with large buffer
        $handle = fopen($inputPath, 'rb');
        if (!$handle) {
            throw new RuntimeException("Cannot open input file: {$inputPath}");
        }
        stream_set_read_buffer($handle, 8 * 1024 * 1024); // 8MB buffer

        while (($line = fgets($handle)) !== false) {
            // Find comma position
            $commaPos = strpos($line, ',');
            if ($commaPos === false) {
                continue;
            }

            // Extract path directly - skip 'https://stitcher.io' (19 chars)
            $path = substr($line, 19, $commaPos - 19);

            // Extract date: YYYY-MM-DD is exactly 10 chars after comma
            $date = substr($line, $commaPos + 1, 10);

            // Get or assign order for this path (preserve insertion order)
            if (!isset($pathToOrder[$path])) {
                $pathToOrder[$path]         = $orderCounter;
                $orderToPath[$orderCounter] = $path;
                $pathOrder                  = $orderCounter;
                $orderCounter++;
            } else {
                $pathOrder = $pathToOrder[$path];
            }

            // Aggregate count
            if (isset($data[$pathOrder][$date])) {
                $data[$pathOrder][$date]++;
            } else {
                $data[$pathOrder][$date] = 1;
            }
        }

        fclose($handle);

        // Generate JSON output directly from array
        $this->generateJson($orderToPath, $data, $outputPath);
    }

    private function generateJson(array $orderToPath, array $data, string $outputPath): void
    {
        // Open output file
        $outHandle = fopen($outputPath, 'wb');
        if (!$outHandle) {
            throw new RuntimeException("Cannot open output file: {$outputPath}");
        }
        stream_set_write_buffer($outHandle, 8 * 1024 * 1024); // 8MB buffer

        fwrite($outHandle, "{");

        $firstPath = true;

        // Iterate in path order (already sorted by insertion order)
        foreach ($orderToPath as $order => $path) {
            if (!isset($data[$order])) {
                continue; // Skip paths with no visits
            }

            $dates = $data[$order];

            // Close previous path
            if (!$firstPath) {
                fwrite($outHandle, ",");
            }
            $firstPath = false;

            // Write path key
            $escapedPath = $this->escapeJsonString($path);
            fwrite($outHandle, "\n    \"{$escapedPath}\": {");

            // Sort dates and write entries
            ksort($dates);

            $firstDate = true;
            foreach ($dates as $date => $count) {
                if (!$firstDate) {
                    fwrite($outHandle, ",");
                }
                $firstDate = false;
                fwrite($outHandle, "\n        \"{$date}\": {$count}");
            }

            fwrite($outHandle, "\n    }");
        }

        fwrite($outHandle, "\n}");
        fclose($outHandle);
    }

    private function escapeJsonString(string $str): string
    {
        return str_replace(
            ['\\', '"', '/', "\b", "\f", "\n", "\r", "\t"],
            ['\\\\', '\\"', '\\/', '\\b', '\\f', '\\n', '\\r', '\\t'],
            $str,
        );
    }
}
