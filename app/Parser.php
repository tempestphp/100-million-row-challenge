<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $file = fopen($inputPath, 'r');
        $collector = [];
        while (($line = fgets($file)) !== false) {
            $comma = strpos($line, ',');
            $firstKey = substr($line, 19, $comma-19);
            $secondKey = substr($line, $comma + 1, -16);
            if (!isset($collector[$firstKey][$secondKey])) {
                $collector[$firstKey][$secondKey] = 0;
            }
            $collector[$firstKey][$secondKey]++;
        }

        fclose($file);

        foreach ($collector as &$dates) {
            ksort($dates);
        }
        
        file_put_contents($outputPath, json_encode($collector, JSON_PRETTY_PRINT));
    }
}
