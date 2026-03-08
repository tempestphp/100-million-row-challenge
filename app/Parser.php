<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $handle = fopen($inputPath, 'r');
        $visits = [];
    
        while (($line = fgets($handle)) !== false) {
            $separatorPosition = strlen($line) - 27;
            $path = substr($line, 19, $separatorPosition - 19);
            $date = substr($line, $separatorPosition + 1, 10);
            
            if (isset($visits[$path][$date])) {
                ++$visits[$path][$date];
            } else {
                $visits[$path][$date] = 1;
            }
        }

        foreach ($visits as &$dailyVisits) {
            ksort($dailyVisits);
        }

        file_put_contents($outputPath, json_encode($visits, JSON_PRETTY_PRINT));
    }
}
