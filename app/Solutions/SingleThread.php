<?php

namespace App\Solutions;

class SingleThread
{
    const BASE_URL = 'https://stitcher.io';
    const DATE_LENGTH = 10;
    const DATE_TOTAL_LENGTH = 25;

    public function __invoke(string $inputPath, string $outputPath): void
    {
        $handle = fopen($inputPath, 'rb');
        $result = [];

        $lenghtBaseUrl = strlen(self::BASE_URL);
   
        while ($line = fgets($handle)) {
           
            $path = substr($line, $lenghtBaseUrl, -self::DATE_TOTAL_LENGTH - 2);
            $date = substr($line, -self::DATE_TOTAL_LENGTH - 1, self::DATE_LENGTH);
            
            if (isset($result[$path][$date])) {
                $result[$path][$date]++;
            } else {
                $result[$path][$date] = 1;
            }
            
        }
      
        fclose($handle);

        foreach ($result as &$dates) {
            ksort($dates);
        }
        
        file_put_contents($outputPath, json_encode($result, JSON_PRETTY_PRINT));
    }
}