<?php

namespace App\Solutions;

class Papoon
{
    public function __invoke(string $inputPath, string $outputPath): void
    {
        $result = [];
        gc_disable();

        $handle = fopen($inputPath, 'rb');
        while (($line = fgets($handle)) !== false) {
            
            [$urlPart, $rest] = explode(',', $line, 2);
            $date = substr($rest, 0, 10);
            
            $pathStart = strpos($urlPart, '/', 8);
            $path = substr($urlPart, $pathStart);
            
            $result[$path][$date] = ($result[$path][$date] ?? 0) + 1;
        }
        fclose($handle);
        gc_enable();
        
        $this->writeOutput($outputPath, $result);
    }

    private function writeOutput(string $outputPath, array $result): void
    {
        $handle = fopen($outputPath, 'wb');
        foreach ($result as &$dates) {
            ksort($dates);
        }
        unset($dates);
    
        fwrite($handle, json_encode($result, JSON_PRETTY_PRINT));
        fclose($handle);
    }
}
