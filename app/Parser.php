<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $start = microtime(true);

        $data = [];
        $pathOrder = [];

        $handle = fopen($inputPath, 'rb');
        if (!$handle) throw new Exception("Cannot open input file: $inputPath");

        $chunkSize = 1024 * 1024; // 1 MB buffer
        $buffer = '';

        while (!feof($handle)) {
            $read = fread($handle, $chunkSize);
            if ($read === false || $read === '') break;
            $buffer .= $read;

            $lastOffset = 0;
            while (($newlinePos = strpos($buffer, "\n", $lastOffset)) !== false) {
                $line = rtrim(substr($buffer, $lastOffset, $newlinePos - $lastOffset), "\r");
                $lastOffset = $newlinePos + 1;

                if ($line === '') continue;
                
                $commaPos = strpos($line, ',');
                if ($commaPos === false) continue;

                $url = substr($line, 0, $commaPos);
                $datetime = substr($line, $commaPos + 1, 10); 

                $pathStart = strpos($url, '/', 8);
                $path = $pathStart === false ? '/' : substr($url, $pathStart);
                
                if (!isset($data[$path])) {
                    $pathOrder[] = $path;
                    $data[$path] = [];
                }

                $dates =& $data[$path];
                $dates[$datetime] = ($dates[$datetime] ?? 0) + 1;
            }
            
            $buffer = substr($buffer, $lastOffset);
        }

        fclose($handle);
        
        foreach ($data as &$dates) ksort($dates);
        unset($dates);
        
        $orderedData = [];
        foreach ($pathOrder as $path) {
            $orderedData[$path] = $data[$path];
        }
        
        $json = json_encode($orderedData, JSON_PRETTY_PRINT);
        if ($json === false) throw new Exception('JSON encoding failed: ' . json_last_error_msg());
        $json = str_replace("\n", "\r\n", $json);

        file_put_contents($outputPath, $json);

        $time = microtime(true) - $start;
        $mem = memory_get_peak_usage(true) / 1024 / 1024;

        echo "Time: " . round($time, 3) . " s | Peak: " . round($mem, 1) . " MB\n";
    }
}