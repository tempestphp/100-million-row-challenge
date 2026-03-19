<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $data = [];
        $urlCache = [];
        
        $file = fopen($inputPath, 'r');
        if (!$file) {
            throw new Exception("Cannot open input file: {$inputPath}");
        }
        
        // Increase read buffer size
        stream_set_chunk_size($file, 64 * 1024);

        try {
            while (($line = fgets($file)) !== false) {
                $commaPos = strpos($line, ',');
                if ($commaPos === false) {
                    continue;
                }

                $url = substr($line, 0, $commaPos);
                
                if (isset($urlCache[$url])) {
                    $path = $urlCache[$url];
                } else {
                    $slashPos = strpos($url, '/', 8); // Skip https://
                    $path = $urlCache[$url] = $slashPos === false ? '/' : substr($url, $slashPos);
                }
                
                $date = substr($line, ++$commaPos, 10);

                $data[$path][$date] = ($data[$path][$date] ?? 0) + 1;
            }

            foreach ($data as &$dates) {
                ksort($dates);
            }
            unset($dates);

            file_put_contents($outputPath, json_encode($data, JSON_PRETTY_PRINT));
        } finally {
            fclose($file);
        }
    }
}