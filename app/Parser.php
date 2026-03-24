<?php

namespace App;

final class Parser
{
    public static function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();
        $handle = fopen($inputPath, 'rb');
        
        stream_set_chunk_size($handle, 128 * 1024);

        $counts = [];
        $urlMap = [];
        $urls = [];
        $nextUrlId = 0;

        while (($line = fgets($handle)) !== false) {
            $commaPos = strrpos($line, ',');
            if ($commaPos === false) continue;

            $slashPos = strpos($line, '/', 8);
            if ($slashPos !== false && $slashPos < $commaPos) {
                $path = substr($line, $slashPos, $commaPos - $slashPos);
            } else {
                $path = '/';
            }

            if (isset($urlMap[$path])) {
                $id = $urlMap[$path];
            } else {
                $id = $nextUrlId++;
                $urlMap[$path] = $id;
                $urls[$id] = $path;
                $counts[$id] = [];
            }

            $date = substr($line, $commaPos + 1, 10);

            if (isset($counts[$id][$date])) {
                ++$counts[$id][$date];
            } else {
                $counts[$id][$date] = 1;
            }
        }
        
        fclose($handle);

        $output = [];
        foreach ($counts as $id => $dates) {
            ksort($dates);
            $output[$urls[$id]] = $dates;
        }

        file_put_contents($outputPath, json_encode($output, JSON_PRETTY_PRINT));
    }
}
