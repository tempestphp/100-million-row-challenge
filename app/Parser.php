<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $handle = fopen($inputPath, 'rb');
        
        stream_set_chunk_size($handle, 128 * 1024);

        $counts = [];

        while (($line = fgets($handle)) !== false) {
            $commaPos = strrpos($line, ',');
            if ($commaPos === false) continue;

            $slashPos = strpos($line, '/', 8);
            if ($slashPos !== false && $slashPos < $commaPos) {
                $path = substr($line, $slashPos, $commaPos - $slashPos);
            } else {
                $path = '/';
            }

            $date = substr($line, $commaPos + 1, 10);

            if (isset($counts[$path][$date])) {
                ++$counts[$path][$date];
            } else {
                $counts[$path][$date] = 1;
            }
        }
        
        fclose($handle);

        foreach ($counts as &$dates) {
            ksort($dates);
        }
        unset($dates);

        file_put_contents($outputPath, json_encode($counts, JSON_PRETTY_PRINT));
    }
}
