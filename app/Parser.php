<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $handle = fopen($inputPath, 'rb');
        if (!$handle) {
            throw new Exception("Cannot open input file: {$inputPath}");
        }

        // Read 4MB at a time from the OS instead of one line at a time
        stream_set_read_buffer($handle, 4 * 1024 * 1024);

        $visits = [];

        while (($line = fgets($handle)) !== false) {
            $commaPos = strpos($line, ',', 19);
            if ($commaPos === false) {
                continue;
            }

            $path = substr($line, 19, $commaPos - 19);
            $date = substr($line, $commaPos + 1, 10);

            $visits[$path][$date] = ($visits[$path][$date] ?? 0) + 1;
        }

        fclose($handle);

        foreach ($visits as &$dateCounts) {
            ksort($dateCounts);
        }

        $json = json_encode($visits, JSON_PRETTY_PRINT);
        $json = str_replace("\n", "\r\n", $json);

        file_put_contents($outputPath, $json);
    }
}
