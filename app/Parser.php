<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $handle = fopen($inputPath, 'r');
        if (!$handle) {
            throw new Exception("Could not open input file: {$inputPath}");
        }

        stream_set_chunk_size($handle, 8388608);

        $visits = [];

        while (($line = fgets($handle)) !== false) {
            if (strlen($line) < 27)
                continue;

            // do we know the domain length?
            $path = substr($line, 19, -27);
            $date = substr($line, -26, 10);

            if (!isset($visits[$path])) {
                $visits[$path] = [$date => 1];
            } elseif (!isset($visits[$path][$date])) {
                $visits[$path][$date] = 1;
            } else {
                $visits[$path][$date]++;
            }
        }

        fclose($handle);

        foreach ($visits as &$dateCounts) {
            ksort($dateCounts);
        }
        unset($dateCounts);

        file_put_contents($outputPath, json_encode($visits, JSON_PRETTY_PRINT));
    }
}
