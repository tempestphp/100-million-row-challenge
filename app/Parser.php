<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $prefixLength = 19;

        $inputFile = fopen($inputPath, 'rb');
        stream_set_read_buffer($inputFile, 1 << 24);

        $visits = [];

        gc_disable();

        while (($line = fgets($inputFile)) !== false) {
            $comma = strpos($line, ',', $prefixLength);
            $date = substr($line, $comma + 1, 10);
            $path = substr($line, $prefixLength, $comma - $prefixLength);
            $visits[$path][$date] = ($visits[$path][$date] ?? 0) + 1;
        }

        foreach ($visits as &$byDate) {
            ksort($byDate, SORT_STRING);
        }

        $json = json_encode($visits, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE, 2);
        file_put_contents($outputPath, $json);
    }
}