<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $result = [];
        $handle = fopen($inputPath, 'r');
        if ($handle === false) {
            throw new \RuntimeException("Failed to open input file: $inputPath");
        }
        $pattern = '/https:\/\/stitcher\.io(\/blog\/[^,]+),(\d{4}-\d{2}-\d{2})T/';
        while (($line = fgets($handle)) !== false) {
            if (preg_match($pattern, $line, $match)) {
                $url = $match[1];
                $date = $match[2];
                $result[$url][$date] = ($result[$url][$date] ?? 0) + 1;
            }
        }
        fclose($handle);
        foreach ($result as $url => &$dates) {
            ksort($dates);
        }
        unset($dates);
        file_put_contents($outputPath, json_encode($result, JSON_PRETTY_PRINT));
    }
}
