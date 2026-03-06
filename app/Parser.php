<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $file = fopen($inputPath, 'r');
        if (!$file) {
            throw new Exception("Could not open the file!");
        }

        $grouped = [];

        while (($line = fgets($file)) !== false) {
            $line = trim($line);

            if ($line === '') {
                continue;
            }

            [$url, $timestamp] = explode(',', $line);

            $path = parse_url($url, PHP_URL_PATH);
            $date = substr($timestamp, 0, 10);

            if (!isset($grouped[$path])) {
                $grouped[$path] = [];
            }

            if (!isset($grouped[$path][$date])) {
                $grouped[$path][$date] = 0;
            }

            $grouped[$path][$date]++;
        }

        fclose($file);

        foreach ($grouped as &$dates) {
            ksort($dates);
        }
        unset($dates);

        $jsonData = json_encode($grouped, JSON_PRETTY_PRINT);

        if (file_put_contents($outputPath, $jsonData) === false) {
            throw new Exception("Could not write to output file!");
        }
    }
}