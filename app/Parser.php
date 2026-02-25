<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $visitsByPath = [];

        $handle = fopen($inputPath, 'r');

        while (($line = fgets($handle)) !== false) {
            [$url, $timestamp] = explode(',', trim($line));
            $path = parse_url($url, PHP_URL_PATH);
            $date = substr($timestamp, 0, 10);

            $visitsByPath[$path][$date] = ($visitsByPath[$path][$date] ?? 0) + 1;
        }

        fclose($handle);

        foreach ($visitsByPath as &$dates) {
            ksort($dates);
        }

        file_put_contents($outputPath, json_encode($visitsByPath, JSON_PRETTY_PRINT));
    }
}