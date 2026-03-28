<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $visitsByPath = [];

        $handle = fopen($inputPath, 'r');

        while (($line = fgets($handle)) !== false) {
            $comma = strpos($line, ',');

            $url = substr($line, 0, $comma);
            $datetime = substr($line, $comma + 1, 25);

            $pos = strpos($url, '/blog/');

            $path = substr($url, $pos);
            $date = substr($datetime, 0, 10);

            $visitsByPath[$path][$date] = ($visitsByPath[$path][$date] ?? 0) + 1;
        }

        fclose($handle);

        foreach ($visitsByPath as &$dates) {
            ksort($dates);
        }

        file_put_contents($outputPath, json_encode($visitsByPath, JSON_PRETTY_PRINT));
    }
}