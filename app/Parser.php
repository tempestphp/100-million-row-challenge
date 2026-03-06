<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $f = fopen($inputPath, 'r');

        $json = [];

        while (($line = fgets($f)) !== false) {
            $comma = strpos($line, ',');
            $pathStart = strpos($line, '/', strpos($line, '//') + 2);
            $path = substr($line, $pathStart, $comma - $pathStart);
            $date = substr($line, $comma + 1, 10);

            $json[$path][$date] = ($json[$path][$date] ?? 0) + 1;
        }

        fclose($f);

        foreach ($json as &$dates) {
            ksort($dates);
        }

        file_put_contents($outputPath, json_encode($json, JSON_PRETTY_PRINT));
    }
}
