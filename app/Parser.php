<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $result = [];
        $endresult = [];

        $handle = fopen($inputPath, 'r');

        while ($line = fgets($handle)) {
            $pos = strpos($line, ',');
            $url = substr($line, 0, $pos);
            $timestamp = substr($line, $pos + 1, 10);

            $result[$url] ??= [];
            $result[$url][$timestamp] =
                ($result[$url][$timestamp] ?? 0) + 1;
        }

        fclose($handle);

        foreach ($result as $index => &$row) {
            ksort($row);
            $endresult[str_replace('https://stitcher.io','',$index)] = $row;
        }

        file_put_contents($outputPath, json_encode($endresult, JSON_PRETTY_PRINT));
    }
}