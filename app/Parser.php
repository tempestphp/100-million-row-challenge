<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $result = [];

        $handle = fopen($inputPath, 'r');
        while (($line = fgets($handle)) !== false) {
            $commaPosition = strpos($line, ',');

            $url = substr($line, 19, $commaPosition - 19);
            $date = substr($line, $commaPosition + 1, 10);

            if (!isset($result[$url][$date])) {
                $result[$url][$date] = 1;
            } else {
                $result[$url][$date]++;
            }
        }

        array_walk($result, fn(&$dates) => ksort($dates));

        file_put_contents($outputPath, json_encode($result, JSON_PRETTY_PRINT));
    }
}
