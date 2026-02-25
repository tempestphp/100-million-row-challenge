<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $handle = fopen($inputPath, 'r');

        $output = [];
        while (($line = fgets($handle)) !== false) {
            $len = strlen($line);
            $url = substr($line, 19, $len - 46);
            $date = substr($line, $len - 26, 10);
            $output[$url][$date] = ($output[$url][$date] ?? 0) + 1;
        }

        foreach ($output as &$dates) {
            ksort($dates, SORT_STRING);
        }

        $outputHandle = fopen($outputPath, 'w');
        fwrite($outputHandle, json_encode($output, JSON_PRETTY_PRINT));
    }
}