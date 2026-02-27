<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $result = [];
        foreach ($this->readLine($inputPath) as $line) {
            [$url, $date] = explode(',', $line);
            $url = str_replace('https://stitcher.io', '', $url);
            $date = substr($date, 0, 10);
            if (!isset($result[$url])) {
                $result[$url] = [];
            }
            if (!isset($result[$url][$date])) {
                $result[$url][$date] = 0;
            }
            $result[$url][$date]++;
        }

        foreach ($result as $url => &$dates) {
            ksort($dates);
        }

        file_put_contents($outputPath, json_encode($result, JSON_PRETTY_PRINT));
    }

    private function readLine(string $inputPath): \Generator
    {
        $handle = fopen($inputPath, 'r');
        if ($handle) {
            while (($line = fgets($handle)) !== false) {
                yield $line;
            }
            fclose($handle);
        }
    }
}
