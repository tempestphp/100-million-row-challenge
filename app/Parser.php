<?php

namespace App;

final class Parser
{
    public const URL_HOST = 'https://stitcher.io';

    public function parse(string $inputPath, string $outputPath): void
    {
        $file = \fopen($inputPath, 'r');
        $outputData = [];
        while ($line = \fgets($file)) {
            [$url, $date] = \explode(',', $line, 2);
            $path = \explode(self::URL_HOST, $url)[1];
            $date = \substr($date, 0, 10);
            if (isset($outputData[$path][$date])) {
                ++$outputData[$path][$date];
            } else {
                $outputData[$path][$date] = 1;
            }
        }
        \fclose($file);

        foreach ($outputData as &$item) {
            \ksort($item, SORT_STRING);
        }
        unset($item);

        \file_put_contents($outputPath, \json_encode($outputData, JSON_PRETTY_PRINT));
    }
}