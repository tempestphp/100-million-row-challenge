<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $input = \fopen($inputPath, 'r');
        $outputData = [];
        while ($line = \fgets($input)) {
            [$path, $date] = \explode(',', $line, 2);
            $path = \substr($path, 19);
            $date = \substr($date, 0, 10);
            $outputData[$path][$date] ??= 0;
            $outputData[$path][$date]++;
        }
        \fclose($input);

        foreach ($outputData as $path => $dates) {
            \ksort($outputData[$path]);
        }

        $outputJson = \json_encode($outputData, \JSON_PRETTY_PRINT);
        \file_put_contents($outputPath, $outputJson);
    }
}