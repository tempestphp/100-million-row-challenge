<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $results = [];
        $resource = \fopen($inputPath, 'r');

        while (($line = \fgetcsv($resource, escape: '\\')) !== FALSE) {
            $path = \str_replace('https://stitcher.io', '', $line[0]);
            $date = \substr($line[1], 0, 10);
            if (!isset($results[$path])) {
                $results[$path] = [$date => 1];
            } else {
                if (!isset($results[$path][$date])) {
                    $results[$path][$date] = 1;
                } else {
                    $results[$path][$date] += 1;
                }
            }
        }
        \fclose($resource);

        foreach ($results as $path => &$path_arr) {
            \ksort($path_arr);
        }

        \file_put_contents($outputPath, \json_encode($results, \JSON_PRETTY_PRINT));
    }
}
