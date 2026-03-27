<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {

        $output = [];

        $file = fopen($inputPath, 'r');

        while (($line = fgets($file, 8192)) !== false) {

            preg_match('/^https:\/\/stitcher\.io(.*),(.*)T(.*)$/', $line, $matches);

            if(!isset($output[$matches[1]])) {
                $output[$matches[1]] = [];
            }

            if(!isset($output[$matches[1]][$matches[2]])) {
                $output[$matches[1]][$matches[2]] = 1;
            } else {
                $output[$matches[1]][$matches[2]]++;
            }

        }

        foreach($output as &$value1) {
            ksort($value1);
        }

        file_put_contents($outputPath, json_encode($output, JSON_PRETTY_PRINT));

    }
}