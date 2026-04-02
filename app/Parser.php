<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $handle = fopen($inputPath, 'r');

        $outArray = [];

        while (($line = fgets($handle)) !== false) {
            $commaPos = strpos($line, ',');
            // if (false === $commaPos) {
            //     break;
            // }

            $url = substr($line, 19, $commaPos - 19);
            $timestamp = substr($line, $commaPos + 1, 10);

            $outArray[$url][$timestamp] = ($outArray[$url][$timestamp] ?? 0) + 1;
        }

        foreach ($outArray as &$temp) {
            ksort($temp);
        }

        fclose($handle);
        file_put_contents(
            $outputPath,
            json_encode($outArray, JSON_PRETTY_PRINT)
        );
    }
}
