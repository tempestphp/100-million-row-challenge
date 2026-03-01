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
            if (false === $commaPos) {
                break;
            }

            $url = substr($line, 19, $commaPos - 19);
            $timestamp = substr($line, $commaPos + 1, 10);

            if (isset($outArray[$url][$timestamp])) {
                ++$outArray[$url][$timestamp];
            } else {
                $outArray[$url][$timestamp] = 1;
            }
        }

        fclose($handle);
        file_put_contents(
            $outputPath,
            str_replace([' ', "\n"], '', json_encode($outArray))
        );
    }
}
