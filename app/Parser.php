<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $stream = fopen($inputPath, 'r');

        while($line = fgets($stream)) {
            $string = substr($line, 19);
            $array = explode(',', $string);
            $uri = $array[0];
            $date = substr($array[1], 0, 10);

            if (!isset($result[$uri][$date])) {
                $result[$uri][$date] = 0;
            }

            $result[$uri][$date]++;
        }

        foreach ($result as &$dates) {
            ksort($dates);
        }

        $json = json_encode($result, flags: \JSON_PRETTY_PRINT);

        file_put_contents($outputPath, $json);
    }
}