<?php

namespace App;

final class Parser
{
    private const PATH_START = 19;

    public function parse(string $inputPath, string $outputPath): void
    {
        $handle = fopen($inputPath, 'r');
        if ($handle === false) {
            return;
        }

        $visits = [];

        
        while (($line = fgets($handle)) !== false) {
            $separatorPosition = strrpos($line, ',');
            if ($separatorPosition === false || $separatorPosition < self::PATH_START) {
                continue;
            }

            $path = substr($line, self::PATH_START, $separatorPosition - self::PATH_START);

            $date = substr($line, $separatorPosition + 1, 10);
            $visits[$path][$date] = ($visits[$path][$date] ?? 0) + 1;
        }


        foreach ($visits as &$dailyVisits) {
            if (count($dailyVisits) > 1) {
                ksort($dailyVisits);
            }
        }
        unset($dailyVisits);


        // Only for Windows
        // $json = json_encode($visits, JSON_PRETTY_PRINT);
        //
        // if ($json === false) {
        //     return;
        // }
        //
        // if (PHP_EOL !== "\n") {
        //     $json = str_replace("\n", PHP_EOL, $json);
        // }
        //
        // file_put_contents($outputPath, $json);
        file_put_contents($outputPath, json_encode($visits, JSON_PRETTY_PRINT));
    }
}
