<?php

namespace App;

use Exception;
use RuntimeException;

final class Parser
{
    /**
     * @throws Exception
     */
    public function parse(string $inputPath, string $outputPath): void
    {
        $handle = fopen($inputPath, 'r');
        if ($handle === false) {
            throw new RuntimeException("Could not open input file: $inputPath");
        }

        $visits = [];

        while (($line = fgets($handle)) !== false) {
            $commaPos = strpos($line, ',');
            if ($commaPos === false) continue;

            $pathStart = strpos($line, '/', 8);

            if ($pathStart !== false && $pathStart < $commaPos) {
                $path = substr($line, $pathStart, $commaPos - $pathStart);
            } else {
                $path = '/';
            }

            $date = substr($line, $commaPos + 1, 10);

            if (isset($visits[$path])) {
                if (isset($visits[$path][$date])) {
                    $visits[$path][$date]++;
                } else {
                    $visits[$path][$date] = 1;
                }
            } else {
                $visits[$path] = [$date => 1];
            }
        }

        fclose($handle);

        foreach ($visits as &$dates) {
            ksort($dates);
        }

        file_put_contents(
            $outputPath,
            json_encode($visits, JSON_PRETTY_PRINT)
        );
    }
}