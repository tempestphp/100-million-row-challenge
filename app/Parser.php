<?php

namespace App;

use Exception, RuntimeException;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $handle = fopen($inputPath, 'rb');

        if ($handle === false) {
            throw new RuntimeException("Unable to open input file: {$inputPath}");
        }

        $visits = [];

        while (($line = fgets($handle)) !== false) {
            $commaPos = strrpos($line, ',');

            if ($commaPos === false) {
                continue;
            }
             
            $pathPos = strpos($line, '/', 8);

            $path = $pathPos === false
                ? '/'
                : substr($line, $pathPos, $commaPos - $pathPos);

            // YYYY-MM-DD is first 10 chars after comma
            $day = substr($line, $commaPos + 1, 10);

            //1
            // if (isset($visits[$path][$day])) {
            //     ++$visits[$path][$day];
            // } elseif (isset($visits[$path])) {
            //     $visits[$path][$day] = 1;
            // } else {
            //     $visits[$path] = [$day => 1];
            // }

            //2
            // $visits[$path][$day] = ($visits[$path][$day] ?? 0) + 1;

            //3
            $perPath = &$visits[$path];
            $perPath[$day] = ($perPath[$day] ?? 0) + 1;
        }

        fclose($handle);

        foreach ($visits as &$perDay) {
            ksort($perDay, SORT_STRING);
        }
        unset($perDay);

        $json = json_encode($visits, JSON_PRETTY_PRINT);

        if ($json === false) {
            throw new RuntimeException('Failed to encode JSON: ' . json_last_error_msg());
        }

        // convert LF -> CRLF to pass validation
        // $json = str_replace("\n", "\r\n", $json);

        if (file_put_contents($outputPath, $json) === false) {
            throw new RuntimeException("Unable to write output file: {$outputPath}");
        }
    }
}
