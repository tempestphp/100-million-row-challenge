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
        $handle = fopen($inputPath, 'rb');
        if (!$handle) {
            throw new RuntimeException("Could not open input file: $inputPath");
        }

        $visits = [];

        while (($line = fgets($handle)) !== false) {
            $line = rtrim($line, "\r\n");
            if ($line === '') {
                continue;
            }

            $commaPos = strpos($line, ',');
            if ($commaPos === false) {
                continue;
            }

            $url = substr($line, 0, $commaPos);
            $timestamp = substr($line, $commaPos + 1);

            // Manual path extraction from URL (find the first '/' after "://")
            $path = '/';
            $protoEnd = strpos($url, '://');
            if ($protoEnd !== false) {
                $pathStart = strpos($url, '/', $protoEnd + 3);
                if ($pathStart !== false) {
                    $path = substr($url, $pathStart);
                }
            }

            $date = substr($timestamp, 0, 10); //YYYY-MM-DD

            if (!isset($visits[$path])) {
                $visits[$path] = [];
            }

            if (!isset($visits[$path][$date])) {
                $visits[$path][$date] = 0;
            }

            $visits[$path][$date]++;
        }

        fclose($handle);

        foreach ($visits as $path => $dates) {
            ksort($visits[$path]);
        }

        file_put_contents(
            $outputPath,
            json_encode($visits, JSON_PRETTY_PRINT)
        );
    }
}