<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        // theoxskyl 
        $fp = fopen($inputPath, 'r');

        $host    = 'https://stitcher.io';
        $hostLen = strlen($host);

        $output = [];
        while (($line = fgets($fp)) !== false) {
            //$line = rtrim($line, "\r\n");

            $pos = strpos($line, ',');
            if ($pos === false) {
                continue;
            }

            $url = substr($line, 0, $pos);
            $rest   = substr($line, $pos + 1);

            if ($rest === '') {
                continue;
            }

            $date = substr($rest, 0, 10);

            if (strncmp($url, $host, $hostLen) === 0) {
                $url = substr($url, $hostLen);
            } else {
                $url = $url;
            }

            $output[$url] = $output[$url] ?? [];
            $output[$url][$date] = ($output[$url][$date] ?? 0) + 1;
        }

        fclose($fp);

        foreach ($output as $idx => $o) {
            ksort($output[$idx]);
        }

        file_put_contents($outputPath, json_encode($output, JSON_PRETTY_PRINT));
    }
}
