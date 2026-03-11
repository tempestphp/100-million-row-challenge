<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $handle = fopen($inputPath, 'r');
        $data = [];

        while(true) {
            $chunk = fread($handle, 1048576);
            $lines = explode("\n", $chunk);
            $remainder = array_pop($lines);

            foreach ($lines as $line) {
                $commaPos = strpos($line, ',');
                $date = substr($line, $commaPos + 1, 10);
                $url = substr($line, 19, $commaPos - 19);
                if (!isset($data[$url])) {
                    $data[$url] = [];
                }
                $count = &$data[$url][$date];
                $count++;
            }
            if ($remainder === '') {
                break;
            }
        }

        fclose($handle);

        foreach ($data as &$dates) {
            ksort($dates);
        }

        file_put_contents($outputPath, json_encode($data, JSON_PRETTY_PRINT));
    }
}
