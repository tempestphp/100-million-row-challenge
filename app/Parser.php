<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $fp = fopen($inputPath, 'r');
        stream_set_read_buffer($fp, 65536);

        $data = [];

        while (($line = stream_get_line($fp, 4096, "\n")) !== false) {
            $commaPos = strpos($line, ',', 19);
            $key = substr($line, 19, $commaPos + 11 - 19);

            if (isset($data[$key])) {
                $data[$key]++;
            } else {
                $data[$key] = 1;
            }
        }

        $nestedData = [];
        foreach ($data as $key => $count) {
            $nestedData[substr($key, 0, -11)][substr($key, -10)] = $count;
        }

        foreach ($nestedData as &$values) {
            ksort($values);
        }

        file_put_contents($outputPath, json_encode($nestedData, JSON_PRETTY_PRINT));
    }
}