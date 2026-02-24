<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $counts = [];
        $handle = fopen($inputPath, 'r');

        set_error_handler(function () {});

        while (($line = stream_get_line($handle, 256, "\n")) !== false) {
            $counts[substr($line, 19, -15)]++;
        }

        restore_error_handler();
        fclose($handle);

        $data = [];
        foreach ($counts as $key => $count) {
            $commaPos = strrpos($key, ',');
            $data[substr($key, 0, $commaPos)][substr($key, $commaPos + 1)] = $count;
        }
        unset($counts);

        foreach ($data as &$dates) {
            ksort($dates);
        }

        file_put_contents($outputPath, json_encode($data, JSON_PRETTY_PRINT));
    }
}
