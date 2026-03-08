<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $stream = fopen($inputPath, 'r');
        $data = [];

        while ($line = fgets($stream)) {
            $path = substr($line, 19, -27);
            $date = substr($line, -26, 10);

            $data[$path][$date] = ($data[$path][$date] ?? 0) + 1;
        }

        foreach ($data as &$dates) {
            ksort($dates, SORT_STRING);
        }

        file_put_contents($outputPath, json_encode($data, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE, 2));
    }
}
