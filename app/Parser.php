<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $handle = fopen($inputPath, 'r');
        $data = [];

        while ($line = fgets($handle, 256)) {
            $path = substr($line, 19, -27);
            $date = substr($line, -26, 10);

            $data[$path][$date] ??= 0;
            $data[$path][$date] += 1;
        }

        foreach ($data as &$visits) {
            ksort($visits);
        }

        file_put_contents($outputPath, json_encode($data, JSON_PRETTY_PRINT));

        fclose($handle);
    }
}
