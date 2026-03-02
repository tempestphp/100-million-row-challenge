<?php

declare(strict_types=1);

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $stream = fopen($inputPath, 'r');
        $data = [];

        while ($line = fgets($stream)) {
            $comma = strpos($line, ',');
            $path = substr($line, 19, $comma - 19);
            $date = substr($line, $comma + 1, 10);

            if (!isset($data[$path])) {
                $data[$path] = [$date => 1];
                continue;
            }

            $visits = &$data[$path];
            $visits[$date] = ($visits[$date] ?? 0) + 1;
            // $data[$path][$date] = ($data[$path][$date] ?? 0) + 1;
        }

        foreach ($data as &$dates) {
            ksort($dates);
        }

        file_put_contents($outputPath, json_encode($data, JSON_PRETTY_PRINT, 2));
    }
}
