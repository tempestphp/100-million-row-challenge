<?php

namespace App;

use DateTimeImmutable;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $handle = fopen($inputPath, 'r');

        $data = [];

        while (($line = fgets($handle)) !== false) {
            [$uri, $date] = explode(',', trim($line));

            $date = new DateTimeImmutable($date);
            $path = parse_url($uri, PHP_URL_PATH);

            $data[$path][$date->format('Y-m-d')] ??= 0;

            $data[$path][$date->format('Y-m-d')] += 1;
        }

        foreach ($data as &$visits) {
            ksort($visits);
        }

        fclose($handle);

        file_put_contents($outputPath, json_encode($data, JSON_PRETTY_PRINT));
    }
}