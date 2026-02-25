<?php

namespace App;

use RuntimeException;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $handle = fopen($inputPath, 'r');

        if (! $handle) {
            throw new RuntimeException('Cannot open input file');
        }

        $result = [];

        while (($line = fgets($handle)) !== false) {
            $line = trim($line);

            if ($line === '') {
                continue;
            }

            [$url, $dateTime] = explode(',', $line);

            $path = parse_url($url, PHP_URL_PATH);

            $date = substr($dateTime, 0, 10);

            if (! isset($result[$path])) {
                $result[$path] = [];
            }

            if (! isset($result[$path][$date])) {
                $result[$path][$date] = 0;
            }

            $result[$path][$date]++;
        }

        fclose($handle);

        foreach ($result as &$dates) {
            ksort($dates, SORT_STRING);
        }
        unset($dates);

        $json = json_encode(
            $result,
            JSON_PRETTY_PRINT
        );

// Windows-Zeilenenden erzwingen
$json = str_replace("\n", "\r\n", $json);

file_put_contents($outputPath, $json);

        file_put_contents($outputPath, $json);
    }
}
