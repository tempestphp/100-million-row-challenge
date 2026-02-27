<?php

namespace App;

final class Parser
{
    private const int OFFSET_SITE = 25;

    public function parse(string $inputPath, string $outputPath): void
    {
        $csv = fopen($inputPath, 'r');
        $data = [];

        while ($line = fgets($csv)) {
            $offset = strcspn($line, ',');
            $path = substr($line, offset: self::OFFSET_SITE, length: $offset - self::OFFSET_SITE);
            $date = substr($line, offset: $offset + 1, length: 10);
            $data[$path][$date] ??= 0;
            $data[$path][$date] += 1;
        }

        fclose($csv);

        $json = '{' . PHP_EOL;

        foreach ($data as $path => $visits) {
            ksort($visits);
            $json .= '    "\/blog\/' . $path . '": {' . PHP_EOL;

            foreach ($visits as $date => $count) {
                $json .= '        "' . $date . '": ' . $count . ',' . PHP_EOL;
            }

            $json = substr($json, 0, -2) . PHP_EOL;

            $json .= '    },' . PHP_EOL;
        }

        $json = substr($json, 0, -2) . PHP_EOL;

        file_put_contents($outputPath, $json . '}');
    }
}