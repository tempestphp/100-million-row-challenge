<?php

declare(strict_types=1);

namespace App;

final class Parser
{
    private const int COMMA = -26; // from end of line (including newline)

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $stream = fopen($inputPath, 'r');
        $data = [];

        while ($line = fgets($stream)) {
            $path = substr($line, 19, self::COMMA - 1);
            $date = substr($line, self::COMMA, 10);

            if (!isset($data[$path])) {
                $data[$path] = [$date => 1];
                continue;
            }

            $visits = &$data[$path];
            $visits[$date] = ($visits[$date] ?? 0) + 1;
            // $data[$path][$date] = ($data[$path][$date] ?? 0) + 1;
        }

        foreach ($data as &$dates) {
            ksort($dates, SORT_STRING);
        }

        file_put_contents($outputPath, json_encode($data, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE, 2));
    }
}
