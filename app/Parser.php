<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();
        $csv = fopen($inputPath, 'r');

        $all = [];

        while ($line = fgets($csv)) {
            $separated = strcspn($line, ',');
            $path = substr($line, offset: 19, length: $separated - 19);
            $date = substr($line, offset: $separated + 1, length: 10);
            $all[$path][$date] ??= 0;
            $all[$path][$date]++;

        }
        fclose($csv);
        foreach ($all as &$dates) {
            ksort($dates, SORT_STRING);
        }

        file_put_contents(
            $outputPath,
            json_encode(
                $all,
                JSON_PRETTY_PRINT
            )
        );
    }
}