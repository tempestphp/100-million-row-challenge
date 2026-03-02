<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $output = [];
        $file = fopen($inputPath, 'r');

        while (($line = fgets($file)) !== false) {
            $url = substr($line, 19, -27);
            $date = substr($line, -26, 10);
            $date = (int) str_replace('-', '', $date);

            if (isset($output[$url][$date])) {
                $output[$url][$date]++;
            } else {
                $output[$url][$date] = 1;
            }
        }

        foreach ($output as &$dates) {
            ksort($dates, SORT_NUMERIC);

            $new = [];
            foreach ($dates as $key => $value) {
                $year = intdiv($key, 10000);
                $month = intdiv($key, 100) % 100;
                $day = $key % 100;

                $new[sprintf('%04d-%02d-%02d', $year, $month, $day)] = $value;
            }
            $dates = $new;
        }

        file_put_contents($outputPath, json_encode($output, JSON_PRETTY_PRINT));
    }
}