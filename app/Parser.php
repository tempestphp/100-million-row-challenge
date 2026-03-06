<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();
        $csv = fopen($inputPath, 'r');

        $all = [];

        $globalMinDay = PHP_INT_MAX;
        $globalMaxDay = 0;

        while (($line = fgets($csv)) !== false) {
            $commaPos = strpos($line, ',');
            $path = substr($line, 19, $commaPos - 19);
            $dateOffset = $commaPos + 1;

            $year  = (int)substr($line, $dateOffset, 4);
            $month = (int)substr($line, $dateOffset + 5, 2);
            $day   = (int)substr($line, $dateOffset + 8, 2);

            $dayId = $year * 10000 + $month * 100 + $day;

            $globalMinDay = min($globalMinDay, $dayId);
            $globalMaxDay = max($globalMaxDay, $dayId);

            $all[$path][$dayId] ??= 0;
            $all[$path][$dayId]++;
        }

        fclose($csv);

        foreach ($all as &$dates) {
            ksort($dates, SORT_NUMERIC);
        }

        foreach ($all as $path => &$dates) {
            foreach ($dates as $dayId => $count) {
                $year  = intdiv($dayId, 10000);
                $month = intdiv($dayId % 10000, 100);
                $day   = $dayId % 100;

                $dateString = sprintf('%04d-%02d-%02d', $year, $month, $day);
                unset($dates[$dayId]);
                $all[$path][$dateString] = $count;
            }
        }

        file_put_contents(
            $outputPath,
            json_encode($all, JSON_PRETTY_PRINT )
        );
    }
}