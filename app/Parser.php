<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        ini_set('memory_limit', '-1');
        gc_disable();

        $fp = \fopen($inputPath, 'r');
        if ($fp === false) {
            throw new \RuntimeException("Unable to open file: $inputPath");
        }

        $result = [];
        $dateCache = [];

        while (($line = \fgets($fp)) !== false) {
            $path = \substr($line, 19, -27); // 19 = strlen('https://stitcher.io'), -27 removes , and date
            $dateStr = \substr($line, -26, 10); // 10 = strlen('YYYY-MM-DD')

            if (!isset($dateCache[$dateStr])) {
                $dateCache[$dateStr] = (int) \str_replace('-', '', $dateStr);
            }
            $dateInt = $dateCache[$dateStr];

            // Fast hash lookup
            if (isset($result[$path][$dateInt])) {
                $result[$path][$dateInt]++;
            } else {
                $result[$path][$dateInt] = 1;
            }
        }

        \fclose($fp);

        // Sort dates for each fully accumulated path
        foreach ($result as &$dates) {
            \ksort($dates, SORT_NUMERIC);
        }

        $this->writeOutput($result, $outputPath);
    }

    private function writeOutput(array $result, string $outputPath): void
    {
        // Build ordered result (paths are already natively insertion-sorted, dates are sorted)
        $ordered = [];
        foreach ($result as $path => $dates) {
            $formattedDates = [];
            foreach ($dates as $dateInt => $count) {
                // Use math to extract YYYY, MM, DD
                $d = $dateInt % 100;
                $m = \intdiv($dateInt, 100) % 100;
                $y = \intdiv($dateInt, 10000);

                $formattedKey = \sprintf('%04d-%02d-%02d', $y, $m, $d);
                $formattedDates[$formattedKey] = $count;
            }
            $ordered[$path] = $formattedDates;
        }

        \file_put_contents($outputPath, \json_encode($ordered, JSON_PRETTY_PRINT));
    }
}
