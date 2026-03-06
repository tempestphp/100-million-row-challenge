<?php

namespace App\Solutions;

class SingleThread
{
    const BASE_URL = 'https://stitcher.io';
    const DATE_LENGTH = 10;
    const DATE_TOTAL_LENGTH = 25;

    public function __invoke(string $inputPath, string $outputPath): void
    {
        $handle = fopen($inputPath, 'rb');
        $result = [];

        $lenghtBaseUrl = strlen(self::BASE_URL);

        $pathEnd   = - (self::DATE_TOTAL_LENGTH + 2);
        $dateStart = - (self::DATE_TOTAL_LENGTH + 1);
        $dateLen   = self::DATE_LENGTH;

        while ($line = fgets($handle)) {

            $path = substr($line, $lenghtBaseUrl, $pathEnd);
            $dateint =  (int) str_replace('-', '', substr($line, $dateStart, $dateLen));

            $ref = &$result[$path];
            if (isset($ref[$dateint])) {
                $ref[$dateint]++;
            } else {
                $ref[$dateint] = 1;
            }
        }

        fclose($handle);

        foreach ($result as &$dates) {
            ksort($dates);
        }


        $result = array_map(function ($dates) {
            $newDates = [];
            foreach ($dates as $dateint => $count) {
                $dateStr = substr((string) $dateint, 0, 4) . '-' . substr((string) $dateint, 4, 2) . '-' . substr((string) $dateint, 6, 2);
                $newDates[$dateStr] = $count;
            }
            return $newDates;
        }, $result);



        file_put_contents($outputPath, json_encode($result, JSON_PRETTY_PRINT));
    }
}
