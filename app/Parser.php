<?php

namespace App;


final class Parser
{
    const string BASE_URL = 'https://stitcher.io';

    public function parse(string $inputPath, string $outputPath): void
    {

        $inputFile = fopen($inputPath, "r");

        $urlPathOffset = strlen(self::BASE_URL);
        $timeOffset = 16;

        gc_disable();

        $urls = $pathKeys = $dateKeys = [];
        $currentPathKey = 0;

        /**
         * Preload an array with dates from 2021 to now. It will be used for faster indexing of the $urls array by date key
         */

        $minDate = mktime(0, 0, 0, 1, 1, 2021);
        $maxDate = time();

        for ($i = $minDate; $i <= $maxDate; $i += 86400) {
            $dateKeys[] = date('Y-m-d', $i);
        }

        $dateKeys = array_flip($dateKeys);

        fseek($inputFile, $urlPathOffset);

        while (true) {

            /**
             * Somehow this is faster than getting the whole line and splitting with substr() or explode())
             */
            $path = stream_get_line($inputFile, 128, ",");

            if ($path === false) break;

            $date = stream_get_line($inputFile, 10);

            $pathKey = &$pathKeys[$path];
            $dateKey = $dateKeys[$date];

            if (!isset($pathKey)) $pathKey = $currentPathKey++;

            if (!isset($urls[$pathKey][$dateKey])) {
                $urls[$pathKey][$dateKey] = 1;
            } else {
                $urls[$pathKey][$dateKey]++;
            }

            fseek($inputFile, $timeOffset + $urlPathOffset, SEEK_CUR);

        }


        $urls = array_combine(array_keys($pathKeys), array_values($urls));

        $dateKeys = array_flip($dateKeys);

        foreach ($urls as &$visits) {

            $visits = array_combine(
                array_map(fn(int $index): string => $dateKeys[$index], array_keys($visits)),
                array_values($visits)
            );

            ksort($visits, SORT_STRING);
        }


        gc_enable();

        fclose($inputFile);

        file_put_contents($outputPath, json_encode($urls, JSON_PRETTY_PRINT));

    }

}