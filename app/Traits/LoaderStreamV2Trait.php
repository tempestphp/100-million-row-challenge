<?php

namespace App\Traits;

trait LoaderStreamV2Trait {
    /**
     * This uses two stream_get_line calls to read in the line without having to do a followup strpos
     * to find the comma in the line
     */
    protected function load(): array
    {
        $inputPath = $this->inputPath;

        $inFile = fopen($inputPath, 'r');

        $urls = [];
        while($urlString = stream_get_line($inFile, self::STREAM_BUFFER_SIZE, ","))
        {
            $dateString = stream_get_line($inFile, self::STREAM_BUFFER_SIZE, "\n");

            $url = substr($urlString, self::DOMAIN_LENGTH);
            $date = substr($dateString, 0, self::DATE_LENGTH);

            if (isset($urls[$url])) {
                if (isset($urls[$url][$date]))
                {
                    $urls[$url][$date] += 1;
                } else {
                    $urls[$url][$date] = 1;
                }

            } else {
                $urls[$url] = [$date => 1];
            }

        }

        // Something is causing an extra empty row to get returned, cleaning this up here for now
        unset($urls[""]);

        fclose($inFile);

        return $urls;
    }
}