<?php

namespace App\Traits;

trait LoaderStreamV1Trait {
    /**
     * This uses a stream_get_line call to read the whole line at once without full csv parsing
     */
    protected function load(): array
    {
        // Hoist class variables
        $inputPath = $this->inputPath;

        $inFile = fopen($inputPath, 'r');

        $urls = [];
        while($rowString = stream_get_line($inFile, self::STREAM_BUFFER_SIZE, "\n"))
        {
            $commaPos = strpos($rowString, ',');
            $url = substr($rowString, self::DOMAIN_LENGTH, $commaPos - self::DOMAIN_LENGTH);
            $date = substr($rowString, $commaPos+1, self::DATE_LENGTH);

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

        fclose($inFile);

        return $urls;
    }
}