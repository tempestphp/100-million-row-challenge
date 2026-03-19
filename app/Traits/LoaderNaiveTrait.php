<?php

namespace App\Traits;

trait LoaderNaiveTrait {
    protected function load(): array
    {
        $inputPath = $this->inputPath;

        $inFile = fopen($inputPath, 'r');

        $urls = [];

        while($row = fgetcsv($inFile, null, ',', '"', '\\'))
        {
            $url = substr($row[0], self::DOMAIN_LENGTH);
            $date = substr($row[1], 0, self::DATE_LENGTH);
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