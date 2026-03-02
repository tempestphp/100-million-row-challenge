<?php

namespace App;

final class Parser
{
    private const int URL_COUNT = 268;
    private const int DATE_BITS = 11;
    private const int DATE_COUNT = 1885;
    private const int ARRAY_SIZE = (2 ** self::DATE_BITS) * self::URL_COUNT;
    private const int DATE_MASK = 2 ** self::DATE_BITS - 1;

    public function parse(string $inputPath, string $outputPath): void
    {
        $date = new \DateTime('2021-01-01');
        $day = new \DateInterval('P1D');
        $dateMap = [];
        $dateLookup = [];
        for ($i = 0; $i < self::DATE_COUNT; $i++) {
            $dateStr = $date->format('Y-m-d');
            $dateMap[\substr($dateStr, 3)] = $i;
            $date->add($day);
            $dateLookup[] = $dateStr;
        }

        $inputStream = \fopen($inputPath, 'r');
        $urlCount = 0;
        $urlMap = [];
        $outputData = \array_fill(0, self::ARRAY_SIZE, 0);

        while ($urlCount < self::URL_COUNT && $line = \fgets($inputStream)) {
            $path = \substr($line, 25, -27);
            $urlMap[$path] ??= $urlCount++ << self::DATE_BITS;
            $outputData[$urlMap[$path] | $dateMap[\substr($line, -23, 7)]]++;
        }

        while ($line = \fgets($inputStream)) {
            $outputData[$urlMap[\substr($line, 25, -27)] | $dateMap[\substr($line, -23, 7)]]++;
        }

        \fclose($inputStream);

        $finalData = [];
        foreach ($urlMap as $url => $urlHash) {
            $end = $urlHash + self::DATE_COUNT;
            $url = "/blog/{$url}";
            for ($i = $urlHash; $i < $end; $i++) {
                if ($outputData[$i] > 0) {
                    $finalData[$url][$dateLookup[$i & self::DATE_MASK]] = $outputData[$i];
                }
            }
        }
        \file_put_contents($outputPath, \json_encode($finalData, \JSON_PRETTY_PRINT));
    }
}