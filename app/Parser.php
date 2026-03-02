<?php

namespace App;

final class Parser
{
    private const int URL_COUNT = 268;
    private const int DATE_BITS = 11;
    private const int DATE_COUNT = 1885;
    private const int HASH_SIZE = (2 ** self::DATE_BITS) * self::URL_COUNT;
    private const int DATE_MASK = 2 ** self::DATE_BITS - 1;

    public function parse(string $inputPath, string $outputPath): void
    {
        $date = new \DateTime('2021-01-01');
        $day = new \DateInterval('P1D');
        $dateMap = [];
        for ($i = 0; $i < self::DATE_COUNT; $i++) {
            $dateMap[$date->format('Y-m-d')] = $i;
            $date->add($day);
        }
        $dateLookup = \array_flip($dateMap);

        $outputData = \array_fill(0, self::HASH_SIZE, 0);

        $inputStream = \fopen($inputPath, 'r');
        $urlCount = 0;
        $urlMap = [];

        while ($urlCount < self::URL_COUNT && $line = \fgets($inputStream)) {
            $path = \substr($line, 19, -27);
            $urlMap[$path] ??= $urlCount++ << self::DATE_BITS;
            $date = \substr($line, -26, 10);
            $hash = $urlMap[$path] | $dateMap[$date];
            $outputData[$hash]++;
        }

        while ($line = \fgets($inputStream)) {
            $path = \substr($line, 19, -27);
            $date = \substr($line, -26, 10);
            $hash = $urlMap[$path] | $dateMap[$date];
            $outputData[$hash]++;
        }

        \fclose($inputStream);

        $finalData = [];
        foreach ($urlMap as $url => $urlHash) {
            $end = $urlHash + self::DATE_COUNT;
            for ($i = $urlHash; $i < $end; $i++) {
                if ($outputData[$i] > 0) {
                    $finalData[$url][$dateLookup[$i & self::DATE_MASK]] = $outputData[$i];
                }
            }
        }
        \file_put_contents($outputPath, \json_encode($finalData, \JSON_PRETTY_PRINT));
    }
}