<?php

namespace App;

final class Parser
{
    private const int URL_COUNT = 268;
    private const int DATE_BITS = 11;
    private const int DATE_COUNT = 1885;
    private const int ARRAY_SIZE = (2 ** self::DATE_BITS) * self::URL_COUNT;
    private const int DATE_MASK = 2 ** self::DATE_BITS - 1;

    private const int MAX_LINE_LENGTH = 101;
    private const int BUFFER_SIZE = 1024 * 128;

    public function parse(string $inputPath, string $outputPath): void
    {
        $date = new \DateTime('2021-01-01');
        $day = new \DateInterval('P1D');
        $dateToHash = [];
        $hashToDate = [];
        for ($i = 0; $i < self::DATE_COUNT; $i++) {
            $dateStr = $date->format('Y-m-d');
            $dateToHash[\substr($dateStr, 3)] = $i;
            $date->add($day);
            $hashToDate[] = $dateStr;
        }

        $inputStream = \fopen($inputPath, 'r');
        $nextHash = 0;
        $pathToHash = [];
        $resultCounts = \array_fill(0, self::ARRAY_SIZE, 0);

        while ($nextHash < self::URL_COUNT && $line = \fgets($inputStream)) {
            $path = \substr($line, 25, -27);
            $pathToHash[$path] ??= $nextHash++ << self::DATE_BITS;
            $resultCounts[$pathToHash[$path] | $dateToHash[\substr($line, -23, 7)]]++;
        }

        \stream_set_read_buffer($inputStream, 0);

        $buffer = '';
        $offset = 0;
        while (true) {
            $buffer .= \fread($inputStream, self::BUFFER_SIZE - \strlen($buffer));
            if (\strlen($buffer) < self::BUFFER_SIZE) {
                break;
            }

            $maxOffset = \strlen($buffer) - self::MAX_LINE_LENGTH;
            while ($offset <= $maxOffset) {
                $comma = \strpos($buffer, ',', $offset);
                $resultCounts[
                    $pathToHash[\substr($buffer, $offset + 25, $comma - $offset - 25)] |
                    $dateToHash[\substr($buffer, $comma + 4, 7)]
                ]++;
                $offset = $comma + 27;
            }

            $buffer = \substr($buffer, $offset);
            $offset = 0;
        }

        while ($offset < \strlen($buffer)) {
            $comma = \strpos($buffer, ',', $offset);
            $resultCounts[
                $pathToHash[\substr($buffer, $offset + 25, $comma - $offset - 25)] |
                $dateToHash[\substr($buffer, $comma + 4, 7)]
            ]++;
            $offset = $comma + 27;
        }

        \fclose($inputStream);

        $outputData = [];
        foreach ($pathToHash as $url => $urlHash) {
            $end = $urlHash + self::DATE_COUNT;
            $url = "/blog/{$url}";
            for ($i = $urlHash; $i < $end; $i++) {
                if ($resultCounts[$i] > 0) {
                    $outputData[$url][$hashToDate[$i & self::DATE_MASK]] = $resultCounts[$i];
                }
            }
        }
        \file_put_contents($outputPath, \json_encode($outputData, \JSON_PRETTY_PRINT));
    }
}