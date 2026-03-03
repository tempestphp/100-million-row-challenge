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
    private const int PATH_OFFSET = 25;
    private const int MAX_OFFSET = self::BUFFER_SIZE - self::MAX_LINE_LENGTH + self::PATH_OFFSET;

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
            $path = \substr($line, self::PATH_OFFSET, -27);
            $pathToHash[$path] ??= $nextHash++ << self::DATE_BITS;
            $resultCounts[$pathToHash[$path] | $dateToHash[\substr($line, -23, 7)]]++;
        }

        \stream_set_read_buffer($inputStream, 0);

        $buffer = \fread($inputStream, self::BUFFER_SIZE);
        while (\strlen($buffer) >= self::BUFFER_SIZE) {
            $pathOffset = self::PATH_OFFSET;
            while ($pathOffset <= self::MAX_OFFSET) {
                $commaOffset = \strpos($buffer, ',', $pathOffset);
                $resultCounts[
                    $pathToHash[\substr($buffer, $pathOffset, $commaOffset - $pathOffset)] |
                    $dateToHash[\substr($buffer, $commaOffset + 4, 7)]
                ]++;
                $pathOffset = $commaOffset + 52;
            }

            $remainingOffset = $pathOffset - self::PATH_OFFSET;
            $buffer = \substr($buffer, $remainingOffset) . \fread($inputStream, $remainingOffset);
        }

        $pathOffset = self::PATH_OFFSET;
        $maxOffset = \strlen($buffer);
        while ($pathOffset < $maxOffset) {
            $commaOffset = \strpos($buffer, ',', $pathOffset);
            $resultCounts[
                $pathToHash[\substr($buffer, $pathOffset, $commaOffset - $pathOffset)] |
                $dateToHash[\substr($buffer, $commaOffset + 4, 7)]
            ]++;
            $pathOffset = $commaOffset + 52;
        }

        \fclose($inputStream);

        $outputData = [];
        foreach ($pathToHash as $path => $pathHash) {
            $end = $pathHash + self::DATE_COUNT;
            $path = "/blog/{$path}";
            for ($i = $pathHash; $i < $end; $i++) {
                if ($resultCounts[$i] > 0) {
                    $outputData[$path][$hashToDate[$i & self::DATE_MASK]] = $resultCounts[$i];
                }
            }
        }
        \file_put_contents($outputPath, \json_encode($outputData, \JSON_PRETTY_PRINT));
    }
}