<?php

namespace App;

final class Parser
{
    private const string HOST_PREFIX = 'https://stitcher.io';
    private const int HOST_PREFIX_LENGTH = 19;
    private const int MAX_LINE_LENGTH = 1024 * 1024;
    private const int READ_BUFFER_SIZE = 1024 * 1024;
    private const string LINE_DELIMITER = "\n";
    private const int DATE_LENGTH = 10;

    public function parse(string $inputPath, string $outputPath): void
    {
        $file = fopen($inputPath, 'r');

        if ($file === false) {
            return;
        }

        stream_set_read_buffer($file, self::READ_BUFFER_SIZE);

        $visitsByPath = [];

        try {
            while (($line = stream_get_line($file, self::MAX_LINE_LENGTH, self::LINE_DELIMITER)) !== false) {
                $commaPosition = strpos($line, ',');

                if ($commaPosition === false) {
                    continue;
                }

                if (
                    $commaPosition <= self::HOST_PREFIX_LENGTH
                    || strncmp($line, self::HOST_PREFIX, self::HOST_PREFIX_LENGTH) !== 0
                ) {
                    continue;
                }

                $pathLength = strcspn($line, '?#,', self::HOST_PREFIX_LENGTH);
                $path = substr($line, self::HOST_PREFIX_LENGTH, $pathLength);

                if (! isset($line[$commaPosition + self::DATE_LENGTH])) {
                    continue;
                }

                $date = substr($line, $commaPosition + 1, self::DATE_LENGTH);

                if (! isset($visitsByPath[$path])) {
                    $visitsByPath[$path] = [];
                }

                $visitsByDate = &$visitsByPath[$path];

                if (isset($visitsByDate[$date])) {
                    $visitsByDate[$date]++;
                } else {
                    $visitsByDate[$date] = 1;
                }
            }
        } finally {
            fclose($file);
        }

        unset($visitsByDate);

        foreach ($visitsByPath as &$visitsByDate) {
            ksort($visitsByDate);
        }

        unset($visitsByDate);

        $json = json_encode($visitsByPath, JSON_PRETTY_PRINT);

        if (! is_string($json)) {
            return;
        }

        file_put_contents($outputPath, $json);
    }
}