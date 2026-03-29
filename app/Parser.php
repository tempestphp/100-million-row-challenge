<?php

namespace App;

use Exception;

final class Parser
{
    private const int CHUNK_SIZE = 1048576; // 1 MB, chosen experimentally
    private const int DOMAIN_LENGTH = 19; // each URL starts with https://stitcher.io
    private const int DATETIME_LENGTH = 25; // each date/time is like 2022-02-05T04:45:41+00:00
    private const int DATE_LENGTH = 10; // date part like 2022-02-05

    public function parse(string $inputPath, string $outputPath): void
    {
        try {
            $readHandle = \fopen($inputPath, 'rb');
            \stream_set_read_buffer($readHandle, 0);
            $writeHandle = \fopen($outputPath, 'wb');

            $chunkSize = (int) (\getenv('CHUNK_SIZE') ?: self::CHUNK_SIZE);
            $stats = [];
            $buffer = '';
            while (!\feof($readHandle)) {
                $buffer .= \fread($readHandle, $chunkSize);
                $lines = \explode("\n", $buffer);
                $buffer = \array_pop($lines); // last line can be partial

                foreach ($lines as &$line) {
                    $url = \substr($line, self::DOMAIN_LENGTH, -self::DATETIME_LENGTH - 1);
                    $date = \substr($line, -self::DATETIME_LENGTH, self::DATE_LENGTH);

                    $stats[$url][$date] ??= 0;
                    $stats[$url][$date]++;
                }
            }

            if ($buffer !== '') {
                $url = \substr($buffer, self::DOMAIN_LENGTH, -self::DATETIME_LENGTH - 1);
                $date = \substr($buffer, -self::DATETIME_LENGTH, self::DATE_LENGTH);

                $stats[$url][$date] ??= 0;
                $stats[$url][$date]++;
            }

            foreach ($stats as &$countPerDate) {
                \ksort($countPerDate, SORT_STRING);
            }

            \fwrite($writeHandle, \json_encode($stats, JSON_PRETTY_PRINT));
        } finally {
            if (!empty($readHandle)) {
                \fclose($readHandle);
            }
            if (!empty($writeHandle)) {
                \fclose($writeHandle);
            }
            echo "Mem: " . round(memory_get_peak_usage() / 1024 / 1024) . "\n";
        }
    }
}
