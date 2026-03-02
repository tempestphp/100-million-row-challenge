<?php

namespace App;

final class Parser
{
    private const int URL_COUNT = 268;
    private const int URL_BITS = 9;
    private const int YEAR_BITS = 3;
    private const int MONTH_BITS = 4;
    private const int DAY_BITS = 5;
    private const int DAY_MONTH_BITS = self::MONTH_BITS + self::DAY_BITS;
    private const int DATE_BITS = self::YEAR_BITS + self::DAY_MONTH_BITS;
    private const int HASH_BITS = self::URL_BITS + self::DATE_BITS;
    private const int HASH_SIZE = 2 ** self::HASH_BITS;

    public function parse(string $inputPath, string $outputPath): void
    {
        $outputData = \array_fill(0, self::HASH_SIZE, 0);

        $inputStream = \fopen($inputPath, 'r');
        $urlCount = 0;
        $urlMap = [];

        while ($urlCount < self::URL_COUNT && $line = \fgets($inputStream, 101)) {
            \preg_match('#^.{25}([^,]+).{4}(\d)-(\d{2})-(\d{2})#s', $line, $m);
            $urlMap[$m[1]] ??= $urlCount++ << self::DATE_BITS;
            $outputData[$urlMap[$m[1]] | ((int) $m[2] << self::DAY_MONTH_BITS) | ((int) $m[3] << self::DAY_BITS) | ((int) $m[4])]++;
        }

        while ($line = \fgets($inputStream, 101)) {
            \preg_match('#^.{25}([^,]+).{4}(\d)-(\d{2})-(\d{2})#s', $line, $m);
            $outputData[$urlMap[$m[1]] | ((int) $m[2] << self::DAY_MONTH_BITS) | ((int) $m[3] << self::DAY_BITS) | ((int) $m[4])]++;
        }

        \fclose($inputStream);

        $outputStream = \fopen($outputPath, 'w');
        \fwrite($outputStream, "{\n");

        foreach ($urlMap as $url => $urlHash) {
            \fwrite($outputStream, "    \"\/blog\/{$url}\": {\n");

            $end = $urlHash + (2 ** self::DATE_BITS);
            for ($i = $urlHash; $i < $end; $i++) {
                if ($outputData[$i] > 0) {
                    $y = ($i >> self::DAY_MONTH_BITS) & 0b111;
                    $m = ($i >> self::DAY_BITS) & 0b1111;
                    $d = $i & 0b11111;
                    \fwrite($outputStream, \sprintf('        "202%d-%02d-%02d": %d,', $y, $m, $d, $outputData[$i]) . "\n");
                }
            }

            \fseek($outputStream, -2, \SEEK_CUR);
            \fwrite($outputStream, "\n    },\n");
        }

        \fseek($outputStream, -2, \SEEK_CUR);
        \fwrite($outputStream, "\n}");
        \fclose($outputStream);
    }
}