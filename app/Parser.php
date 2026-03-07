<?php

namespace App;

use function fclose;
use function fgets;
use function file_put_contents;
use function fopen;
use function json_encode;
use function ksort;
use const JSON_PRETTY_PRINT;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $stream = fopen($inputPath, 'r');

        while ($line = fgets($stream)) {
            $commaPos = strpos($line, ',', 19);
            $url = substr($line, 19, $commaPos - 19);
            $date = substr($line, $commaPos + 1, 10);

            $result[$url][$date] = ($result[$url][$date] ?? 0) + 1;
        }
        fclose($stream);

        foreach ($result as &$dates) {
            ksort($dates);
        }

        $json = json_encode($result, flags: JSON_PRETTY_PRINT);

        file_put_contents($outputPath, $json);
    }
}