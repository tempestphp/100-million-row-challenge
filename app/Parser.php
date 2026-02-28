<?php

namespace App;

use DateTimeImmutable;
use Exception;

final class Parser
{
    /**
     * @throws \DateMalformedStringException
     */
    public function parse(string $inputPath, string $outputPath): void
    {
        $output = [];

        $input = fopen($inputPath, 'r');
        stream_set_read_buffer($input, 4 * 2^10 * 2^10);

        while ($line = fgets($input)) {
            $commaPos = strpos($line, ',');

            $path = substr($line, 19, $commaPos - 19);

            $date = substr($line, $commaPos + 1, 10);

            $output[$path] ??= [];

            $output[$path][$date] ??= 0;
            $output[$path][$date]++;
        }

        foreach ($output as &$data) {
            ksort($data);
        }

        $json = json_encode($output, flags: JSON_PRETTY_PRINT);
        file_put_contents($outputPath, $json);
    }
}
