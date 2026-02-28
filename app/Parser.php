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

        $paths = [];

        while ($csvRow = fgetcsv($input, escape: '')) {
            $urlInput = $csvRow[0];
            $dateInput = $csvRow[1];

            $path = $paths[$urlInput] ??= $this->parsePathFromUrl($urlInput);

            $output[$path] ??= [];

            $formattedDate = $this->parseDate($dateInput);
            $output[$path][$formattedDate] ??= 0;
            $output[$path][$formattedDate]++;
        }

        foreach ($output as &$data) {
            ksort($data);
        }

        $json = json_encode($output, flags: JSON_PRETTY_PRINT);
        file_put_contents($outputPath, $json);
    }

    private function parsePathFromUrl(string $urlInput): mixed
    {
        return parse_url($urlInput, PHP_URL_PATH);
    }

    private function parseDate(string $dateInput): string
    {
        return substr($dateInput, 0, 10);
    }
}
