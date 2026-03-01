<?php

namespace App;

final class Parser
{
    private const PREFIX = 'https://stitcher.io';
    private const PREFIX_LEN = 19; // strlen('https://stitcher.io')

    public function parse(string $inputPath, string $outputPath): void
    {
        $outArray = [];

        $handle = fopen($inputPath, 'r');
        if (!$handle) {
            throw new \Exception("Unable to open the file: {$inputPath}");
        }

        try {
            while (($line = fgets($handle)) !== false) {
                // Find the last comma to split (avoids explode allocating an array)
                $commaPos = strpos($line, ',');
                if (false === $commaPos) {
                    break;
                }

                $rawUrl = substr($line, 0, $commaPos);

                // Use substr instead of str_replace — fixed-length prefix is faster
                if (0 === strncmp($rawUrl, self::PREFIX, self::PREFIX_LEN)) {
                    $url = substr($rawUrl, self::PREFIX_LEN);
                } else {
                    $url = $rawUrl;
                }

                // Extract just the date portion (YYYY-MM-DD = 10 chars) after the comma
                $timestamp = substr($line, $commaPos + 1, 10);

                if (isset($outArray[$url][$timestamp])) {
                    ++$outArray[$url][$timestamp];
                } else {
                    $outArray[$url][$timestamp] = 1;
                }
            }
        } finally {
            fclose($handle);
        }

        // Encode directly without pretty print — no need to generate whitespace just to strip it
        file_put_contents($outputPath, json_encode($outArray));
    }
}
