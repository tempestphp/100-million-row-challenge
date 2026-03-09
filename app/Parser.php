<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        // throw new Exception('TODO');

        $inputPath = __DIR__.'/../data/test-data.csv';
        $outputPath = __DIR__.'/../data/test-data.json';

        // $file = new \SplFileObject($inputPath);
        // $file->setFlags(\SplFileObject::READ_CSV);
        // $file->setCsvControl(',', '"', '\\');

        $handle = fopen($inputPath, 'r');

        $outArray = [];

        while (($line = fgets($handle)) !== false) {
            $commaPos = strpos($line, ',');
            if (false === $commaPos) {
                break;
            }

            $url = substr($line, 19, $commaPos - 19);
            $timestamp = substr($line, $commaPos + 1, 10);

            // $row = explode(',', trim($row));
            // if (!isset($row[0], $row[1])) {
            //     break;
            // }

            // $url = json_encode((string) $row[0]);
            // $url = str_replace('/', '\/', str_replace('https://stitcher.io', '', $row[0]));
            // $url = str_replace('https://stitcher.io', '', $row[0]);
            // $url = substr($rawUrl, 19);
            // $timestamp = substr($row[1], 0, 10);

            // $timestampCount = $outArray[$url][$timestamp] ?? 0;
            // ++$timestampCount;

            // if (isset($outArray[$url][$timestamp])) {
            //     ++$outArray[$url][$timestamp];
            // } else {
            //     $outArray[$url][$timestamp] = 1;
            // }

            $outArray[$url][$timestamp] = ($outArray[$url][$timestamp] ?? 0) + 1;

            // if (false !== $tempStrSearch) {
            //     print_r($outArray[$url]);
            //     echo "\n";
            // }
        }

        // foreach ($outArray as &$innerArray) {
        //     ksort($innerArray);
        // }

        // $innerArray = [];
        foreach ($outArray as &$temp) {
            ksort($temp); // Sort by timestamp (faster than string dates)

            // Convert back to date strings

            // foreach ($temp as $timestamp => $value) {
            //     $innerArray[date('Y-m-d', $timestamp)] = $value;
            // }
        }

        fclose($handle);
        file_put_contents(
            $outputPath,
            json_encode($outArray, JSON_PRETTY_PRINT)
        );
    }

    /* public function readFileLineByLine(string $filePath): \Generator
    {
        $handle = fopen($filePath, 'r');
        if (!$handle) {
            throw new \Exception("Unable to open the file: {$filePath}");
        }

        try {
            while (($line = fgets($handle)) !== false) {
                yield $line; // Yields one line at a time
            }
        } finally {
            fclose($handle); // Ensures the file is closed even if an exception occurs
        }
    } */
}
