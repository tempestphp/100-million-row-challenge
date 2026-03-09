<?php

namespace App;

use Exception;

final class Parser
{
    // private const PREFIX = 'https://stitcher.io';
    // private const PREFIX_LEN = 19;
    // private const TIMESTAMP_LEN = 10;

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

        // $first_N_elements = array_slice($outArray, 0, 3, true);

        // print_r($outArray);

        // echo "\n";

        // $chunks = array_chunk($outArray, 10, true);

        // file_put_contents($outputPath, "{\n", FILE_APPEND | LOCK_EX);
        // foreach ($chunks as $chunk) {
        //     file_put_contents(
        //         $outputPath,
        //         trim(json_encode($chunk, JSON_PRETTY_PRINT), '{}'),
        //         FILE_APPEND | LOCK_EX
        //     );
        // }
        // file_put_contents($outputPath, "\n}", FILE_APPEND | LOCK_EX);

        // The FILE_APPEND flag preserves existing content and adds new data to the end
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
