<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        // throw new Exception('TODO');

        $inputPath = __DIR__.'/../data/test-data.csv';
        $outputPath = __DIR__.'/../data/test-data.json';

        $file = new \SplFileObject($inputPath);
        $file->setFlags(\SplFileObject::READ_CSV);
        $file->setCsvControl(',', '"', '\\');

        $outArray = [];

        foreach ($file as $key => $row) {
            if (!isset($row[0], $row[1])) {
                break;
            }

            $url = json_encode($row[0]);
            $timestamp = $row[1];

            $timestampCount = $outArray[$url][$timestamp] ?? 0;
            ++$timestampCount;

            $outArray[$url] = [
                $timestamp => $timestampCount,
            ];

            // if ($key >= 10) {
            //     break;
            // }
        }

        print_r($outArray);

        echo "\n";
    }
}
