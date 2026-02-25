<?php

namespace App;

use SplFileObject;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $result = [];
        $endresult = [];

        $file = new SplFileObject($inputPath);
        while (!$file->eof()) {
            $line = $file->current();

            if ($pos = strpos($line, ',')) {
                $url = substr($line, 0, $pos);
                $timestamp = substr($line, $pos + 1, 10);

                $result[$url] ??= [];
                $result[$url][$timestamp] = ($result[$url][$timestamp] ?? 0) + 1;
            }

            $file->next();
        }

        foreach ($result as $index => &$row) {
            ksort($row);
            $endresult[str_replace('https://stitcher.io','',$index)] = $row;
        }

        file_put_contents($outputPath, json_encode($endresult, JSON_PRETTY_PRINT));
    }
}