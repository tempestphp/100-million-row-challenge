<?php

namespace App;

use Exception;
use Generator;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $outputData = [];

        foreach ($this->readLineByLine($inputPath) as $line) {
            $parts = explode(',', $line);
            $path = substr($parts[0], 19);
            $date = substr($parts[1], 0, 10);

            if (!isset($outputData[$path][$date])) {
                $outputData[$path][$date] = 0;
            }

            $outputData[$path][$date]++;
        }

        $json = json_encode((object) $outputData);

        file_put_contents($outputPath, $json);
    }

    private function readLineByLine(string $inputPath): Generator
    {
        $file = fopen($inputPath, 'r');

        while ($line = fgets($file)) {
            yield $line;
        }

        fclose($file);
    }
}