<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $result = [];

        $file = \fopen($inputPath, 'r');
        while ($line = \fgets($file)) {
            // Practically unsafe, but \fgetcsv is slow
            $result[\substr($line, 19, \strpos($line, ',') - 19)][] = \substr($line, \strpos($line, ',') + 1, 10);
        }

        foreach ($result as &$visits) {
            $visits = \array_count_values($visits);
            \ksort($visits, \SORT_STRING);
        }

        \file_put_contents($outputPath, \json_encode($result, \JSON_PRETTY_PRINT));
    }
}
