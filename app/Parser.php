<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $results = [];
        $resource = \fopen($inputPath, 'r');

        while (($line = \fgets($resource)) !== false) {
            $path = \substr($line, 19, -27);
            $date = \substr($line, -26, 10);
            $results[$path][$date] = ($results[$path][$date] ?? 0) + 1;
        }
        \fclose($resource);

        foreach ($results as $path => &$path_arr) {
            \ksort($path_arr);
        }

        $out = \fopen($outputPath, 'w');
        \fwrite($out, \json_encode($results, \JSON_PRETTY_PRINT));
        \fclose($out);
    }
}
