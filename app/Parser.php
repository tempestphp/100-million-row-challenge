<?php
namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $fp = fopen($inputPath, 'r');
        while (! feof($fp)) {
            $line = fgets($fp);
            $key  = substr($line, 19, strpos($line, ',') - 19);
            if ($key !== '') {
                $results[$key][] = substr($line, strpos($line, ',') + 1, 10);
            }
        }
        foreach ($results as $key => $values) {
            sort($values);
            $results[$key] = array_count_values($values);
        }
        file_put_contents($outputPath, json_encode($results, JSON_PRETTY_PRINT));
    }
}
