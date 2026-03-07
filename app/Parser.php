<?php

namespace App;

final class Parser
{

    private array $map = [];
    public function parse(string $inputPath, string $outputPath): void
    {
        $inputFile = fopen($inputPath, 'r');
        while (($line = fgets($inputFile, 128)) !== false) {
            $commaPos = strpos($line, ',');
            $path = substr($line, 19, $commaPos - 19);
            $date = substr($line, $commaPos + 1, 10);

            $date = (int) str_replace('-', '', $date);
            $this->map[$path][$date] = ($this->map[$path][$date] ?? 0) + 1;
        }
        fclose($inputFile);

        foreach ($this->map as &$line) {
            ksort($line);

            $modified = [];
            foreach ($line as $key => $value) {
                $modified[
                    substr($key, 0, 4) . '-' .
                    substr($key, 4, 2) . '-' .
                    substr($key, 6, 2)
                ] = $value;
            }
            $line = $modified;
        }

        file_put_contents($outputPath, json_encode($this->map, JSON_PRETTY_PRINT));
    }
}
