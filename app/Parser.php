<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $result = [];
        $file = file($inputPath);
        foreach ($file as $line) {
            [$url, $date] = explode(',', $line);
            if (!isset($result[$url])) {
                $result[$url] = [];
            }
            if (!isset($result[$url][$date])) {
                $result[$url][$date] = 0;
            }
            $result[$url][$date]++;
        }

        file_put_contents($outputPath, json_encode($result));
    }
}
