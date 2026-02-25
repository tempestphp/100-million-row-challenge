<?php

namespace App;

use Exception;

final class Parser
{
    const PATH_PREFIX_LENGTH = 19;
    const DATE_LENGTH = 10;
    const TIME_LENGTH = 15;
    const DATETIME_LENGTH = self::DATE_LENGTH + self::TIME_LENGTH;


    public function parse(string $inputPath, string $outputPath): void
    {
        $data = [];
        foreach($this->parseInput($inputPath) as $url => $date) {
            if (isset($data[$url][$date])) {
                $data[$url][$date]++;
            } else {
                $data[$url][$date] = 1;
            }
        }
        array_walk(
            $data,
            static fn(&$dates) => ksort($dates)
        );
        file_put_contents($outputPath, json_encode($data, flags:JSON_PRETTY_PRINT));
    }

    private function parseInput(string $inputPath) {
        $r = fopen($inputPath, 'r');
        $data = [];
        while (($line = fgets($r)) !== false) {
            yield substr($line, self::PATH_PREFIX_LENGTH, -self::DATETIME_LENGTH - 2) => substr($line, -self::DATETIME_LENGTH - 1, self::DATE_LENGTH);
        }
    }
}
