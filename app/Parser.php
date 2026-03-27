<?php

//declare(strict_types=1);

namespace App;

final class Parser
{
    private const string PATTERN = '`https://stitcher.io(.+)?,(.{10}).+`';

    public function parse(string $inputPath, string $outputPath): void
    {
        $csv = \fopen($inputPath, 'r');

        //$time = -\microtime(true);
        $data = [];
        while ($line = \fgets($csv)) {
            \preg_match(self::PATTERN, $line, $match);
            $data[$match[1]][$match[2]] = ($data[$match[1]][$match[2]] ?? 0) + 1;
        }
        //echo " > Parsing time: " . \round(\microtime(true) + $time, 3) . "s" . PHP_EOL;

        //$time = -\microtime(true);
        \array_walk($data, function (array &$items) { \ksort($items); });
        //echo " > Sorting time: " . \round(\microtime(true) + $time, 3) . "s" . PHP_EOL;

        //$time = -\microtime(true);
        $json = \json_encode($data, flags: \JSON_PRETTY_PRINT);
        //echo " > Encoding time: " . \round(\microtime(true) + $time, 3) . "s" . PHP_EOL;

        //$time = -\microtime(true);
        \file_put_contents($outputPath, $json);
        //echo " > Writing time: " . \round(\microtime(true) + $time, 3) . "s" . PHP_EOL;
    }
}
