<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {       
        $f = fopen($inputPath, 'rnb');
        $v = fgets($f);
        $s = strpos($v, '/', 8);
        $h = [];
        do {
            $h[substr($v, $s, -27)][] = substr($v, -26, 10);
        } while ($v = stream_get_line($f, 8192, PHP_EOL));

        foreach ($h as &$v) {
            $v = array_count_values($v);
            ksort($v);
        };

        file_put_contents($outputPath, json_encode($h, JSON_PRETTY_PRINT));
    }
}