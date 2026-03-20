<?php

namespace App;

use Throwable;

use function Tempest\Support\Str\slice;

final class Parser
{

    public function parse(string $inputPath, string $outputPath): void
    {
        $file = fopen($inputPath, "r");
        $arr = [];
        $row = fgets($file);
        $originLength =  strpos($row, '/', 8);
        fseek($file, 0);

        while ($row = fgets($file)) {
            [$url, $time] = explode(',', $row);
            $url = substr($url, $originLength);
            $time = substr($time, 0, 10);
            $arr[$url][$time] ??= 0;
            $arr[$url][$time]++;
        }
        fclose($file);
        foreach ($arr as &$xf2) {
            ksort($xf2, SORT_STRING);
        }
        $f = fopen($outputPath, "w");
        fwrite($f, json_encode($arr, JSON_PRETTY_PRINT));
        fclose($f);
    }
}