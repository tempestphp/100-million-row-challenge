<?php

namespace App;

use Throwable;

use function Tempest\Support\Str\slice;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $csvFile = $inputPath;
        $file = fopen($csvFile, "r");
        $arr = [];
        try {
            while ($row = fgetcsv($file, escape: "\\")) {
                [$dest, $time] = $row;
                $dest = slice($dest, 19);
                $arr[$dest] ??= [];
                $arr[$dest][$k = slice($time, 0, 10)] ??= 0;
                $arr[$dest][$k]++;
            }
        } catch (Throwable $e) {
            //
            echo "Oops";
        }
        fclose($file);
        foreach ($arr as &$i) {
            ksort($i);
        }
        $f = fopen($outputPath, "w");
        fwrite($f, "{\n");
        foreach ($arr as $k => $dest) {
            $x = str_replace("/", "\\/", $k);
            fwrite($f, "    \"$x\": {\n");
            foreach ($arr[$k] as $l => $v) {
                fwrite($f, "        \"$l\": $v,\n");
            }
            fseek($f, -2, SEEK_CUR);
            fwrite($f, "\n");
            fwrite($f, "    },\n");
        }
        fseek($f, -2, SEEK_CUR);
        fwrite($f, "\n");
        fwrite($f, "}");
        fclose($f);
    }
}