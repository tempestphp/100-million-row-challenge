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
        $times = [];
        // for ($i = 2021; $i < 2027; $i++) {
        //     for ($j = 1; $j < 13; $j++) {
        //         for ($k = 0; $k < 32; $k++) {
        //             // is this reall y worth it?
        //             $l = fn ($m) => $m;
        //             $times["{$i}-{$l(sprintf("%02d-%02d", $j, $k))}"] = [];
        //         }
        //     }
        // }
        try {
            fseek($file, 19);
            while ($row = fgets($file)) {
                // assuming timestamp is always 26 chars long
                $dest = substr($row, 0, -27);
                $arr[$dest] ??= [];
                $arr[$dest][$k = substr($row, -26, 10)] ??= 0;
                // $times[$k = substr($row, -26, 10)][$dest] ??= 0;
                // $times[$k][$dest]++;
                $arr[$dest][$k]++;
                fseek($file, 19, SEEK_CUR);
            }
        } catch (Throwable $e) {
            //
            echo "Oops";
        }
        fclose($file);
        // ksort($times, SORT_STRING);
        // foreach ($times as $time => $dests) {
        //     foreach ($dests as $dest => $count) {
        //         if (!$count) continue;
        //         $arr[$dest][$time] = $count;
        //     }
        // }
        foreach ($arr as &$xf2) {
            ksort($xf2);
        }
        $f = fopen($outputPath, "w");
        $str1 =  "        \"";
        $str2 = "\": ";
        $str3 = ",\n";
        $json =  "{\n";
        foreach ($arr as $k => $dest) {
            $x = str_replace("/", "\\/", $k);
            $json .= "    \"$x\": {\n";
            $parts = [];
            foreach ($arr[$k] as $l => $v) {
                $parts[] = $str1 . $l. $str2 . $v;
            }
            $json .= implode($str3, $parts);
            $json .=  "\n    },\n";
            fwrite($f, $json);
            $json = "";
        }
        fseek($f, -2, SEEK_CUR);
        $json .= "\n}";
        fwrite($f, $json);
        // fwrite($f, json_encode($arr, JSON_PRETTY_PRINT));
        fclose($f);
    }
}