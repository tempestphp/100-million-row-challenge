<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();
        $csv = fopen($inputPath, 'rb');

        $all = [];

        while ($line = fgets($csv)) {
            $separated = strcspn($line, ',');
            $path = substr($line, offset: 25, length: $separated - 25);
            $date = substr($line, offset: $separated + 1, length: 10);
            $all[$path][$date] ??= 0;
            $all[$path][$date]++;

        }
        fclose($csv);


        $jsonString = "{\n";
        $lastPath = array_key_last($all);
        $lastDates = $all[$lastPath];
        unset($all[$lastPath]);
        foreach ($all as $path => $dates) {
            $jsonString .= '    "\/blog\/'.$path.'"'.": {";
            ksort($dates);
            $firstDate =  key($dates);
            $jsonString .= "\n        ".'"'.$firstDate.'": '.$dates[$firstDate];
            unset($dates[$firstDate]);
            foreach ($dates as $date => $count) {
                $jsonString .= ",\n        ".'"'.$date.'": '.$count;
            }

            $jsonString .= "\n    },\n";
        }
        $jsonString .= '    "\/blog\/'.$lastPath.'": {';
        ksort($lastDates);
        $firstDate =  key($lastDates);
        $jsonString .= "\n        ".'"'.$firstDate.'": '.$lastDates[$firstDate];
        unset($lastDates[$firstDate]);
        foreach ($lastDates as $date => $count) {
            $jsonString .= ",\n        ".'"'.$date.'": '.$count;
        }
        $jsonString .= "\n    }\n}";

        file_put_contents($outputPath, $jsonString);
    }
}