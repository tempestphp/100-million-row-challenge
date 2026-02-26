<?php

namespace App;

use DateTime;

use function Tempest\Clock\now;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $reader = fopen($inputPath, 'r');
        $visits = [];
        $json = [];

        //Finding these values might be as easy as checking the first result if the url's are uniform.
        $urlEndLen = 19;
        $dateEndLen = -16;

        while(($line = fgets($reader)) !== false) {
            //shave off the first 19 characters and the last 17 characters. If the url isn't exactly the same across all records you're a bit more stuck.
            $key = substr($line, $urlEndLen, $dateEndLen);
            //I think the above is optimal, I'm clocking 7s for 100 million records.
            //any time save will come from building and sorting the json.

            //convert key to numeric representation and store the numeric to the key.
            // $intVal = unpack('H*', $key)[1];
            // $dictionary[$intVal] = $key;
            //I KNOW that integer array checks are faster, but converting to an integer takes more time.

            $visits[$key] = ($visits[$key] ?? 0) + 1; //This takes 32s.
            //$visits[$key] = 1; //This takes 32s.
            // if(!array_key_exists($key, $visits)) { //this takes 34s
            //     $visits[$key] = 1;
            // }
            //$visits[] = $key; //This takes 16s. So overwriting the key is doubling our time. But I'm losing more time by checking it.

        }
        //I figured out that if you can minimize the number of times you access and set a multidimensional array, you can save time.
        //So instead of doing $visits[$path][$date]++ for every line, we just do $visits[$key]++ and then build the json in one go.
        //for some reason this is disgustingly fast.
        foreach($visits as $key => $count) {
            $break = strpos($key, ',');
            $path = substr($key, 0, $break);
            $date = substr($key, $break + 1);
            //dd($path, $date, $count, $key, $break);
            $json[$path][$date] = $count;
        }
        foreach($json as $path => &$dates) {
            ksort($dates);
        }

        //There really isn't a lot *I* could do to optimize json_encode. We aren't even getting close to memory limits either.
        $output = json_encode($json, JSON_PRETTY_PRINT);

        file_put_contents($outputPath, $output);

        //echo memory_get_peak_usage() / pow(1024, 3) . "GB\n";

        fclose($reader);
    }
}