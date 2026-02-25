<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $inFd = fopen($inputPath, 'r');
        $arr = [];
        $outFd = fopen($outputPath, 'w');
        while(!feof($inFd)) {
            $line = fgets($inFd);
            $line = substr($line, 19, -1);
            if ($line === '') {
                break;
            }
            [$url, $time] = explode(',', $line, 2);
            $date = substr($time, 0, 10);
            [$year, $month, $day] = explode('-', $date);
            $arr[$url] ??= [];
            $arr[$url][$year] ??= [];
            $arr[$url][$year][$month] ??= [];
            $arr[$url][$year][$month][$day] ??= 0;
            $arr[$url][$year][$month][$day]++;
        }

        $out = [];
        foreach ($arr as $url => $data) {
            $years = array_keys($data);
            sort($years);
            foreach ($years as $year) {
                $months = array_keys($data[$year]);
                sort($months);
                foreach ($months as $month) {
                    $days = array_keys($data[$year][$month]);
                    sort($days);
                    foreach ($days as $day) {
                        $out[$url]["{$year}-{$month}-{$day}"] = $data[$year][$month][$day];
                    }
                }
            }
        }

        echo memory_get_usage(true) / 1024 / 1024;

        fwrite($outFd, json_encode($out, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT));
    }
}