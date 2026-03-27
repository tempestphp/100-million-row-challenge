<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $url_map = [];
        $time_map = [];
        $url_counter = 0;
        $time_counter = 0;
        $counts = [];

        $handle = fopen($inputPath, 'r');

        while ($line = fgets($handle)) {
            $pos = strpos($line, ',');
            $url = substr($line, 0, $pos);
            $timestamp = substr($line, $pos + 1, 10);

            if (!isset($url_map[$url])) {
                $url_map[$url] = $url_counter++;
            }

            if (!isset($time_map[$timestamp])) {
                $time_map[$timestamp] = $time_counter++;
            }

            $uid = $url_map[$url];
            $tid = $time_map[$timestamp];

            if (!isset($counts[$uid])) {
                $counts[$uid] = [];
            }

            $counts[$uid][$tid] = ($counts[$uid][$tid] ?? 0) + 1;
        }

        fclose($handle);

        $map = [];

        $id_to_url = array_flip($url_map);
        $id_to_time = array_flip($time_map);

        foreach ($counts as $uid => $time_counts) {
            $url = str_replace('https://stitcher.io', '', $id_to_url[$uid]);

            foreach ($time_counts as $tid => $count) {
                $map[$url][$id_to_time[$tid]] = $count;
            }

            ksort($map[$url]);
        }

        file_put_contents($outputPath, json_encode($map, JSON_PRETTY_PRINT));
    }
}