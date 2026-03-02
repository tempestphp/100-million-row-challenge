<?php

namespace App;

use App\Commands\Visit;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $max_uri_length = Visit::all()
            |> (fn($vs) => array_map(fn($v) => strlen($v->uri) - 17, $vs))
            |> max(...);

        $uris = [];

        $in = fopen($inputPath, 'r');
        fseek($in, 19); // Ignore https://stitcher.io
        while ($uri = stream_get_line($in, $max_uri_length, ",")) {
            $date = stream_get_line($in, 10);
            fseek($in, 35, SEEK_CUR); // Skip time and https://stitcher.io
            $uris[$uri][] = $date; // It's cheaper not to check for and initialise the array
        }

        foreach ($uris as &$visits) {
            $visits = array_count_values($visits);
            ksort($visits, SORT_STRING);
        }

        file_put_contents($outputPath, json_encode($uris, JSON_PRETTY_PRINT, 3));
    }
}