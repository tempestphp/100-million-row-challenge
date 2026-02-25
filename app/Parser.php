<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $handle = fopen($inputPath, 'r');

        $visits = [];
        $next_url_id = 0;
        $url_map = [];
        $next_date_id = 0;
        $date_map = [];

        while (! feof($handle)) {
            $line = fgets($handle);
            if (strpos($line, ',') === false) {
                continue;
            }

            $url = substr($line, 0, -27);
            $date = substr($line, -26, 10);

            $url_id = $url_map[$url] ??= $next_url_id++;
            $date_id = $date_map[$date] ??= $next_date_id++;

            $visits[$url_id][$date_id] = ($visits[$url_id][$date_id] ?? 0) + 1;
        }

        fclose($handle);

        $url_list = array_flip($url_map);
        $date_list = array_flip($date_map);

        $write_handle = fopen($outputPath, 'w');

        $segment = '{';

        $first_url = true;
        foreach ($visits as $url_id => $dates) {
            uksort($dates, static fn ($a, $b) => $date_list[$a] <=> $date_list[$b]);

            $url = $url_list[$url_id];
            [, $formatted_url] = explode('.io\/', str_replace('/', '\/', $url), 2);
            if (! $first_url) {
                $segment .= "\n    },\n    \"\/$formatted_url\": {";
            } else {
                $first_url = false;
                $segment .= "\n    \"\/$formatted_url\": {";
            }

            $first_entry = true;
            foreach ($dates as $date_id => $count) {
                $date = $date_list[$date_id];
                if (! $first_entry) {
                    $segment .= ",\n        \"$date\": $count";
                } else {
                    $first_entry = false;
                    $segment .= "\n        \"$date\": $count";
                }
            }

            if (strlen($segment) >= 4096) {
                fwrite($write_handle, $segment);
                $segment = '';
            }
        }

        $segment .= "\n    }\n}";
        fwrite($write_handle, $segment);
    }
}
