<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $handle = fopen($inputPath, 'r');

        $visits = [];

        while (! feof($handle)) {
            $line = fgets($handle);
            if (strpos($line, ',') === false) {
                continue;
            }

            $comma_pos = strpos($line, ',');
            $url = substr($line, 0, $comma_pos);
            $timestamp = substr($line, $comma_pos + 1);

            $t_pos = strpos($timestamp, 'T');
            $date = substr($timestamp, 0, $t_pos);

            if (isset($visits[$url])) {
                if (isset($visits[$url][$date])) {
                    $visits[$url][$date]++;
                } else {
                    $visits[$url][$date] = 1;
                }
            } else {
                $visits[$url] = [
                    $date => 1,
                ];
            }
        }

        fclose($handle);

        $write_handle = fopen($outputPath, 'w');

        $segment = '{';

        $first_line = true;

        foreach ($visits as $url => $dates) {
            ksort($dates);

            [, $formatted_url] = explode('.io\/', str_replace('/', '\/', $url), 2);
            if (! $first_line) {
                $segment .= "\n    },\n    \"\/$formatted_url\": {";
            } else {
                $first_line = false;
                $segment .= "\n    \"\/$formatted_url\": {";
            }

            $first_entry = true;
            foreach ($dates as $date => $count) {
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

