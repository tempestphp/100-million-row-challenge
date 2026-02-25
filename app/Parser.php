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

        $carry = '';

        while (true) {
            $chunk = fread($handle, 1048576);

            if ($chunk === '' || $chunk === false) {
                break;
            }

            $buffer = $carry.$chunk;
            $pos = 0;

            while (true) {
                $new_line_pos = strpos($buffer, "\n", $pos);
                if ($new_line_pos === false) {
                    $carry = substr($buffer, $pos);
                    break;
                }

                $line_len = $new_line_pos - $pos;

                $url = substr($buffer, $pos, $line_len - 26);
                $date = substr($buffer, $pos + $line_len - 25, 10);

                $url_id = $url_map[$url] ??= $next_url_id++;
                $date_id = $date_map[$date] ??= $next_date_id++;

                $visits[$url_id][$date_id] = ($visits[$url_id][$date_id] ?? 0) + 1;

                $pos = $new_line_pos + 1;
            }
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
