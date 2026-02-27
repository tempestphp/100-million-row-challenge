<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();
        $handle = fopen($inputPath, 'r');
        stream_set_read_buffer($handle, 0);

        // Assuming that we'll only see 2048 unique dates
        $date_stride = 2048;
        $read_chunk_size = 262144;
        $next_url_id = 0;
        $url_map = [];
        $next_date_id = 0;
        $date_map = [];
        $date_counts = [];

        $carry = '';

        while (true) {
            $chunk = fread($handle, $read_chunk_size);

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

                $path = substr($buffer, $pos + 20, $line_len - 46);
                $date = substr($buffer, $pos + $line_len - 25, 10);

                $url_id = $url_map[$path] ?? -1;
                if ($url_id === -1) {
                    $url_id = $next_url_id++;
                    $url_map[$path] = $url_id;
                    array_push($date_counts, ...array_fill(0, $date_stride, 0));

                }

                $date_id = $date_map[$date] ??= $next_date_id++;

                $date_counts[($url_id * $date_stride) + $date_id]++;

                $pos = $new_line_pos + 1;
            }
        }

        fclose($handle);

        $url_list = array_flip($url_map);
        ksort($date_map);
        $date_list = array_flip($date_map);
        $sorted_date_ids = array_values($date_map);

        $output = '{';

        for ($url_id = 0; $url_id < $next_url_id; $url_id++) {
            $formatted_url = str_replace('/', '\/', $url_list[$url_id]);

            if ($url_id > 0) {
                $output .= "\n    },\n    \"\/$formatted_url\": {";
            } else {
                $output .= "\n    \"\/$formatted_url\": {";
            }

            $base = $url_id * $date_stride;
            $first_entry = true;
            foreach ($sorted_date_ids as $date_id) {
                $count = $date_counts[$base + $date_id];
                if ($count === 0) {
                    continue;
                }
                if ($first_entry) {
                    $output .= "\n        \"{$date_list[$date_id]}\": $count";
                    $first_entry = false;
                } else {
                    $output .= ",\n        \"{$date_list[$date_id]}\": $count";
                }
            }
        }

        $output .= "\n    }\n}";

        file_put_contents($outputPath, $output);
    }
}
