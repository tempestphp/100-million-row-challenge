<?php

namespace App;

use Exception;

final class Parser
{
    // 4MB read/write buffers.
    const STREAM_BUFFER_SIZE = 4 << 20;

    // A line is ±75 characters, so 75 * 2^14 = 1.2 MiB, which is a good buffer size for this input,
    // so set a mask to quickly check when this many lines have been buffered.
    const LINE_BUFFER_MASK = (1 << 14) - 1;

    // Strip this prefix from every URI (length = 19).
    private const URI_PREFIX_LEN = 19;    

    public function parse(string $inputPath, string $outputPath): void
    {
        try {
            $input = \fopen($inputPath, 'rb');
            $output = \fopen($outputPath, 'wb');

            \stream_set_read_buffer($input,  self::STREAM_BUFFER_SIZE);
            \stream_set_write_buffer($output, self::STREAM_BUFFER_SIZE);

            $counters = [];

            while ($line = \fgets($input)) {
                $comma = \strpos($line, ',', self::URI_PREFIX_LEN);
                $uri  = \substr($line, self::URI_PREFIX_LEN, $comma - self::URI_PREFIX_LEN);
                $date = \substr($line, $comma + 1, 10);                

                if (!isset($counters[$uri])) {
                    $counters[$uri] = [];
                }

                $dates = &$counters[$uri];
                if (isset($dates[$date])) {
                    $dates[$date]++;
                } else {
                    $dates[$date] = 1;
                }
            }

            $buffer = "{";
            $lines = 0;
            $firstUri = true;
            // Output validation requires that the URIs are the order they were first encountered in the input.
            foreach ($counters as $uri => &$dates) {
                if (!$firstUri) {
                    $buffer .= ",";
                }
                $firstUri = false;

                // Output validation requires that the URI slashes are escaped.
                $buffer .= "\n    \"" . str_replace('/', '\\/', $uri) . "\": {";

                // Output validation requires that the dates are in ascending order.
                ksort($dates);

                $firstDate = true;
                foreach ($dates as $date => &$count) {
                    if (!$firstDate) {
                        $buffer .=  ",";
                    }
                    $firstDate = false;
                    $buffer .=  "\n        \"$date\": $count";
                    if ((++$lines & self::LINE_BUFFER_MASK) === 0) {
                        \fwrite($output, $buffer);
                        $buffer = '';
                    }
                }
                $buffer .= "\n    }";
            }
            $buffer .= "\n}";
            \fwrite($output, $buffer);
        } finally {
            @\fclose($input);
            @\fclose($output);
        }
    }
}
