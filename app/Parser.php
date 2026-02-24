<?php

namespace App;

use Exception;

final class Parser
{
    // 4MB read/write buffers.
    const STREAM_BUFFER_SIZE = 4 << 20;

    // 16MB chunk size when reading input.
    const READ_CHUNK_SIZE = 16 << 20;

    // A line is ±75 characters, so 75 * 2^14 = 1.2 MiB, which is a good buffer size for this input,
    // so set a mask to quickly check when this many lines have been buffered.
    const LINE_BUFFER_MASK = (1 << 14) - 1;

    // Strip this prefix from every URI (length = 19).
    const URI_PREFIX_LEN = 19;

    public function parse(string $inputPath, string $outputPath): void
    {
        try {
            $input = \fopen($inputPath, 'rb');
            $output = \fopen($outputPath, 'wb');

            \stream_set_read_buffer($input,  self::STREAM_BUFFER_SIZE);
            \stream_set_write_buffer($output, self::STREAM_BUFFER_SIZE);

            $counters = [];

            $chunk = \fread($input, self::READ_CHUNK_SIZE);
            $start = 0;
            do {
                // Try and find a line ending.
                $lineEnd = \strpos($chunk, "\n", $start);

                // When there is no line ending, but more input to read, grab another chunk and append
                // it to what is left of the current chunk.
                if ($lineEnd === false && !\feof($input)) {
                    // No newline found, and we are not at the end of the file, so carry this chunk over to the next read.
                    $chunk = \substr($chunk, $start) . \fread($input, self::READ_CHUNK_SIZE);
                    $start = 0;
                    continue;
                }

                // When at the end of the file, there may not be a line ending, so set the line end to the end of the chunk.
                if ($lineEnd === false) {
                    $lineEnd = \strlen($chunk);
                }

                $uriStart = $start + self::URI_PREFIX_LEN;
                $comma = \strpos($chunk, ',', $uriStart);

                if ($comma !== false && $comma < $lineEnd) {
                    $uri  = \substr($chunk, $uriStart, $comma - $uriStart);
                    $date = \substr($chunk, $comma + 1, 10);
                    if (!isset($counters[$uri][$date])) {
                        $counters[$uri][$date] = 1;
                    } else {
                        $counters[$uri][$date] ++;
                    }
                }

                $start = $lineEnd + 1;

            } while ($start < \strlen($chunk) || !\feof($input));


            $buffer = "{";
            $lines = 0;
            $firstUri = true;
            // Output validation requires that the URIs are the order they were first encountered in the input.
            foreach ($counters as $uri => $dates) {
                if (!$firstUri) {
                    $buffer .= ",";
                }
                $firstUri = false;

                // Output validation requires that the URI slashes are escaped.
                $buffer .= "\n    \"" . str_replace('/', '\\/', $uri) . "\": {";

                // Output validation requires that the dates are in ascending order.
                ksort($dates);

                $firstDate = true;
                foreach ($dates as $date => $count) {
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
