<?php

namespace App;

use Exception;

final class Parser
{
    // Most URLs are less than 100 characters, so 256 is a safe buffer size to find at least one newline.
    const NEWLINE_SEARCH_SIZE = 256;

    // 16MB read/write buffers.
    const STREAM_BUFFER_SIZE = 16 << 20;

    // 256B chunk size when reading input.
    const READ_CHUNK_SIZE = 128 << 20;

    // Number of workers to split the input into.
    const WORKER_COUNT = 4;

    // A line is ±75 characters, so 75 * 2^14 = 1.2 MiB, which is a good buffer size for this input,
    // so set a mask to quickly check when this many lines have been buffered.
    const LINE_BUFFER_MASK = (1 << 14) - 1;

    // Strip this prefix from every URI (length = 19).
    const URI_PREFIX_LEN = 19;

    public function parse(string $inputPath, string $outputPath): void
    {
        // Don't bother with garbage collection, to avoid any random slowdowns.
        \gc_disable();

        // Memory usage is expected to be high, but limited by the chunk size, so limit it to less than the 1.5GB that the test machine has.
        \ini_set('memory_limit', '1024M');


        $time = \microtime(true);

        $fileSize = \filesize($inputPath);
        $splitSize = (int)($fileSize / self::WORKER_COUNT);
        // The first worker will always start at the beginning of the file, so start with that split point.
        $splits = [0];

        // Calculate split points for the remaining workers by finding the first newline after
        // the split point, and positioning the split after it. This ensures that workers are
        // always processing whole lines.
        $input = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($input,  self::NEWLINE_SEARCH_SIZE);
        for ($i = 1; $i < self::WORKER_COUNT; $i++) {
            \fseek($input, $i * $splitSize);
            $data = \fread($input, self::NEWLINE_SEARCH_SIZE);

            // Find the first newline in the after the split point.
            $nl = \strpos($data, "\n");

            // Bail if the newline is not found, so an error is thrown instead of silently producing incorrect output.
            if ($nl === false) {
                throw new Exception("Failed to find newline after split point for worker $i");
            }

            $splits[] = \ftell($input) - \strlen($data) + $nl + 1;
        }
        $splits[] = $fileSize;

        for ($i = 0; $i < self::WORKER_COUNT; $i++) {
            socket_create_pair(AF_UNIX, SOCK_STREAM, 0, $sockets[$i]);
            $pid = pcntl_fork();
            if ($pid === 0) {
                // Each worker closes the parent end of the socket,...
                socket_close($sockets[$i][0]);

                // ...sends the results back to the parent through the socket, and exits.
                socket_write($sockets[$i][1], self::serialize($this->parseSection($inputPath, $splits[$i], $splits[$i + 1])));
                socket_close($sockets[$i][1]);
                exit(0);
            }
            socket_close($sockets[$i][1]);
        }

        // Fetch and unserialize the results from the workers.
        foreach ($sockets as $pair) {
            $data = '';
            while ($chunk = socket_read($pair[0], self::STREAM_BUFFER_SIZE)) $data .= $chunk;
            $results[] = self::unserialize($data);
        }

        // Take the first worker results as the starting point, and merge the results of the other workers into it.
        $counters = $results[0];
        for($i = 1; $i < self::WORKER_COUNT; $i++) {
            foreach ($results[$i] as $uri => $dates) {
                foreach ($dates as $date => $count) {
                    if (!isset($counters[$uri][$date])) {
                        $counters[$uri][$date] = $count;
                    } else {
                        $counters[$uri][$date] += $count;
                    }
                }
            }
        }

        echo "Parsed in " . (\microtime(true) - $time) . " seconds.\n";

        $output = \fopen($outputPath, 'wb');
        \stream_set_write_buffer($output, self::STREAM_BUFFER_SIZE);
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
            ksort($dates, \SORT_STRING);

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

        echo "Written in " . (\microtime(true) - $time) . " seconds.\n";

        @\fclose($output);
    }

    private function parseSection($inputPath, $start, $end): array
    {
        $input = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($input,  self::STREAM_BUFFER_SIZE);

        $counters = [];

        \fseek($input, $start);
        $chunkSize = min(self::READ_CHUNK_SIZE, $end - $start);
        $chunk = \fread($input, $chunkSize);
        $chunkLen = \strlen($chunk);
        $chunkStart = 0;
        $current = \ftell($input);
        do {
            // Try and find a line ending.
            $lineEnd = \strpos($chunk, "\n", $chunkStart);

            // When there is no line ending, but more input to read, grab another chunk and append
            // it to what is left of the current chunk.
            if ($lineEnd === false && $current < $end) {
                // No newline found, and we are not at the end of the file, so carry this chunk over to the next read.
                $chunkSize = min(self::READ_CHUNK_SIZE, $end - $current);
                $chunk = \substr($chunk, $chunkStart) . \fread($input, $chunkSize);
                $chunkLen = \strlen($chunk);
                $chunkStart = 0;
                $current = \ftell($input);
                continue;
            }

            // When at the end of the file, there may not be a line ending, so set the line end to the end of the chunk.
            if ($lineEnd === false) {
                $lineEnd = $chunkLen;
            }

            $uriStart = $chunkStart + self::URI_PREFIX_LEN;

            // A comma is expected to be found here, because either a line ending or end of file has been found. No
            // validation is done because the input format is expected to be correct.
            $comma = \strpos($chunk, ',', $uriStart);

            $uri  = \substr($chunk, $uriStart, $comma - $uriStart);
            $date = \substr($chunk, $comma + 1, 10);

            if (!isset($counters[$uri][$date])) {
                $counters[$uri][$date] = 1;
            } else {
                $counters[$uri][$date]++;
            }

            $chunkStart = $lineEnd + 1;
        } while ($chunkStart < $chunkLen || $current < $end);

        \fclose($input);

        return $counters;
    }

    private static function serialize(mixed $data): string
    {
        if (\function_exists('igbinary_serialize')) {
            return \igbinary_serialize($data);
        }
        return \serialize($data);
    }

    private static function unserialize(string $data): mixed
    {
        if (\function_exists('igbinary_unserialize')) {
            return \igbinary_unserialize($data);
        }
        return \unserialize($data);
    }
}
