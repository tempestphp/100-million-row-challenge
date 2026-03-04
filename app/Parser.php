<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $visitStats = [];
        $readLimit = 1024 * 1024; // 1MB
        $writeLimit = 1024 * 1024; // 1MB

        // open the input file and read line by line
        $inputRes = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($inputRes, 0);
        $baseUrlLen = 0;
        $previousRaw = '';
        $timestampLen = 25; // e.g. 2024-09-13T06:26:07+00:00
        while (true) {
            $raw = \fread($inputRes, $readLimit);
            if ($raw === '' || $raw === false) {
                break;
            }
            $raw = $previousRaw . $raw;
            $from = 0;
            while (true) {
                $newlinePos = \strpos($raw, "\n", $from);
                if ($newlinePos === false) {
                    $previousRaw = \substr($raw, $from);
                    break;
                }
                $comma = $newlinePos - $timestampLen - 1;
                if ($baseUrlLen === 0) {
                    $baseUrlLen = \strpos($raw, ':', $from) + 3;
                    $baseUrlLen = \strpos($raw, '/', $baseUrlLen) + 1;
                }

                $from += $baseUrlLen;
                $url = \substr($raw, $from, $comma - $from);
                // first three year digits are always 202, so we can skip them
                $date = \substr($raw, $comma + 4, 7);
                if (!isset($visitStats[$url])) {
                    $visitStats[$url] = [$date => 1];
                } else {
                    $visitStats[$url][$date] = ($visitStats[$url][$date] ?? 0) + 1;
                }

                $from = $newlinePos + 1;
            }
        }
        assert($previousRaw === '', 'The input file does not end with a newline character');
        \fclose($inputRes);

        // write the result to the output file
        $outputRes = \fopen($outputPath, 'wb');
        \stream_set_write_buffer($outputRes, 0);
        $buffer = "{\n";
        $bufferLen = 2;
        $hasFirstUrlWritten = false;
        foreach ($visitStats as $url => $data) {
            if ($hasFirstUrlWritten) {
                $buffer .= ",\n";
                $bufferLen += 2;
            }
            $hasFirstUrlWritten = true;
            $url = \str_replace('/', '\/', $url);
            $buffer .= "    \"\\/$url\": {\n";
            $bufferLen += 40; // rough estimate of the length of the URL
            \ksort($data, \SORT_STRING);
            $hasFirstDateWritten = false;
            foreach ($data as $date => $count) {
                if ($hasFirstDateWritten) {
                    $buffer .= ",\n";
                    $bufferLen += 2;
                }
                $hasFirstDateWritten = true;
                $buffer .= "        \"202$date\": $count";
                $bufferLen += 23; // rough estimate of the length of the date and count
                if ($bufferLen > $writeLimit) {
                    \fwrite($outputRes, $buffer);
                    $buffer = '';
                    $bufferLen = 0;
                }
            }
            $buffer .= "\n    }";
            $bufferLen += 6;
        }

        $buffer .= "\n}";
        \fwrite($outputRes, $buffer);
        \fclose($outputRes);
        unset($visitStats);
    }
}
