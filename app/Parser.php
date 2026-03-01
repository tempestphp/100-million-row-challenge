<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $visitStats = [];
        $readLimit = 1024 * 1024 * 5; // 5MB
        $writeLimit = 1024 * 1024; // 1MB

        // open the input file and read line by line
        $inputRes = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($inputRes, 0);
        $baseUrlLen = 0;
        $previousRaw = '';
        while (true) {
            $raw = \fread($inputRes, $readLimit);
            if ($raw === '' || $raw === false) {
                break;
            }
            $lines = \explode("\n", $previousRaw . $raw);
            $lineCount = \count($lines);
            $previousRaw = $lines[$lineCount - 1];
            $lineCount--;
            for ($i = 0; $i < $lineCount; $i++) {
                $line = $lines[$i];
                if ($baseUrlLen === 0) {
                    $baseUrlLen = \strpos($line, ':') + 3;
                    $baseUrlLen = \strpos($line, '/', $baseUrlLen) + 1;
                }
                $comma = \strpos($line, ',', $baseUrlLen);
                $url = \substr($line, $baseUrlLen, $comma - $baseUrlLen);
                $date = \substr($line, $comma + 1, 10);
                if (!isset($visitStats[$url])) {
                    $visitStats[$url] = [$date => 1];
                } else {
                    $visitStats[$url][$date] = ($visitStats[$url][$date] ?? 0) + 1;
                }
            }
        }
        if ($previousRaw !== '') { // if somehow the input file does not end with a newline
            $line = $previousRaw;
            $comma = \strpos($line, ',', $baseUrlLen);
            $url = \substr($line, $baseUrlLen, $comma - $baseUrlLen);
            $date = \substr($line, $comma + 1, 10);
            if (!isset($visitStats[$url])) {
                $visitStats[$url] = [$date => 1];
            } else {
                $visitStats[$url][$date] = ($visitStats[$url][$date] ?? 0) + 1;
            }
        }
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
            $temp = "    \"\\/$url\": {\n";

            \ksort($data, \SORT_STRING);
            $hasFirstDateWritten = false;
            foreach ($data as $date => $count) {
                if ($hasFirstDateWritten) {
                    $temp .= ",\n";
                }
                $hasFirstDateWritten = true;
                $temp .= "        \"$date\": $count";
            }
            $temp .= "\n    }";
            $buffer .= $temp;
            $bufferLen += \strlen($temp);

            if ($bufferLen > $writeLimit) {
                \fwrite($outputRes, $buffer);
                $buffer = '';
                $bufferLen = 0;
            }
        }

        $buffer .= "\n}";
        \fwrite($outputRes, $buffer);
        \fclose($outputRes);
        unset($visitStats);
    }
}
