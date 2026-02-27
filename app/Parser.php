<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        // \gc_disable();
        $partSize = 4096 * 20;
        $f = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($f, 0);
        $prev = '';
        $results = [];
        while ($data = \fread($f, $partSize)) {
            $data = $prev . $data;

            $pos = \strrpos($data, "\n");
            if ($pos !== false) {
                $prev = \substr($data, $pos);
                $data = \substr($data, 0, $pos);
            }

            // https://stitcher.io/blog/php-81-performance-in-real-life,2022-01-12T10:32:24+00:00

            \preg_match_all('/\/\/stitcher\.io(.+)\,(\d{4}-\d{2}-\d{2})/', $data, $match, \PREG_SET_ORDER);

            foreach ($match as [, $path, $date]) {
                $results[$path][$date] ??= 0;
                $results[$path][$date]++;
            }

            $match = [];
        }

        \fclose($f);
        // \gc_enable();

        foreach ($results as &$array) {
            \ksort($array);
        }

        \file_put_contents($outputPath, \json_encode($results, \JSON_PRETTY_PRINT));
    }
}
