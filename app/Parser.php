<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $return = [];

        $handle = fopen($inputPath, "r");

        while ($data = fgets($handle)) {
            $data = explode(',', $data);

            $url = $data[0];
            $date = substr($data[1], 0, 10);

            $return[$url][$date] = ($return[$url][$date] ?? 0) + 1;
        }

        fclose($handle);

        foreach ($return as $_ => &$data) {
            ksort($data);
        }

        file_put_contents(
            $outputPath,
            str_replace('https:\/\/stitcher.io', '', json_encode($return, JSON_PRETTY_PRINT))
        );
    }
}