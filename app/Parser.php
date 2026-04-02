<?php

namespace App;

use function feof;
use function fread;
use function strrpos;
use function strpos;
use function substr;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();
        $handle = fopen($inputPath, 'r');
        stream_set_read_buffer($handle, 0);
        $visits = [];
        $leftover = '';
        while (!feof($handle)) {
            $chunk = $leftover . fread($handle, 2_097_152);
            $lastPos = $newlinePos = 0;
            $lastNewLine = strrpos($chunk, "\n");
            while ($newlinePos !== $lastNewLine) {
                $commaPos = strpos($chunk, ',', $lastPos + 27);
                $newlinePos = $commaPos + 26;
                $url = substr($chunk, $lastPos + 19, $commaPos - $lastPos - 19);
                $date = substr($chunk, $commaPos + 1, 10);
                if (!isset($visits[$url][$date])) {
                    $visits[$url][$date] = 1;
                } else {
                    $visits[$url][$date]++;
                }
                $lastPos = $newlinePos + 1;
            }
            $leftover = substr($chunk, $lastPos);
        }
        $commaPos = strpos($leftover, ",");
        $url = substr($leftover, 19, $commaPos - 19);
        $date = substr($leftover, $commaPos + 1, 10);
        if ('' !== $url) {
            if (!isset($visits[$url][$date])) {
                $visits[$url][$date] = 1;
            } else {
                $visits[$url][$date]++;
            }
        }

        fclose($handle);
        foreach ($visits as $_ => &$dates) {
            ksort($dates);
        }

        $json = json_encode($visits, JSON_PRETTY_PRINT);
        file_put_contents($outputPath, $json);
    }
}