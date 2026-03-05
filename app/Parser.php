<?php

namespace App;

use function ksort;
use function substr;
use function strpos;
use function stream_set_read_buffer;
use function file_put_contents;
use function fclose;
use function fopen;
use function json_encode;
use function count;
use function gc_disable;
use function gc_enable;
use function fgets;
use const SORT_STRING;
use const FALSE;
use const JSON_PRETTY_PRINT;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $keyedData = [];
        $handle = fopen($inputPath, 'r');
        stream_set_read_buffer($handle, 1048576);
        gc_disable();

        while (($data = fgets($handle)) !== FALSE) {
            $firstComma = strpos($data, ',');
            $key = substr($data, 19, $firstComma - 19);
            $value = substr($data, $firstComma + 1, 10);

            if (isset($keyedData[$key][$value])) {
                $keyedData[$key][$value]++;
            } else {
                $keyedData[$key][$value] = 1;
            }
        }

        fclose($handle);
        unset($handle, $data);

        foreach ($keyedData as &$value) {
            if (count($value) > 1) {
                ksort($value, SORT_STRING);
            }
        }
        unset($value);
        gc_enable();

        file_put_contents($outputPath, json_encode($keyedData, JSON_PRETTY_PRINT));
    }
}