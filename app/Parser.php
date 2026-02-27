<?php

namespace App;

use function ksort;
use function substr;
use function file_put_contents;
use function fclose;
use function fopen;
use function json_encode;
use function count;
use function gc_disable;
use function gc_enable;
use function fgets;
use function explode;
use const SORT_STRING;
use const FALSE;
use const JSON_PRETTY_PRINT;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $keyedData = [];
        $handle = fopen($inputPath, 'r');
        gc_disable();
        while (($data = fgets($handle)) !== FALSE) {
            $data = explode(',', $data);
            $key = substr($data[0], 19);
            $value = substr($data[1], 0, 10);

            $keyedData[$key][$value] = ($keyedData[$key][$value] ?? 0) + 1;
        }

        fclose($handle);
        gc_enable();
        unset($handle);
        unset($data);

        foreach ($keyedData as &$value) {
            if (count($value) > 1) {
                ksort($value, SORT_STRING);
            }
        }
        unset($value);

        file_put_contents($outputPath, json_encode($keyedData, JSON_PRETTY_PRINT));
    }
}