<?php

namespace App;

final class Parser
{
    private const CHUNK_SIZE = 100000;

    public function parse(string $inputPath, string $outputPath): void
    {
        $buffer = '';
        $handle = fopen($inputPath, 'r');
        stream_set_read_buffer($handle, 0);

        $data = [];
       
        while (feof($handle) === false) {
            $buffer .= fread($handle, self::CHUNK_SIZE);
            $lines = explode("\n", $buffer);
            $buffer = array_pop($lines);
            foreach ($lines as $line) {
                $comma = strpos($line, ',');
                $baseUrl = substr($line, 19, $comma - 19);
                $date = substr($line, $comma + 1, 10);
                $data[$baseUrl][$date] = ($data[$baseUrl][$date] ?? 0) + 1;
            }
        }

        if (!empty($buffer)) {
            $comma = strpos($buffer, ',');
            $baseUrl = substr($buffer, 19, $comma - 19);
            $date = substr($buffer, $comma + 1, 10);
            $data[$baseUrl][$date] = ($data[$baseUrl][$date] ?? 0) + 1;
        }

        fclose($handle);

        $handle = fopen($outputPath, 'wb');
        $firstUrl = true;

        $dataKeys = array_keys($data);
        $last_url = str_replace('/', '\/', end($dataKeys));

        fwrite($handle, '{');

        foreach ($data as $url => $timestamps) {
            ksort($timestamps);

            if ($firstUrl) {
                fwrite($handle, "\n");
                $firstUrl = false;
            }

            $url = str_replace('/', '\/', $url);
            $buffer = '    "' . $url . '": {' . "\n";

            $timestampKeys = array_keys($timestamps);
            $last_ts = end($timestampKeys);

            foreach ($timestamps as $timestamp => $count) {
                $buffer .= '        "' . $timestamp. '": ';
                $buffer .= $count;
                if ($timestamp !== $last_ts) $buffer .= ',';
                $buffer .= "\n";
            }

            $buffer .= '    }';
            if ($url !== $last_url) $buffer .= ',';
            $buffer .=  "\n";
            fwrite($handle, $buffer);
        }
        fwrite($handle, '}');
    }
}