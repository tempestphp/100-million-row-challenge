<?php

namespace App;

final class Parser
{
    public static function parse(string $inputPath, string $outputPath): void
    { 
        gc_disable();      
        $f = fopen($inputPath, 'r');
        stream_set_chunk_size($f, 1<<24);

        $h = [];

        $chunk = '';
        while (!feof($f)) {
            $chunk .= fread($f, 1<<24);
            $p = 0;
            while ($e = strpos($chunk, PHP_EOL, $p)) {
                $h[substr($chunk, $p, $e - $p - 26)][] = substr($chunk, $e - 25, 10);
                $p = $e + 1;
            }
            $chunk = substr($chunk,strrpos($chunk, PHP_EOL, -1));
        }

        $r = [];
        foreach ($h as $u => $v) {
            $v = array_count_values($v);
            ksort($v);
            $r[parse_url($u, PHP_URL_PATH)] = $v;
        };

        file_put_contents($outputPath, json_encode($r, JSON_PRETTY_PRINT));
    }
}

if ($_SERVER['SCRIPT_FILENAME'] === 'app/Parser.php') {
    \App\Parser::parse(
        $argv[2] ?? (__DIR__ . '/../data/data.csv'),
        $argv[3] ?? (__DIR__ . '/../data/data.json')
    );
}