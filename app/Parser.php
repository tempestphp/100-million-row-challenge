<?php

namespace App;

final class Parser
{
    public static function parse(string $inputPath, string $outputPath): void
    {
        $ncpu = 4;
        $chunkSize = floor(filesize($inputPath) / $ncpu);
        $children = [];
        for ($i=0; $i < $ncpu; $i++) {
            $tmp = tmpfile();
            $pid = pcntl_fork();
            if ($pid === 0) {  
                $job = self::parseInterval($inputPath, $i * $chunkSize, $i * $chunkSize + $chunkSize);
                fwrite($tmp, serialize($job));
                die; // children work is done, parachute
            }
            $children[$pid] = $tmp;
        }

        $h = [];
        foreach ($children as $pid => $tmp) {
            $pid = pcntl_waitpid($pid, $status);
            rewind($tmp);
            $c = stream_get_contents($tmp);
            $p = unserialize($c);

            foreach ($p as $k => $v) {
                foreach ($v as $d => $c) {
                    $h[$k][$d] ??= 0;
                    $h[$k][$d] += $c;
                }
            };
        }

        foreach ($h as $k => &$v) {
            ksort($v);
        };

        file_put_contents($outputPath, json_encode($h, JSON_PRETTY_PRINT));
    }

    public static function parseInterval($inputPath, $start, $limit) {
        $f = fopen($inputPath, 'rb');
        stream_set_chunk_size($f, 1<<24);

        if ($start != 0) {
            fseek($f, $start - 1);
            if (fgetc($f) !== PHP_EOL) {
                $start += strlen(fgets($f)); // skip first line: job N-1 ends with the first line of job N
            }
        }

        $chunkSize = min(1<<24, $limit - $start);

        $h = [];
        $chunk = '';
        while (true) {
            $chunk .= fread($f, $chunkSize);
            $p = 0;
            while ($e = strpos($chunk, PHP_EOL, $p)) {
                $h[substr($chunk, $p, $e - $p - 26)][] = substr($chunk, $e - 25, 10);
                $p = $e + 1;
            }
            $chunk = substr($chunk, strrpos($chunk, PHP_EOL, -1));
            if (ftell($f) >= $limit) {
                // complete last line
                if (strlen($chunk) > 1) {
                    $chunk .= fgets($f);
                    $e = strpos($chunk, PHP_EOL);
                    $h[substr($chunk, 1, -27)][] = substr($chunk, -26, 10);
                }
                break;
            }
        }

        $hits = [];
        foreach ($h as $url => $dates) {
            $dates = array_count_values($dates);
            $hits[parse_url($url, PHP_URL_PATH)] = $dates;
        }

        return $hits;
    }
}
