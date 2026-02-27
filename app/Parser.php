<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $ncpu = 8;
        $chunk = floor(filesize($inputPath) / $ncpu);
        $children = [];
        for ($i=0; $i < $ncpu; $i++) {
            $tmp = tmpfile();
            $pid = pcntl_fork();
            if ($pid === 0) {
                $f = fopen($inputPath, 'rnb');
                $job = $this->parseChunk($f, $i, $chunk);
                fwrite($tmp, igbinary_serialize($job));
                die; // children work is done, parachute
            }
            $children[$pid] = $tmp;
        }

        $h = [];
        foreach ($children as $pid => $tmp) {
            $pid = pcntl_waitpid($pid, $status);
            rewind($tmp);
            $c = stream_get_contents($tmp);
            $p = igbinary_unserialize($c);

            foreach ($p as $k => $v) {
                foreach ($v as $d => $c) {
                    $h[$k][$d] ??= 0;
                    $h[$k][$d] += $c;
                }
            };
        }

        $r = [];
        foreach ($h as $k => &$v) {
            ksort($v);
            preg_match('@://[^/]+(/.*)@', $k, $m);
            $r[$m[1]] = $v;
        };
        

        file_put_contents($outputPath, json_encode($r, JSON_PRETTY_PRINT));
    }

    public function parseChunk($f, $i, $chunk) {

        if ($i > 0) {
            fseek($f, $i * $chunk);
            $chunk -= strlen(fgets($f)); // go to next line (skip incomplete lines)
        }

        $h = [];
        while ($chunk > 0 && $v = stream_get_line($f, 8192, PHP_EOL)) {
            $h[substr($v, 0, -26)][] = substr($v, -25, 10);
            $chunk -= strlen($v);
            $chunk--; // EOL
        }

        foreach ($h as &$v) {
            $v = array_count_values($v);
        };

        return $h;
    }
}