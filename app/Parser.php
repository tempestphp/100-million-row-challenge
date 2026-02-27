<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();
        $f = fopen($inputPath, 'rb');
        $v = fgets($f);
        $s = strpos($v, '/', 8); // discover scheme://host prefix len
        fclose($f);

        $shm = shm_attach(0, 1000000000);
        $sem = sem_get(0);

        $ncpu = 8;
        $chunk = floor(filesize($inputPath) / $ncpu);
        $children = [];
        for ($i=0; $i < $ncpu; $i++) {
            $pid = pcntl_fork();
            if ($pid === 0) {
                $f = fopen($inputPath, 'rnb');
                $job = $this->parseChunk($f, $s, $i, $chunk);
                sem_acquire($sem); // shm needs mutex
                shm_put_var($shm, $i, $job); // passed as string to avoid serializing inside semaphore
                sem_release($sem);
                die; // children work is done, parachute
            }
            $children[$i] = $pid;
        }

        $h = [];
        for ($i=0; $i < $ncpu; $i++) {
            $pid = pcntl_waitpid(-1, $status);
            $p = unserialize(shm_get_var($shm, array_search($pid, $children)));

            foreach ($p as $k => $v) {
                foreach ($v as $d => $c) {
                    $h[$k][$d] ??= 0;
                    $h[$k][$d] += $c;
                }
            };
        }

        foreach ($h as &$v) {
           ksort($v);
        };
        

        file_put_contents($outputPath, json_encode($h, JSON_PRETTY_PRINT));
    }

    public function parseChunk($f, $s, $i, $chunk) {

        if ($i > 0) {
            fseek($f, $i * $chunk);
            $chunk -= strlen(fgets($f)); // go to next line (skip incomplete lines)
        }

        $h = [];
        while ($chunk > 0 && $v = stream_get_line($f, 8192, PHP_EOL)) {
            $h[substr($v, $s, -26)][] = substr($v, -25, 10);
            $chunk -= strlen($v);
            $chunk--; // EOL
        }

        foreach ($h as &$v) {
            $v = array_count_values($v);
        };

        return serialize($h);
    }
}