<?php

namespace App;

use Exception;

final class Parser
{
    const PATH_PREFIX_LENGTH = 19;
    const DATE_LENGTH = 10;
    const TIME_LENGTH = 15;
    const DATETIME_LENGTH = self::DATE_LENGTH + self::TIME_LENGTH;

    private float $lastTime = 0;

    // Helper for timing parts of the code
    private function time(string $tag = '', ?float $start = null): float {
        $newtime = microtime(true);
        echo $newtime - ($start ?? $this->lastTime), ' '.$tag, PHP_EOL;
        $this->lastTime = $newtime;
        return $newtime;
    }

    public function parse(string $inputPath, string $outputPath): void
    {
        // $start = $this->time('start');
        $data = [];
        foreach($this->parseInput($inputPath) as $url => $date) {
            if (isset($data[$url][$date])) {
                $data[$url][$date]++;
            } else {
                $data[$url][$date] = 1;
            }
        }
        // $start = $this->time('reading and parsing', $start);
        $this->sortAndOutput($data, $outputPath);
        // $start = $this->time('sorting and output', $start);
    }

    private function parseInput(string $inputPath) {
        $r = fopen($inputPath, 'r');
        while (($line = fgets($r)) !== false) {
            yield substr($line, self::PATH_PREFIX_LENGTH, -self::DATETIME_LENGTH - 2) => substr($line, -self::DATETIME_LENGTH - 1, self::DATE_LENGTH);
        }
    }

    private function sortAndOutput($data, $outputPath) {
        $output = "{\n";
        $limitPaths = count($data);
        $p = 0;
        foreach ($data as $path => $dates) {
            $escapedPath = str_replace('/', '\\/', $path);
            $output .= "    \"$escapedPath\": {\n";
            ksort($dates);
            $limit = count($dates);
            $i = 0;
            foreach ($dates as $date => $times) {
                if (++$i < $limit) {
                    $output .= "        \"$date\": $times,\n";
                } else {
                    $output .= "        \"$date\": $times\n";
                }
            }
            if (++$p < $limitPaths) {
                $output .= "    },\n";
            } else {
                $output .= "    }\n}";
            }
        }
        // $this->time('sorting and encoding');
        file_put_contents($outputPath, $output);
        // $this->time('file_put_contents');
    }

    // Fill a heap instead of an array, a bit faster and sorted but slower to output when you need to count dates
    private function fillHeap(string $inputPath): array {
        $data = [];
        foreach($this->parseInput($inputPath) as $url => $date) {
            $data[$url] ??= new \SplMinHeap();
            $data[$url]->insert($date);
        }
        return $data;
    }
    // Not working, but too slow anyway
    private function outputHeap($data, $outputPath) {
        $output = "{\n";
        $countPaths = count($data);
        $p = 0;
        foreach ($data as $path => $dates) {
            $escapedPath = str_replace('/', '\\/', $path);
            $output .= "    \"$escapedPath\": {\n";
            $count = count($dates);
            $i = 0;
            $prevDate = null;
            $times = 1;
            foreach ($dates as $date) {
                if ($date !== $prevDate) {
                    if ($prevDate !== null) {
                        $output .= "        \"$prevDate\": $times,\n";
                    }
                    $times = 1;
                    $prevDate = $date;
                } else {
                    $times++;
                }
                if ($i++ == $count - 1) {
                    $output .= "        \"$date\": $times\n";
                    break;
                }
            }
            if ($p++ < $countPaths - 1) {
                $output .= "    },\n";
            } else {
                $output .= "    }\n";
            }
        }
        $output .= "}";
        $this->time('counting and encoding');
        file_put_contents($outputPath, $output);
        $this->time('file_put_contents');
    }
    // file_get_contents and then a state machine, not optimized but already too slow
    private function parseInput2(string $inputPath) {
        $dataRaw = file_get_contents($inputPath);
        $state = 1;
        $slashes = 0;
        $c = true;
        for ($i = 0; $c !== false ;++$i) {
            $c = $dataRaw[$i] ?? false;
            switch ($state) {
                case 1:
                    if ($c === '/') {
                        $slashes++;
                        if ($slashes >= 3) {
                            $state = 2;
                            $path = '/';
                        }
                    }
                    continue 2;
                case 2:
                    if ($c === ',') {
                        $state = 3;
                        $date = '';
                        continue 2;
                    }
                    $path .= $c;
                    continue 2;
                case 3:
                    if ($c === 'T') {
                        $state = 4;
                        yield $path => $date;
                    }
                    $date .= $c;
                    continue 2;
                case 4:
                    if ($c === '/') {
                        $state = 1;
                        $slashes = 1;
                    }
                    continue 2;
            }
        }
    }
    // fgetcsv, for reference
    private function parseInputCsv(string $inputPath) {
        $r = fopen($inputPath, 'r');
        stream_set_read_buffer($r, 1_000_000);
        while (($line = fgetcsv($r, escape:'')) !== false) {
            yield $line[0] => $line[1];
        }
    }
    // ksort and json_encode, for reference
    private function sortAndOutputJsonEncode($data, $outputPath) {
        array_walk(
            $data,
            static fn(&$dates) => ksort($dates)
        );
        $this->time('sorting');
        $output = json_encode($data, flags:JSON_PRETTY_PRINT);
        $this->time('encoding');
        file_put_contents($outputPath, $output);
    }
}
