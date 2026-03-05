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
        \gc_disable();
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
        $r = \fopen($inputPath, 'r');
        while (($line = \fgets($r)) !== false) {
            yield \substr($line, self::PATH_PREFIX_LENGTH, -self::DATETIME_LENGTH - 2) => \substr($line, -self::DATETIME_LENGTH - 1, self::DATE_LENGTH);
        }
    }

    private function sortAndOutput($data, $outputPath) {
        $output = "{\n";
        $first = true;
        foreach ($data as $path => $dates) {
            $escapedPath = \str_replace('/', '\\/', $path);
            if ($first) {
                $first = false;
            } else {
                $output .= ",\n";
            }
            \ksort($dates);
            $limit = \count($dates);
            $firstDate = true;
            foreach ($dates as $date => $times) {
                if ($firstDate) {
                    $firstDate = false;
                    $output .= "    \"{$escapedPath}\": {\n        \"{$date}\": {$times}";
                } else {
                    $output .= ",\n        \"{$date}\": {$times}";
                }
            }
            $output .= "\n    }";
        }
        $output .= "\n}";
        // $this->time('sorting and encoding');
        \file_put_contents($outputPath, $output);
        // $this->time('file_put_contents');
    }
}
