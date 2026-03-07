<?php

namespace App;

use function array_pop;
use function explode;
use function fclose;
use function feof;
use function fread;
use function fopen;
use function fwrite;
use function json_encode;
use function ksort;
use function printf;
use function strlen;
use function stream_set_read_buffer;
use function substr;

final class Parser
{
    private array $output = [];
    private const int CHUNK_SIZE = 1024 * 512;

    public function parse(string $inputPath, string $outputPath): void
    {
        $handle = fopen($inputPath, 'r');
        stream_set_read_buffer($handle, 0);

        $buffer = '';
        while (feof($handle) === false) {
            $buffer .= fread($handle, self::CHUNK_SIZE);
            $lines = explode("\n", $buffer);

            //get partial line for next chunk
            $buffer = array_pop($lines);

            foreach ($lines as $line) {
                $url = substr($line, 19, -26);
                $timestamp = substr($line, -25, 10);

                $this->output[$url][$timestamp] ??= 0;
                $this->output[$url][$timestamp]++;
            }
            unset($lines);
        }

        //parse final line
        if ($buffer !== '') {
            $url = substr($buffer, 19, -26);
            $timestamp = substr($buffer, -25, 10);

            $this->output[$url][$timestamp] ??= 0;
            $this->output[$url][$timestamp]++;
            unset($buffer);
        }
        fclose($handle);

        $this->writeOutput($outputPath);

        printf(
            'Memory usage: %.2fM' . PHP_EOL,
            memory_get_peak_usage() / (1024 * 1024)
        );
    }

    private function writeOutput(string $outputPath): void
    {
        foreach ($this->output as $url => $timestamps) {
            ksort($timestamps);
            $this->output[$url] = $timestamps;
        }

        $handle = fopen($outputPath, 'w');
        $data = json_encode($this->output, JSON_PRETTY_PRINT);
        fwrite($handle, $data, strlen($data));
        unset($this->output);
    }
}