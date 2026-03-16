<?php

namespace App;

use Exception;
use Generator;

final class Parser
{
    private const int CHUNK_SIZE = 1024 * 1024 * 150;
    private static array $urls = [];

    public function parse(string $inputPath, string $outputPath): void
    {
        $this->getUrlsAndDates($inputPath);
        $this->jsonStream($outputPath);
    }

    private function getUrlsAndDates(string $inputPath): void
    {
        $left = '';

        foreach ($this->readChunkByChunk($inputPath) as $chunk) {
            $lastEolPos = strrpos($chunk, "\n");
            $buffer = $left . substr($chunk, 0, $lastEolPos);
            $left = substr($chunk, $lastEolPos + 1);

            $lines = explode("\n", $buffer);

            foreach ($lines as $line) {
                $parts = explode(',', $line);
                $url = $parts[0];
                $date = substr($parts[1], 0, 10);

                if (!isset(self::$urls[$url][$date])) {
                    self::$urls[$url][$date] = 0;
                }
                self::$urls[$url][$date]++;
            }
        }
    }

    private function getUrlPath(string $url, bool $escaped = false): string
    {
        $path = substr($url, 25);

        if ($escaped) {
            $path = '"\/blog\/'.$path.'"';
        }

        return $path;
    }

    private function readChunkByChunk(string $inputPath): Generator
    {
        $file = fopen($inputPath, 'r');

        while (!feof($file)) {
            yield fread($file, self::CHUNK_SIZE);
        }

        fclose($file);
    }

    private function jsonStream(string $outputPath): void
    {
        $fileOutput = fopen($outputPath, 'w');
        fwrite($fileOutput, "{\n");

        $isFirst = true;

        foreach (self::$urls as $url => $dates) {
            $escapedPath = $this->getUrlPath($url, escaped: true);

            ksort($dates);

            if ($isFirst) {
                $content = '    '.$escapedPath.': {';
            } else {
                $content = ",\n".'    '.$escapedPath.': {';
            }

            $dateKeys = array_keys($dates);
            $last = array_key_last($dateKeys);

            foreach ($dateKeys as $i => $date) {
                $comma = $i === $last ? '' : ',';
                $content .= "\n        " . '"' . $date . '": ' . $dates[$date] . $comma;
            }

            fwrite($fileOutput, $content."\n    }");
            $isFirst = false;
        }

        fwrite($fileOutput, "\n}");
        fclose($fileOutput);
    }
}
