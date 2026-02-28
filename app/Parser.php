<?php

namespace App;

use Exception;
use Generator;

final class Parser
{
    private const int CHUNK_SIZE = 1024 * 1024 * 70;
    private static array $urls = [];

    public function parse(string $inputPath, string $outputPath): void
    {
        foreach ($this->getUrlsAndDates($inputPath) as $chunkUrlsAndDates) {
            foreach ($chunkUrlsAndDates as $url => $dates) {
                if (!isset(self::$urls[$url])) {
                    self::$urls[$url] = $dates;
                } else {
                    foreach ($dates as $date => $count) {
                        self::$urls[$url][$date] = (self::$urls[$url][$date] ?? 0) + $count;
                    }
                }
            }
        }

        $this->jsonStream($outputPath);
    }

    private function getUrlsAndDates(string $inputPath): Generator
    {
        $left = '';

        foreach ($this->readChunkByChunk($inputPath) as $chunk) {
            $lastEolPos = strrpos($chunk, "\n");
            $buffer = $left . substr($chunk, 0, $lastEolPos);
            $left = substr($chunk, $lastEolPos + 1);

            $urlsAndDates = [];
            $line = strtok($buffer, "\n");

            while ($line !== false) {
                $commaPos = strpos($line, ',');
                $url = substr($line, 0, $commaPos);
                $date = substr($line, $commaPos + 1, 10);

                if (!isset($urlsAndDates[$url][$date])) {
                    $urlsAndDates[$url][$date] = 0;
                }

                $urlsAndDates[$url][$date]++;

                $line = strtok("\n");
            }

            yield $urlsAndDates;
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
                fwrite($fileOutput, '    '.$escapedPath.': {');
            } else {
                fwrite($fileOutput, ",\n".'    '.$escapedPath.': {');
            }

            $dateKeys = array_keys($dates);
            $last = array_key_last($dateKeys);

            foreach ($dateKeys as $i => $date) {
                $comma = $i === $last ? '' : ',';
                fwrite($fileOutput, "\n        " . '"' . $date . '": ' . $dates[$date] . $comma);
            }

            fwrite($fileOutput, "\n    }");
            $isFirst = false;
        }

        fwrite($fileOutput, "\n}");
        fclose($fileOutput);
    }
}
