<?php

namespace App;

use Exception;
use Generator;

final class Parser
{
    private const int CHUNK_SIZE = 1024 * 1024 * 14;
    private const int DATE_CHUNK_SIZE = 1024 * 1024 * 5;
    private static array $urls = [];
    private static array $doneUrls = [];

    public function parse(string $inputPath, string $outputPath): void
    {
        foreach ($this->getUrls($inputPath) as $chunkUrls) {
            foreach ($this->getUrlsAndDates($inputPath) as $chunkUrlsAndDates) {
                foreach ($chunkUrls as $url => $true) {
                    if (!isset($chunkUrlsAndDates[$url])) {
                        continue;
                    }

                    $dates = $chunkUrlsAndDates[$url];

                    if (!isset(self::$urls[$url])) {
                        self::$urls[$url] = $dates;
                    } else {
                        foreach ($dates as $date => $count) {
                            self::$urls[$url][$date] = (self::$urls[$url][$date] ?? 0) + $count;
                        }
                    }

                    ksort(self::$urls[$url]);
                }
            }

            $this->jsonStream($outputPath);
            self::$urls = [];
        }
    }

    private function getUrls(string $inputPath): Generator
    {
        $left = '';

        foreach ($this->readChunkByChunk($inputPath, self::CHUNK_SIZE) as $chunk) {
            $lastEolPost = strrpos($chunk, PHP_EOL);
            $buffer = $left . substr($chunk, 0, $lastEolPost);
            $left = substr($chunk, $lastEolPost + 1, strlen($chunk));
            $lines = explode(PHP_EOL, $buffer);
            $urls = [];

            foreach ($lines as $line) {
                $parts = explode(',', $line);

                if (!isset(self::$doneUrls[$parts[0]])) {
                    $urls[$parts[0]] = true;
                }
            }

            yield $urls;
        }
    }

    private function getUrlsAndDates(string $inputPath): Generator
    {
        $left = '';

        foreach ($this->readChunkByChunk($inputPath, self::DATE_CHUNK_SIZE) as $chunk) {
            $lastEolPost = strrpos($chunk, PHP_EOL);
            $buffer = $left . substr($chunk, 0, $lastEolPost);
            $left = substr($chunk, $lastEolPost + 1, strlen($chunk));
            $lines = explode(PHP_EOL, $buffer);
            $urlsAndDates = [];

            foreach ($lines as $line) {
                $parts = explode(',', $line);
                $url = $parts[0];
                $date = substr($parts[1], 0, 10);

                if (!isset($urlsAndDates[$url][$date])) {
                    $urlsAndDates[$url][$date] = 0;
                }

                $urlsAndDates[$url][$date]++;
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

    private function readChunkByChunk(string $inputPath, int $chunkSize): Generator
    {
        $file = fopen($inputPath, 'r');

        while (!feof($file)) {
            yield fread($file, $chunkSize);
        }

        fclose($file);
    }

    private function jsonStream(string $outputPath): void
    {
        $fileOutput = fopen($outputPath, 'w');
        fwrite($fileOutput, "{\n");
        $isFirst = true;
        $content = '';

        foreach (self::$urls as $url => $dates) {
            $escapedPath = $this->getUrlPath($url, escaped: true);
            $content .= $isFirst
                ? '    '.$escapedPath.': {'
                : ",\n".'    '.$escapedPath.': {';
            $dateKeys = array_keys($dates);
            $last = array_key_last($dateKeys);

            foreach ($dateKeys as $i => $date) {
                $comma = $i === $last ? '' : ',';
                $content .= "\n        " . '"' . $date . '": ' . $dates[$date] . $comma;
            }

            $content .= "\n    }";
            $isFirst = false;
            self::$doneUrls[$url] = true;
        }

        fwrite($fileOutput, $content."\n}");
        fclose($fileOutput);
    }
}