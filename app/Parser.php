<?php

namespace App;

use Exception;
use Generator;

final class Parser
{
    private const int CHUNK_SIZE = 1024 * 1024 * 1;

    public function parse(string $inputPath, string $outputPath): void
    {
        $urls = $this->getUrls($inputPath);
        $this->jsonStreamUrlsWithPlaceholders($urls, $outputPath);
        $this->handleDatesForEachUrl($urls, $inputPath, $outputPath);
    }

    private function getUrlPath(string $url, bool $escaped = false): string
    {
        $path = substr($url, 19);

        if ($escaped) {
            $path = '"'.str_replace('/', '\/', $path).'"';
        }

        return $path;
    }

    private function readLineByLine(string $inputPath): Generator
    {
        $file = fopen($inputPath, 'r');

        while (($line = fgets($file)) !== false) {
            yield $line;
        }

        fclose($file);
    }

    private function readChunkByChunk(string $inputPath): Generator
    {
        $file = fopen($inputPath, 'r');

        while (!feof($file)) {
            yield fread($file, self::CHUNK_SIZE);
        }

        fclose($file);
    }

    private function getUrls(string $inputPath): array
    {
        $urls = [];
        $left = '';

        foreach ($this->readChunkByChunk($inputPath) as $chunk) {
            $lastEolPost = strrpos($chunk, PHP_EOL);
            $buffer = $left . substr($chunk, 0, $lastEolPost);
            $left = substr($chunk, $lastEolPost + 1, strlen($chunk));
            $lines = explode(PHP_EOL, $buffer);

            foreach ($lines as $line) {
                $parts = explode(',', $line);
                $urls[$parts[0]] = true;
            }
        }

        return $urls;
    }

    private function jsonStreamUrlsWithPlaceholders(array $urls, string $outputPath): void
    {
        $fileOutput = fopen($outputPath, 'w');
        fwrite($fileOutput, "{\n");
        $isFirst = true;
        $content = '';

        foreach (array_keys($urls) as $url) {
            $escapedPath = $this->getUrlPath($url, escaped: true);

            if ($isFirst) {
                $content .= '    '.$escapedPath.': {dates_placeholder}';
            } else {
                $content .= ",\n".'    '.$escapedPath.': {dates_placeholder}';
            }

            $isFirst = false;
        }

        fwrite($fileOutput, $content."\n}");
        fclose($fileOutput);
    }

    private function handleDatesForEachUrl(array $urls, string $inputPath, string $outputPath): void
    {
        foreach (array_keys($urls) as $url) {
            $dates = $this->getDates($url, $inputPath);
            ksort($dates);
            $this->jsonStreamReplaceDatesPlaceholders($url, $dates, $outputPath);
        }
    }

    private function getDates(string $url, string $inputPath): array
    {
        $dates = [];
        $left = '';

        foreach ($this->readChunkByChunk($inputPath) as $chunk) {
            $lastEolPost = strrpos($chunk, PHP_EOL);
            $buffer = $left . substr($chunk, 0, $lastEolPost);
            $left = substr($chunk, $lastEolPost + 1, strlen($chunk));
            $lines = explode(PHP_EOL, $buffer);

            foreach ($lines as $line) {
                $parts = explode(',', $line);

                if ($url === $parts[0]) {
                    $date = substr($parts[1], 0, 10);

                    $dates[$date] = $dates[$date] ?? 0;
                    $dates[$date]++;
                }
            }
        }

        return $dates;
    }

    private function jsonStreamReplaceDatesPlaceholders(string $url, array $dates, string $outputPath): void
    {
        $escapedPath = $this->getUrlPath($url, escaped: true);
        $search = $escapedPath . ': {dates_placeholder}';
        $dateContent = '';
        $dateKeys = array_keys($dates);
        $last = array_key_last($dateKeys);

        foreach ($dateKeys as $i => $date) {
            $comma = $i === $last ? '' : ',';
            $dateContent .= "\n        " . '"' . $date . '": ' . $dates[$date] . $comma;
        }

        $replace = $escapedPath . ': {' . $dateContent . "\n    }";

        $tmpPath = tempnam(dirname($outputPath), '.tmp_');
        $in = fopen($outputPath, 'rb');
        $out = fopen($tmpPath, 'wb');

        $overlap = strlen($search) - 1;
        $carry   = '';

        while (!feof($in)) {
            $chunk = fread($in, self::CHUNK_SIZE);

            if ($chunk === false || $chunk === '') {
                break;
            }

            $buffer = $carry . $chunk;

            if (!feof($in)) {
                $carry = substr($buffer, -$overlap);
                $writeChunk = substr($buffer, 0, strlen($buffer) - $overlap);
            } else {
                $carry = '';
                $writeChunk = $buffer;
            }

            fwrite($out, str_replace($search, $replace, $writeChunk));
        }

        if ($carry !== '') {
            fwrite($out, str_replace($search, $replace, $carry));
        }

        fclose($in);
        fclose($out);

        chmod($tmpPath, fileperms($outputPath));
        rename($tmpPath, $outputPath);
    }
}