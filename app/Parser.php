<?php

declare(strict_types=1);

namespace App;

use DateTime;
use Exception;
use Generator;
use function array_filter;
use function count;
use function fclose;
use function feof;
use function fgets;
use function file_put_contents;
use function fopen;
use function fread;
use function gc_disable;
use function json_encode;
use function preg_match_all;
use function sprintf;
use function str_ends_with;

final class Parser
{
    private const int READ_CHUNK_SIZE = 1024 * 1024;

    private const string REGEX_LINE_PARSER = ";^https://stitcher.io(/[^,]+),(.{10});m";

    private const int START_YEAR = 1965;

    /**
     * @var false|resource
     */
    private $inputHandle;

    /**
     * @throws Exception
     */
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();
        $toEncode = [];

        // Build a pre-computed date array, and then copy it for every new path we encounter.  This means 1 date-related
        // memory allocation per new path instead of one per new path/date combination
        $preComputedDates = $this->prebuildDates();

        try {
            ($this->inputHandle = fopen($inputPath, "r")) || throw new Exception("Couldn't open input file");
            foreach ($this->getChunk() as $chunk) {
                $matches = [];
                preg_match_all(self::REGEX_LINE_PARSER, $chunk, $matches);
                $matchCount = count($matches[1]);

                for ($key = 0; $key < $matchCount; $key++) {
                    $path = $matches[1][$key];
                    $date = $matches[2][$key];

                    isset($toEncode[$path]) || $toEncode[$path] = $preComputedDates;

                    $toEncode[$path][$date] += 1;
                }
            }

            foreach($toEncode as &$dates) {
                $dates = array_filter($dates);
                ksort($dates);
            }

            file_put_contents($outputPath, json_encode($toEncode, JSON_PRETTY_PRINT));
        } finally {
            !$this->inputHandle || fclose ($this->inputHandle);
        }
    }

    /**
     * @return Generator<string>
     */
    private function getChunk(): Generator
    {
        while(!feof($this->inputHandle)) {
            $str = fread($this->inputHandle, self::READ_CHUNK_SIZE);
            if (!str_ends_with($str, "\n")) {
                $str .= (string) fgets($this->inputHandle, 1024);
            }
            yield($str);
        }
    }

    private function prebuildDates(): array
    {
        $dates = [];
        $now = new DateTime();
        $currentYear = (int) $now->format("Y");

        for ($y = self::START_YEAR; $y <= $currentYear; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                for ($d = 1; $d <= 31; $d++) {
                    $dates[sprintf("%04d-%02d-%02d", $y, $m, $d)] = 0;
                }
            }
        }

        return $dates;
    }
}
