<?php

declare(strict_types=1);

namespace App;

use Exception;
use Generator;
use function count;
use function fclose;
use function feof;
use function fgets;
use function file_put_contents;
use function fopen;
use function fread;
use function json_encode;
use function preg_match_all;
use function str_ends_with;

final class Parser
{
    private const int READ_CHUNK_SIZE = 1024 * 1024 * 4;

    private const string REGEX_LINE_PARSER = ";^https://stitcher.io(/[^,]+),(.{10});";
    /**
     * @var false|resource
     */
    private $inputHandle;

    /**
     * @throws Exception
     */
    public function parse(string $inputPath, string $outputPath): void
    {
        \gc_disable();
        try {
            $toEncode = [];
            ($this->inputHandle = fopen($inputPath, "r")) || throw new Exception("Couldn't open input file");
            foreach ($this->getChunk() as $chunk) {
                $matches = [];
                preg_match_all(self::REGEX_LINE_PARSER, $chunk, $matches);
                $matchCount = count($matches[1]);

                for ($key = 0; $key < $matchCount; $key++) {
                    $path = $matches[1][$key];
                    $date = $matches[2][$key];

                    if (!isset($toEncode[$path])) {
                        $toEncode[$path] = [$date => 0];
                    }
                    if (!isset($toEncode[$path][$date])) {
                        $toEncode[$path][$date] = 0;
                    }

                    $toEncode[$path][$date]++;
                }
            }

            foreach($toEncode as &$dates) {
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
}
