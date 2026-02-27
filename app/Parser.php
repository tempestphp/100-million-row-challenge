<?php

declare(strict_types=1);

namespace App;

use Exception;
use Generator;
use function explode;
use function fclose;
use function feof;
use function fgets;
use function file_put_contents;
use function fopen;
use function fread;
use function json_encode;
use function str_ends_with;
use function substr;

final class Parser
{
    private const int READ_CHUNK_SIZE = 1024 * 1024 * 4;

    // "https://stitcher.io" is 19 chars long
    private const int URL_HOST_LEN = 19;

    // The last 15 characters of the timestamp is the time portion
    private const int TIMESTAMP_TIME_LEN = -15;

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
            $matches = [];
            ($this->inputHandle = fopen($inputPath, "r")) || throw new Exception("Couldn't open input file");
            foreach ($this->getChunk() as $chunk) {
                preg_match_all(";^https://stitcher.io(/[^,]+),(.{10});m", $chunk, $matches);
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
