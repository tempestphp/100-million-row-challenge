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
    private const int READ_CHUNK_SIZE = 1024 * 1024 * 16;

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
        try {
            $toEncode = [];
            ($this->inputHandle = fopen($inputPath, "r")) || throw new Exception("Couldn't open input file");
            foreach ($this->getChunk() as $chunk) {
                /** @var string $row */
                foreach($chunk as $row) {
                    if(!$row) {
                        continue;
                    }
                    $parts = explode(",", substr($row, self::URL_HOST_LEN, self::TIMESTAMP_TIME_LEN));
                    if (!isset($toEncode[$parts[0]])) {
                        $toEncode[$parts[0]] = [$parts[1] => 0];
                    }
                    if (!isset($toEncode[$parts[0]][$parts[1]])) {
                        $toEncode[$parts[0]][$parts[1]] = 0;
                    }

                    $toEncode[$parts[0]][$parts[1]]++;
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
     * @return Generator<array<int, string>>
     */
    private function getChunk(): Generator
    {
        while(!feof($this->inputHandle)) {
            $str = fread($this->inputHandle, self::READ_CHUNK_SIZE);
            if (!str_ends_with($str, "\n")) {
                $str .= (string) fgets($this->inputHandle, 1024);
            }
            yield explode("\n", $str);
        }
    }
}
