<?php

declare(strict_types=1);

namespace App;

use Exception;
use Generator;
use function explode;
use function fclose;
use function feof;
use function fgets;
use function fopen;
use function fread;
use function str_ends_with;

final class Parser
{
    private const int READ_CHUNK_SIZE = 1024 * 1024 * 16;

    // "https://stitcher.io" is 19 chars long
    private const int URL_HOST_LEN = 19;
    /**
     * @var false|resource
     */
    private $inputHandle;

    /**
     * @throws Exception
     */
    public function parse(string $inputPath, string $outputPath): void
    {
        /** @var resource $inputHandle */
        try {
            $data = [];
            ($this->inputHandle = fopen($inputPath, "r")) || throw new Exception("Couldn't open input file");
            foreach ($this->getChunk() as $chunk) {
                foreach($chunk as $row) {
                    if(!$row) {
                        continue;
                    }
//                    $separatorPos = \strpos($row, ",", self::URL_HOST_LEN);
//                    $path = substr($row, self::URL_HOST_LEN, $separatorPos - self::URL_HOST_LEN);
//                    $date = substr($row, $separatorPos + 1, 10);
                    $parts = explode(",", $row);
//                    print_r([$row, $path, $date]);
                }
            }
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
