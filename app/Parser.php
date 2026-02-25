<?php

namespace App;

use const JSON_PRETTY_PRINT;
use const JSON_THROW_ON_ERROR;

final class Parser
{
    public array $data = [];
    private int $buffer = 64 * 1024; // 64 KB — benchmark winner (data/buffer_benchmark.csv)

    public function setBuffer(int $buffer): self
    {
        if ($buffer > 0) {
            $this->buffer = $buffer;
        }

        return $this;
    }

    public function parse(string $inputPath, string $outputPath): void
    {
        $handle = fopen($inputPath, 'r');

        $data = [];

        $toSeek = 0;
        while (!feof($handle)) {
            if ($toSeek) {
                fseek($handle, $toSeek);
            }

            $content = fread($handle, $this->buffer);
            $curSeek = ftell($handle);

            $posLastNewline = mb_strrpos($content, "\n");
            $posDiffFromLastNewline = $curSeek - $toSeek - $posLastNewline;

            $content = mb_substr($content, 0, -1 * $posDiffFromLastNewline);
            $toSeek = $curSeek - $posDiffFromLastNewline + 1;

            $lines = explode("\n", $content);
            foreach ($lines as $line) {
                $splits = explode(',', mb_trim(mb_substr(mb_substr($line, 19), 0, -15)));

                if (!isset($data[$splits[0]][$splits[1]])) {
                    $data[$splits[0]][$splits[1]] = 0;
                }

                ++$data[$splits[0]][$splits[1]];
            }
        }

        foreach ($data as &$item) {
            ksort($item);
        }

        file_put_contents($outputPath, json_encode($data, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT));
    }
}
