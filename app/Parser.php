<?php

namespace App;

use SplFileObject;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $result = [];

        $file = new SplFileObject($inputPath);
        while (!$file->eof()) {
            if (preg_match('/^https:\/\/stitcher.io(.*),(.{10}).*$/', $file->current(), $matches)) {
                $result[$matches[1]] ??= [];
                $result[$matches[1]][$matches[2]] =
                    ($result[$matches[1]][$matches[2]] ?? 0) + 1;
            }

            $file->next();
        }

        foreach ($result as &$row) {
            ksort($row);
        }

        file_put_contents($outputPath, json_encode($result, JSON_PRETTY_PRINT));
    }
}