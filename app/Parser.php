<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $in = \file_get_contents($inputPath);
        \preg_match_all('@^(?:https://stitcher\.io)(.+?),(202[\d:+-]+)(?:T.+)$@m', $in, $matches, \PREG_SET_ORDER);

        $data = [];

        foreach ($matches as $match) {
            $path = $match[1];
            $date = $match[2]; // list / [] is somehow slower
            $data[$path][$date] = ($data[$path][$date] ?? 0) + 1;
        }

        foreach ($data as &$dates) {
            \ksort($dates);
        }

        \file_put_contents($outputPath, \json_encode($data, \JSON_PRETTY_PRINT));
    }
}
