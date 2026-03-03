<?php

namespace App;

final class Parser
{
    public function parse($inputPath, $outputPath)
    {
        \gc_disable();

        \preg_match_all('(^https://[^/]*(/.*),([0-9]{4}-[0-9]{2}-[0-9]{2}))m', \file_get_contents($inputPath), $matches, \PREG_SET_ORDER);
        $results = [];
        foreach ($matches as $match) {
            $results[$match[1]][] = $match[2];
        }
        foreach ($results as &$result) {
            $result = \array_count_values($result);
            \ksort($result, \SORT_STRING);
        }

        \file_put_contents($outputPath, \json_encode($results, \JSON_PRETTY_PRINT));
        \gc_enable();
    }
}
