<?php

namespace App\Traits;

trait WriterNaiveTrait {
    protected function write(): void
    {
        // Hoist class variables
        $urls = $this->data;
        $outputPath = $this->outputPath;

        $this->naiveSorter($urls);

        $outFile = fopen($outputPath, 'w');
        fwrite($outFile, json_encode($urls, JSON_PRETTY_PRINT));
        fclose($outFile);
    }

    protected function naiveSorter(&$urls): void
    {
        foreach ($urls as $url => $dates) {
            ksort($urls[$url]);
        }
    }
}