<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $inputStream = \fopen($inputPath, 'r');
        \stream_set_read_buffer($inputStream, 2 ** 16);
        \stream_set_chunk_size($inputStream, 2 ** 16);

        $date = new \DateTime('2021-01-01');
        $end = new \DateTime('2026-03-01');
        $day = new \DateInterval('P1D');
        $datesInit = [];
        while ($date < $end) {
            $datesInit[$date->format('Y-m-d')] = 0;
            $date->add($day);
        }

        $outputData = [];
        while (\count($outputData) < 268 && $line = \fgets($inputStream, 101)) {
            $outputData[\substr($line, 19, -27)] ??= $datesInit;
            $outputData[\substr($line, 19, -27)][\substr($line, -26, 10)]++;
        }

        while ($line = \fgets($inputStream, 101)) {
            $outputData[\substr($line, 19, -27)][\substr($line, -26, 10)]++;
        }

        \fclose($inputStream);

        $outputData = \array_map(\array_filter(...), $outputData);
        \file_put_contents($outputPath, \json_encode($outputData, \JSON_PRETTY_PRINT));
    }
}