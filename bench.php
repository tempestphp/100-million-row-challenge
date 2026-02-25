<?php

require_once __DIR__ . '/vendor/autoload.php';

use App\Parser;

$inputPath = $argv[1] ?? __DIR__ . '/data/data.csv';
$outputPath = __DIR__ . '/data/bench-output.json';
$runs = (int)($argv[2] ?? 3);

$times = [];
$parser = new Parser();

for ($i = 0; $i < $runs; $i++) {
    $start = hrtime(true);
    $parser->parse($inputPath, $outputPath);
    $elapsed = (hrtime(true) - $start) / 1e9;
    $times[] = $elapsed;
    fprintf(STDERR, "Run %d: %.4fs\n", $i + 1, $elapsed);
}

sort($times);
$min = $times[0];
$max = $times[count($times) - 1];
$avg = array_sum($times) / count($times);
$median = $times[(int)(count($times) / 2)];

fprintf(STDERR, "\nResults (%d runs):\n", $runs);
fprintf(STDERR, "  Min:    %.4fs\n", $min);
fprintf(STDERR, "  Median: %.4fs\n", $median);
fprintf(STDERR, "  Avg:    %.4fs\n", $avg);
fprintf(STDERR, "  Max:    %.4fs\n", $max);
