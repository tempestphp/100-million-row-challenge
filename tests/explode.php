<?php

const CHUNK_SIZE = 128 * 1024 * 1024;
const ITERATIONS = 5;
const BLOG_PREFIX_LEN = 25; // strlen('https://stitcher.io/blog/')

function load_chunk(string $filepath): string {
    $fh = fopen($filepath, 'rb');
    $chunk = fread($fh, CHUNK_SIZE);
    fclose($fh);

    // Trim to last newline so both methods work on identical complete lines
    $lastNl = strrpos($chunk, "\n");
    return substr($chunk, 0, $lastNl + 1);
}

function bench_strpos(string $window): array {
    $tallies = [];
    $windowLen = strlen($window);
    $lastNl = strrpos($window, "\n");

    $o = 0;
    $windowEnd = $lastNl;
    $minLineLen = 40; // tune to your shortest possible line

    $fence = $windowEnd - (5 * 99);

    while ($o < $fence) {

        $wEnd = strpos($window, "\n", $o + 35);
        $slug = substr($window, $o + 25, $wEnd - $o - 51);
        $date = substr($window, $o - 25, 10);
        isset($tallies[$slug][$date]) ? $tallies[$slug][$date]++ : $tallies[$slug][$date] = 1;
        $o = $wEnd + 1;

        $wEnd = strpos($window, "\n", $o + 35);
        $slug = substr($window, $o + 25, $wEnd - $o - 51);
        $date = substr($window, $o - 25, 10);
        isset($tallies[$slug][$date]) ? $tallies[$slug][$date]++ : $tallies[$slug][$date] = 1;
        $o = $wEnd + 1;

        $wEnd = strpos($window, "\n", $o + 35);
        $slug = substr($window, $o + 25, $wEnd - $o - 51);
        $date = substr($window, $o - 25, 10);
        isset($tallies[$slug][$date]) ? $tallies[$slug][$date]++ : $tallies[$slug][$date] = 1;
        $o = $wEnd + 1;

        $wEnd = strpos($window, "\n", $o + 35);
        $slug = substr($window, $o + 25, $wEnd - $o - 51);
        $date = substr($window, $o - 25, 10);
        isset($tallies[$slug][$date]) ? $tallies[$slug][$date]++ : $tallies[$slug][$date] = 1;
        $o = $wEnd + 1;

        $wEnd = strpos($window, "\n", $o + 35);
        $slug = substr($window, $o + 25, $wEnd - $o - 51);
        $date = substr($window, $o - 25, 10);
        isset($tallies[$slug][$date]) ? $tallies[$slug][$date]++ : $tallies[$slug][$date] = 1;
        $o = $wEnd + 1;

    }

    while ($o < $windowLen) {
        $wEnd = strpos($window, "\n", $o + 35);
        if ($wEnd === false || $wEnd > $windowEnd) break;
        $slug = substr($window, $o + 25, $wEnd - $o - 51);
        $date = substr($window, $o - 25, 10);
        isset($tallies[$slug][$date]) ? $tallies[$slug][$date]++ : $tallies[$slug][$date] = 1;
        $o = $wEnd + 1;
    }

    return $tallies;
}

function bench_explode(string $chunk): array {
    $tallies = [];
    $lines = explode("\n", $chunk);
    $count = count($lines) - 1; // last element will be empty due to trailing newline

    for ($i = 0; $i < $count; $i++) {
        $line = $lines[$i];
        $nl = strlen($line);

        $slug = substr($line, BLOG_PREFIX_LEN, $nl - 26 - BLOG_PREFIX_LEN);
        $date = substr($line, $nl - 25, 10);

        isset($tallies[$slug][$date]) ? $tallies[$slug][$date]++ : $tallies[$slug][$date] = 1;
    }

    return $tallies;
}

function bench_preg(string $chunk): array {
    $tallies = [];
    preg_match_all('/blog\/([^,]+),(\d{4}-\d{2}-\d{2})/', $chunk, $m);

    $slugs = $m[1];
    $dates = $m[2];
    $count = count($slugs);

    for ($i = 0; $i < $count; $i++) {
        isset($tallies[$slugs[$i]][$dates[$i]])
            ? $tallies[$slugs[$i]][$dates[$i]]++
            : $tallies[$slugs[$i]][$dates[$i]] = 1;
    }

    return $tallies;
}

function run_bench(string $name, callable $fn, string $chunk): void {
    $times = [];
    $lineCount = 0;

    for ($i = 0; $i < ITERATIONS; $i++) {
        $start = hrtime(true);
        $result = $fn($chunk);
        $elapsed = hrtime(true) - $start;
        $times[] = $elapsed;

        if ($i === 0) {
            $lineCount = array_sum(array_map('array_sum', $result));
        }
    }

    $avgNs  = array_sum($times) / ITERATIONS;
    $minNs  = min($times);
    $maxNs  = max($times);

    printf(
        "%-12s | lines: %8d | avg: %7.1fms | min: %7.1fms | max: %7.1fms\n",
        $name,
        $lineCount,
        $avgNs / 1e6,
        $minNs / 1e6,
        $maxNs / 1e6,
    );
}

// --- Main ---

$filepath = __DIR__ . '/../data/data.csv';

echo "Loading chunk (up to " . (CHUNK_SIZE / 1024 / 1024) . "MB)...\n";
$chunk = load_chunk($filepath);
$actualMb = round(strlen($chunk) / 1024 / 1024, 2);
echo "Loaded {$actualMb}MB, running " . ITERATIONS . " iterations each.\n\n";

// Warm the CPU caches / opcode cache with a throwaway run
bench_strpos($chunk);
bench_explode($chunk);
bench_preg($chunk);

echo str_repeat('-', 75) . "\n";

run_bench('strpos', 'bench_strpos', $chunk);
run_bench('explode', 'bench_explode', $chunk);
run_bench('preg', 'bench_preg', $chunk);