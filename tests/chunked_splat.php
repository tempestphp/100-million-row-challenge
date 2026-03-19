<?php

/**
 * Benchmark: pack() full splat vs chunked splat
 *
 * Simulates the counts array from WorkerTokenizedSocketV2Trait
 * using real dimensions: 268 URLs × 2011 dates = 538,948 elements
 */

gc_disable();

const URL_COUNT  = 268;
const DATE_COUNT = 2011;
const ITERATIONS = 20;
const CHUNK_SIZE = 4096;

// ------------------------------------------------------------
// Build a realistic counts array — sparse-ish, like real data
// ------------------------------------------------------------
$total = URL_COUNT * DATE_COUNT;
$counts = array_fill(0, $total, 0);

// Seed roughly 30% of entries with non-zero values (adjust to taste)
mt_srand(42);
$nonZero = (int)($total * 0.30);
for ($i = 0; $i < $nonZero; $i++) {
    $counts[mt_rand(0, $total - 1)] = mt_rand(1, 1000);
}

$maxVal = max($counts);
$v16    = $maxVal <= 65535;
$fmt    = $v16 ? 'v*' : 'V*';

echo "---------------------------------------------\n";
echo "PHP version  : " . PHP_VERSION . "\n";
echo "Total elements: " . number_format($total) . "\n";
echo "Non-zero entries (approx): " . number_format($nonZero) . "\n";
echo "Format: " . ($v16 ? 'v* (uint16)' : 'V* (uint32)') . "\n";
echo "Iterations: " . ITERATIONS . "\n";
echo "Chunk size: " . number_format(CHUNK_SIZE) . "\n";
echo "---------------------------------------------\n\n";

// ------------------------------------------------------------
// Method 1: Full splat
// ------------------------------------------------------------
$fullSplatTimes = [];

for ($iter = 0; $iter < ITERATIONS; $iter++) {
    $t1 = hrtime(true);
    $packed = pack($fmt, ...$counts);
    $t2 = hrtime(true);

    $fullSplatTimes[] = ($t2 - $t1) / 1e6; // ms
    unset($packed);
}

// ------------------------------------------------------------
// Method 2: Chunked splat
// ------------------------------------------------------------
$chunkedTimes = [];

for ($iter = 0; $iter < ITERATIONS; $iter++) {
    $t1 = hrtime(true);
    $packed = '';
    for ($i = 0; $i < $total; $i += CHUNK_SIZE) {
        $packed .= pack($fmt, ...array_slice($counts, $i, CHUNK_SIZE));
    }
    $t2 = hrtime(true);

    $chunkedTimes[] = ($t2 - $t1) / 1e6;
    unset($packed);
}

// ------------------------------------------------------------
// Method 3: Chunked splat with array_chunk (alternative)
// ------------------------------------------------------------
$chunkedAltTimes = [];

for ($iter = 0; $iter < ITERATIONS; $iter++) {
    $t1 = hrtime(true);
    $packed = '';
    foreach (array_chunk($counts, CHUNK_SIZE) as $chunk) {
        $packed .= pack($fmt, ...$chunk);
    }
    $t2 = hrtime(true);

    $chunkedAltTimes[] = ($t2 - $t1) / 1e6;
    unset($packed);
}

// ------------------------------------------------------------
// Results
// ------------------------------------------------------------
function stats(array $times): array
{
    $count = count($times);
    $sum   = array_sum($times);
    $mean  = $sum / $count;
    $min   = min($times);
    $max   = max($times);

    // Drop best and worst, recalculate trimmed mean
    sort($times);
    $trimmed     = array_slice($times, 1, $count - 2);
    $trimmedMean = array_sum($trimmed) / count($trimmed);

    // Std deviation
    $variance = array_sum(array_map(fn($t) => ($t - $mean) ** 2, $times)) / $count;
    $stddev   = sqrt($variance);

    return compact('mean', 'trimmedMean', 'min', 'max', 'stddev');
}

function printStats(string $label, array $times): void
{
    $s = stats($times);
    printf(
        "%-30s  mean=%6.2fms  trimmed=%6.2fms  min=%6.2fms  max=%6.2fms  stddev=%5.2fms\n",
        $label,
        $s['mean'],
        $s['trimmedMean'],
        $s['min'],
        $s['max'],
        $s['stddev']
    );
}

echo "Results (lower is better):\n";
echo str_repeat('-', 95) . "\n";
printStats('Full splat',              $fullSplatTimes);
printStats('Chunked splat (for loop)',  $chunkedTimes);
printStats('Chunked splat (array_chunk)', $chunkedAltTimes);
echo str_repeat('-', 95) . "\n\n";

// Winner
$fullMean    = stats($fullSplatTimes)['trimmedMean'];
$chunkedMean = min(
    stats($chunkedTimes)['trimmedMean'],
    stats($chunkedAltTimes)['trimmedMean']
);

if ($fullMean < $chunkedMean) {
    printf("Winner: Full splat  (%.1f%% faster than best chunked)\n",
        (($chunkedMean - $fullMean) / $chunkedMean) * 100);
} else {
    printf("Winner: Chunked splat  (%.1f%% faster than full splat)\n",
        (($fullMean - $chunkedMean) / $fullMean) * 100);
}

echo "\nRaw times (ms):\n";
printf("  %-6s  %10s  %10s  %10s\n", 'Iter', 'Full splat', 'Chunked for', 'Chunked chunk');
for ($i = 0; $i < ITERATIONS; $i++) {
    printf("  %-6d  %10.2f  %10.2f  %10.2f\n",
        $i + 1,
        $fullSplatTimes[$i],
        $chunkedTimes[$i],
        $chunkedAltTimes[$i]
    );
}