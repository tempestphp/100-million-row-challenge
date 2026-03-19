<?php

/**
 * Benchmark: Alternatives to pack() from SplFixedArray
 *
 * The problem: pack($fmt, ...$splFixedArray) is slow because PHP must
 * go through the Iterator protocol for each element rather than
 * accessing a C array directly.
 *
 * Candidates:
 *   1. Regular array + pack()                   — baseline
 *   2. SplFixedArray + pack()                   — the slow one we already know
 *   3. SplFixedArray → toArray() → pack()       — pay conversion cost, then fast pack
 *   4. SplFixedArray → iterator_to_array() → pack()
 *   5. Pre-allocated binary string, patch with chr() during writes — skip pack() entirely
 *   6. Pre-allocated binary string, patch with pack('v', $val) per write
 */

const URL_COUNT   = 268;
const DATE_COUNT  = 2011;
const ITERATIONS  = 20;

$total = URL_COUNT * DATE_COUNT; // 538,948

mt_srand(42);
$writePairs = [];
$writeCount = (int)($total * 0.30);
for ($i = 0; $i < $writeCount; $i++) {
    $writePairs[] = [mt_rand(0, $total - 1), mt_rand(1, 1000)];
}

$fmt = 'v*';

echo "---------------------------------------------\n";
echo "PHP version    : " . PHP_VERSION . "\n";
echo "Total elements : " . number_format($total) . "\n";
echo "Write pairs    : " . number_format($writeCount) . " (~30% fill)\n";
echo "Iterations     : " . ITERATIONS . "\n";
echo "---------------------------------------------\n\n";

// ------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------
function stats(array $times): array
{
    $count = count($times);
    $mean  = array_sum($times) / $count;
    $min   = min($times);
    $max   = max($times);

    sort($times);
    $trimmed     = array_slice($times, 1, $count - 2);
    $trimmedMean = array_sum($trimmed) / count($trimmed);

    $variance = array_sum(array_map(fn($t) => ($t - $mean) ** 2, $times)) / $count;
    $stddev   = sqrt($variance);

    return compact('mean', 'trimmedMean', 'min', 'max', 'stddev');
}

function printStats(string $label, array $times): void
{
    $s = stats($times);
    printf(
        "  %-42s  trimmed=%7.3fms  mean=%7.3fms  min=%7.3fms  max=%7.3fms  stddev=%6.3fms\n",
        $label,
        $s['trimmedMean'], $s['mean'], $s['min'], $s['max'], $s['stddev']
    );
}

// ------------------------------------------------------------------
// Run a full cycle benchmark: init + writes + pack/binary-build
// Returns array of times.
// ------------------------------------------------------------------

// 1. Baseline: regular array + pack()
$times1 = [];
for ($iter = 0; $iter < ITERATIONS; $iter++) {
    $t1 = hrtime(true);
    $counts = array_fill(0, $total, 0);
    foreach ($writePairs as [$pos, $val]) {
        $counts[$pos] = $val;
    }
    $packed = pack($fmt, ...$counts);
    $t2 = hrtime(true);
    $times1[] = ($t2 - $t1) / 1e6;
    unset($counts, $packed);
}

// 2. SplFixedArray + pack() — the slow path
$times2 = [];
for ($iter = 0; $iter < ITERATIONS; $iter++) {
    $t1 = hrtime(true);
    $counts = new SplFixedArray($total);
    foreach ($writePairs as [$pos, $val]) {
        $counts[$pos] = $val;
    }
    $packed = pack($fmt, ...$counts);
    $t2 = hrtime(true);
    $times2[] = ($t2 - $t1) / 1e6;
    unset($counts, $packed);
}

// 3. SplFixedArray → toArray() → pack()
$times3 = [];
for ($iter = 0; $iter < ITERATIONS; $iter++) {
    $t1 = hrtime(true);
    $counts = new SplFixedArray($total);
    foreach ($writePairs as [$pos, $val]) {
        $counts[$pos] = $val;
    }
    $packed = pack($fmt, ...$counts->toArray());
    $t2 = hrtime(true);
    $times3[] = ($t2 - $t1) / 1e6;
    unset($counts, $packed);
}

// 4. SplFixedArray → iterator_to_array() → pack()
$times4 = [];
for ($iter = 0; $iter < ITERATIONS; $iter++) {
    $t1 = hrtime(true);
    $counts = new SplFixedArray($total);
    foreach ($writePairs as [$pos, $val]) {
        $counts[$pos] = $val;
    }
    $packed = pack($fmt, ...iterator_to_array($counts));
    $t2 = hrtime(true);
    $times4[] = ($t2 - $t1) / 1e6;
    unset($counts, $packed);
}

// 5. Pre-allocated binary string, patch with chr() during writes — no pack() at all
//    Writes little-endian uint16 directly: low byte, high byte
$times5 = [];
for ($iter = 0; $iter < ITERATIONS; $iter++) {
    $t1 = hrtime(true);
    $out = str_repeat("\x00", $total * 2);
    foreach ($writePairs as [$pos, $val]) {
        $i        = $pos * 2;
        $out[$i]     = chr($val & 0xFF);
        $out[$i + 1] = chr($val >> 8);
    }
    $t2 = hrtime(true);
    $times5[] = ($t2 - $t1) / 1e6;
    unset($out);
}

// 6. Pre-allocated binary string, patch with pack('v', $val) per write
$times6 = [];
for ($iter = 0; $iter < ITERATIONS; $iter++) {
    $t1 = hrtime(true);
    $out = str_repeat("\x00", $total * 2);
    foreach ($writePairs as [$pos, $val]) {
        $word = pack('v', $val);
        $out[$pos * 2]     = $word[0];
        $out[$pos * 2 + 1] = $word[1];
    }
    $t2 = hrtime(true);
    $times6[] = ($t2 - $t1) / 1e6;
    unset($out);
}

// ------------------------------------------------------------------
// Results
// ------------------------------------------------------------------
echo "Full cycle results (init + writes + serialize), lower is better:\n";
echo str_repeat('-', 105) . "\n";
printStats('1. array_fill + pack() [baseline]',          $times1);
printStats('2. SplFixedArray + pack()',                   $times2);
printStats('3. SplFixedArray + toArray() + pack()',       $times3);
printStats('4. SplFixedArray + iterator_to_array() + pack()', $times4);
printStats('5. Binary string + chr() patch [no pack()]', $times5);
printStats('6. Binary string + pack(v) patch [no pack()]', $times6);
echo str_repeat('-', 105) . "\n\n";

// Rank them
$all = [
    '1. array_fill + pack()'                    => stats($times1)['trimmedMean'],
    '2. SplFixedArray + pack()'                 => stats($times2)['trimmedMean'],
    '3. SplFixedArray + toArray() + pack()'     => stats($times3)['trimmedMean'],
    '4. SplFixedArray + iterator_to_array()'    => stats($times4)['trimmedMean'],
    '5. Binary string + chr() patch'            => stats($times5)['trimmedMean'],
    '6. Binary string + pack(v) patch'          => stats($times6)['trimmedMean'],
];

asort($all);
echo "Ranking (fastest to slowest):\n";
$rank = 1;
$best = null;
foreach ($all as $label => $mean) {
    if ($best === null) $best = $mean;
    $pct = (($mean - $best) / $best) * 100;
    printf("  %d. %-42s %.3fms  %s\n",
        $rank++, $label, $mean,
        $best === $mean ? '← fastest' : sprintf('+%.1f%%', $pct)
    );
}

echo "\nRaw times (ms):\n";
printf("  %-4s  %10s  %10s  %10s  %10s  %10s  %10s\n",
    'Iter', 'arr+pack', 'spl+pack', 'toArray', 'iter2arr', 'chr patch', 'pack patch');
for ($i = 0; $i < ITERATIONS; $i++) {
    printf("  %-4d  %10.3f  %10.3f  %10.3f  %10.3f  %10.3f  %10.3f\n",
        $i + 1,
        $times1[$i], $times2[$i], $times3[$i],
        $times4[$i], $times5[$i], $times6[$i]
    );
}