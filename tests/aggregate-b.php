<?php


/**
 * Strategy B Optimization Benchmark
 *
 * Explores targeted micro-optimizations to the direct flat-array increment approach:
 *
 *  B0: Baseline B (two lookups + multiply)
 *  B1: Pre-shifted URL tokens  — eliminates the * DATE_COUNT multiply per row
 *  B2: Single combined flat key — one string-concat lookup vs two separate lookups
 *  B3: Persist $counts across chunks — eliminates array_fill(538K) per chunk
 *  B4: B1 + B3 combined
 *  B5: Pre-shifted + combined lookup (no multiply, one lookup)
 *  B6: B3 + unrolled (x4) inner loop
 *  B7: $counts as local var reference (avoid symbol table re-lookup on ++)
 *
 * Additionally isolates:
 *  - array_fill cost alone
 *  - array_fill vs array initialization alternatives
 */

declare(strict_types=1);

const URL_COUNT   = 268;
const DATE_COUNT  = 2008;
const ROW_COUNT   = 500_000;
const FLAT_SIZE   = URL_COUNT * DATE_COUNT; // 537,744

// ─────────────────────────────────────────────
// DATA GENERATION (same as main benchmark)
// ─────────────────────────────────────────────

function generateData(): array
{
    $urlSlugs = [];
    for ($i = 0; $i < URL_COUNT; $i++) {
        $len = rand(4, 30);
        $urlSlugs[$i] = substr(str_repeat(md5((string)$i), 3), 0, $len);
    }

    $dateStrings = [];
    $base = strtotime('2019-01-01');
    for ($i = 0; $i < DATE_COUNT; $i++) {
        $dateStrings[$i] = date('Y-m-d', $base + ($i * 86400));
    }

    $domainPrefix = str_pad('example.com/blog/', 25, '/');
    $tsRemainder  = str_pad(' 12:34:56.000  ', 15, ' ');

    $rows = [];
    for ($i = 0; $i < ROW_COUNT; $i++) {
        $slug = $urlSlugs[array_rand($urlSlugs)];
        $date = $dateStrings[array_rand($dateStrings)];
        $rows[] = $domainPrefix . $slug . ',' . $date . $tsRemainder;
    }

    return [
        'chunk'       => implode("\n", $rows) . "\n",
        'urlSlugs'    => $urlSlugs,
        'dateStrings' => $dateStrings,
    ];
}

function buildTables(array $urlSlugs, array $dateStrings): array
{
    // Standard tokens
    $urlTokens  = [];
    $dateTokens = [];
    foreach ($urlSlugs    as $id => $slug) $urlTokens[$slug]   = $id;
    foreach ($dateStrings as $id => $date) $dateTokens[$date]  = $id;

    // B1/B4: Pre-shifted — store id * DATE_COUNT so multiply is gone in hot path
    $urlTokensShifted = [];
    foreach ($urlSlugs as $id => $slug) $urlTokensShifted[$slug] = $id * DATE_COUNT;

    // B2: Single flat string-key combined lookup  "slug\0date" => flat_index
    // Using \0 as separator since it can't appear in slugs or dates
    $flatCombined = [];
    foreach ($urlSlugs as $uid => $slug) {
        foreach ($dateStrings as $did => $date) {
            $flatCombined[$slug . "\0" . $date] = $uid * DATE_COUNT + $did;
        }
    }

    // B5: Pre-shifted + combined — "slug\0date" => shifted_url_base + date_id
    // Same as flatCombined (value is already the final index), just confirming same table works
    // Actually B5 = flatCombined, just using urlShifted differently

    // dateChars for baseline A comparison
    $dateChars = [];
    foreach ($dateStrings as $id => $date) $dateChars[$date] = pack('v', $id);

    return compact('urlTokens', 'dateTokens', 'urlTokensShifted', 'flatCombined', 'dateChars');
}

// ─────────────────────────────────────────────
// STRATEGY A (baseline for reference)
// ─────────────────────────────────────────────
function strategyA(string $chunk, int $chunkEnd, array $urlTokens, array $dateChars): array
{
    $buckets = array_fill(0, URL_COUNT, '');
    $rowOffset = 0;

    while ($rowOffset < $chunkEnd) {
        $rowComma = strpos($chunk, ",", $rowOffset + 29);
        if ($rowComma === false || $rowComma + 26 > $chunkEnd) break;
        $buckets[$urlTokens[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)]]
            .= $dateChars[substr($chunk, $rowComma + 1, 10)];
        $rowOffset = $rowComma + 27;
    }

    $counts = array_fill(0, FLAT_SIZE, 0);
    for ($s = 0; $s < URL_COUNT; $s++) {
        if ($buckets[$s] === '') continue;
        $base = $s * DATE_COUNT;
        foreach (array_count_values(unpack('v*', $buckets[$s])) as $dateId => $count) {
            $counts[$base + $dateId] = $count;
        }
    }
    return $counts;
}

// ─────────────────────────────────────────────
// B0: Baseline B — two lookups + multiply
// ─────────────────────────────────────────────
function strategyB0(string $chunk, int $chunkEnd, array $urlTokens, array $dateTokens): array
{
    $counts = array_fill(0, FLAT_SIZE, 0);
    $rowOffset = 0;

    while ($rowOffset < $chunkEnd) {
        $rowComma = strpos($chunk, ",", $rowOffset + 29);
        if ($rowComma === false || $rowComma + 26 > $chunkEnd) break;
        $counts[$urlTokens[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)] * DATE_COUNT
        + $dateTokens[substr($chunk, $rowComma + 1, 10)]]++;
        $rowOffset = $rowComma + 27;
    }

    return $counts;
}

// ─────────────────────────────────────────────
// B1: Pre-shifted URL tokens — eliminates multiply
//     $urlTokensShifted[$slug] already = $uid * DATE_COUNT
// ─────────────────────────────────────────────
function strategyB1(string $chunk, int $chunkEnd, array $urlTokensShifted, array $dateTokens): array
{
    $counts = array_fill(0, FLAT_SIZE, 0);
    $rowOffset = 0;

    while ($rowOffset < $chunkEnd) {
        $rowComma = strpos($chunk, ",", $rowOffset + 29);
        if ($rowComma === false || $rowComma + 26 > $chunkEnd) break;
        $counts[$urlTokensShifted[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)]
        + $dateTokens[substr($chunk, $rowComma + 1, 10)]]++;
        $rowOffset = $rowComma + 27;
    }

    return $counts;
}

// ─────────────────────────────────────────────
// B2: Single combined string-key lookup
//     One hash lookup instead of two + an add
//     Key: "$slug\0$date"
// ─────────────────────────────────────────────
function strategyB2(string $chunk, int $chunkEnd, array $flatCombined): array
{
    $counts = array_fill(0, FLAT_SIZE, 0);
    $rowOffset = 0;

    while ($rowOffset < $chunkEnd) {
        $rowComma = strpos($chunk, ",", $rowOffset + 29);
        if ($rowComma === false || $rowComma + 26 > $chunkEnd) break;
        $counts[$flatCombined[
        substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)
        . "\0"
        . substr($chunk, $rowComma + 1, 10)
        ]]++;
        $rowOffset = $rowComma + 27;
    }

    return $counts;
}

// ─────────────────────────────────────────────
// B3: Persist $counts across chunks
//     Simulates multi-chunk processing — array_fill only once
//     (run N synthetic chunks against one pre-allocated array)
// ─────────────────────────────────────────────
function strategyB3_persistent(string $chunk, int $chunkEnd, array $urlTokens, array $dateTokens, array &$counts): void
{
    $rowOffset = 0;

    while ($rowOffset < $chunkEnd) {
        $rowComma = strpos($chunk, ",", $rowOffset + 29);
        if ($rowComma === false || $rowComma + 26 > $chunkEnd) break;
        $counts[$urlTokens[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)] * DATE_COUNT
        + $dateTokens[substr($chunk, $rowComma + 1, 10)]]++;
        $rowOffset = $rowComma + 27;
    }
}

// ─────────────────────────────────────────────
// B4: B1 + B3 — pre-shifted AND persistent counts
// ─────────────────────────────────────────────
function strategyB4_persistent(string $chunk, int $chunkEnd, array $urlTokensShifted, array $dateTokens, array &$counts): void
{
    $rowOffset = 0;

    while ($rowOffset < $chunkEnd) {
        $rowComma = strpos($chunk, ",", $rowOffset + 29);
        if ($rowComma === false || $rowComma + 26 > $chunkEnd) break;
        $counts[$urlTokensShifted[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)]
        + $dateTokens[substr($chunk, $rowComma + 1, 10)]]++;
        $rowOffset = $rowComma + 27;
    }
}

// ─────────────────────────────────────────────
// B6: Persistent + 4x unrolled
// ─────────────────────────────────────────────
function strategyB6_unrolled(string $chunk, int $chunkEnd, array $urlTokensShifted, array $dateTokens, array &$counts): void
{
    $fence = $chunkEnd - (4 * 99);
    $rowOffset = 0;

    while ($rowOffset < $fence) {
        $rowComma = strpos($chunk, ",", $rowOffset + 29);
        $counts[$urlTokensShifted[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)] + $dateTokens[substr($chunk, $rowComma + 1, 10)]]++;
        $rowOffset = $rowComma + 27;

        $rowComma = strpos($chunk, ",", $rowOffset + 29);
        $counts[$urlTokensShifted[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)] + $dateTokens[substr($chunk, $rowComma + 1, 10)]]++;
        $rowOffset = $rowComma + 27;

        $rowComma = strpos($chunk, ",", $rowOffset + 29);
        $counts[$urlTokensShifted[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)] + $dateTokens[substr($chunk, $rowComma + 1, 10)]]++;
        $rowOffset = $rowComma + 27;

        $rowComma = strpos($chunk, ",", $rowOffset + 29);
        $counts[$urlTokensShifted[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)] + $dateTokens[substr($chunk, $rowComma + 1, 10)]]++;
        $rowOffset = $rowComma + 27;
    }

    while ($rowOffset < $chunkEnd) {
        $rowComma = strpos($chunk, ",", $rowOffset + 29);
        if ($rowComma === false || $rowComma + 26 > $chunkEnd) break;
        $counts[$urlTokensShifted[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)] + $dateTokens[substr($chunk, $rowComma + 1, 10)]]++;
        $rowOffset = $rowComma + 27;
    }
}

// ─────────────────────────────────────────────
// RUNNER
// ─────────────────────────────────────────────

function bench(string $label, callable $fn, int $iterations = 7): void
{
    $fn(); // warmup

    $times = [];
    for ($i = 0; $i < $iterations; $i++) {
        $t = hrtime(true);
        $fn();
        $times[] = (hrtime(true) - $t) / 1e6;
    }

    sort($times);
    $min = $times[0];
    $med = $times[(int)($iterations / 2)];
    $p75 = $times[(int)($iterations * 0.75)];

    printf("%-58s  min=%7.2fms  p50=%7.2fms  p75=%7.2fms\n", $label, $min, $med, $p75);
}

// ─────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────

echo "Generating data (" . number_format(ROW_COUNT) . " rows)...\n";
$data   = generateData();
$tables = buildTables($data['urlSlugs'], $data['dateStrings']);

$chunk    = $data['chunk'];
$chunkEnd = strlen($chunk);

echo "Chunk: " . number_format($chunkEnd / 1024 / 1024, 2) . " MB | Flat array: "
    . number_format(FLAT_SIZE) . " cells (" . number_format(FLAT_SIZE * 8 / 1024 / 1024, 2) . " MB)\n\n";

// ── Correctness check ──────────────────────────────────────
echo "Verifying correctness...\n";
$ref = strategyA($chunk, $chunkEnd, $tables['urlTokens'], $tables['dateChars']);
$refTotal = array_sum($ref);

$checks = [
    'B0' => strategyB0($chunk, $chunkEnd, $tables['urlTokens'], $tables['dateTokens']),
    'B1' => strategyB1($chunk, $chunkEnd, $tables['urlTokensShifted'], $tables['dateTokens']),
    'B2' => strategyB2($chunk, $chunkEnd, $tables['flatCombined']),
];
foreach ($checks as $name => $result) {
    $t = array_sum($result);
    printf("  %s: %s visits %s\n", $name, number_format($t), $t === $refTotal ? '✓' : '✗ MISMATCH');
}

// B3/B4/B6 persistent: accumulate into a fresh array, check sum
$b3counts = array_fill(0, FLAT_SIZE, 0);
strategyB3_persistent($chunk, $chunkEnd, $tables['urlTokens'], $tables['dateTokens'], $b3counts);
printf("  B3: %s visits %s\n", number_format(array_sum($b3counts)), array_sum($b3counts) === $refTotal ? '✓' : '✗ MISMATCH');

$b4counts = array_fill(0, FLAT_SIZE, 0);
strategyB4_persistent($chunk, $chunkEnd, $tables['urlTokensShifted'], $tables['dateTokens'], $b4counts);
printf("  B4: %s visits %s\n", number_format(array_sum($b4counts)), array_sum($b4counts) === $refTotal ? '✓' : '✗ MISMATCH');

$b6counts = array_fill(0, FLAT_SIZE, 0);
strategyB6_unrolled($chunk, $chunkEnd, $tables['urlTokensShifted'], $tables['dateTokens'], $b6counts);
printf("  B6: %s visits %s\n", number_format(array_sum($b6counts)), array_sum($b6counts) === $refTotal ? '✓' : '✗ MISMATCH');

echo "\n";
echo str_repeat('─', 100) . "\n";
echo "BENCHMARK — B VARIANTS (7 iterations, sorted, reporting min/p50/p75)\n";
echo str_repeat('─', 100) . "\n";

bench('A:  Current (string buffer + unpack) [reference]',
    fn() => strategyA($chunk, $chunkEnd, $tables['urlTokens'], $tables['dateChars']));

bench('B0: Direct flat array (two lookups + multiply)',
    fn() => strategyB0($chunk, $chunkEnd, $tables['urlTokens'], $tables['dateTokens']));

bench('B1: Pre-shifted URL tokens (two lookups + add, no multiply)',
    fn() => strategyB1($chunk, $chunkEnd, $tables['urlTokensShifted'], $tables['dateTokens']));

bench('B2: Single combined key lookup (one lookup, string concat)',
    fn() => strategyB2($chunk, $chunkEnd, $tables['flatCombined']));

echo str_repeat('─', 100) . "\n";
echo "Persistent variants (array_fill cost eliminated — closest to real multi-chunk worker):\n";
echo str_repeat('─', 100) . "\n";

bench('B3: Persistent counts + two lookups + multiply', function() use ($chunk, $chunkEnd, $tables) {
    $counts = array_fill(0, FLAT_SIZE, 0);
    strategyB3_persistent($chunk, $chunkEnd, $tables['urlTokens'], $tables['dateTokens'], $counts);
});

bench('B4: Persistent + pre-shifted (two lookups + add)', function() use ($chunk, $chunkEnd, $tables) {
    $counts = array_fill(0, FLAT_SIZE, 0);
    strategyB4_persistent($chunk, $chunkEnd, $tables['urlTokensShifted'], $tables['dateTokens'], $counts);
});

bench('B6: Persistent + pre-shifted + 4x unrolled', function() use ($chunk, $chunkEnd, $tables) {
    $counts = array_fill(0, FLAT_SIZE, 0);
    strategyB6_unrolled($chunk, $chunkEnd, $tables['urlTokensShifted'], $tables['dateTokens'], $counts);
});

echo str_repeat('─', 100) . "\n";

// ── Isolate array_fill cost ────────────────────────────────
echo "\nIsolating array_fill(0, " . number_format(FLAT_SIZE) . ", 0) cost alone:\n";
bench('array_fill(0, FLAT_SIZE, 0) only',
    fn() => array_fill(0, FLAT_SIZE, 0));

// Compare with alternative init strategies
bench('array_fill(0, FLAT_SIZE, 0) + unset (fresh hash)',
    function() {
        $a = array_fill(0, FLAT_SIZE, 0);
        unset($a);
        return array_fill(0, FLAT_SIZE, 0);
    });

echo str_repeat('─', 100) . "\n";
echo "\nDone.\n";