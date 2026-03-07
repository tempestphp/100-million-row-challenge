<?php

/**
 * Aggregation Strategy Benchmark
 * Tests different approaches for tallying (URL, date) visit counts.
 *
 * Row format: [25-char domain prefix][url_slug],[date 10 chars][15-char timestamp remainder]\n
 * Total row width: 25 + slug_len + 1 + 25 + 1 = variable, but comma always at slug end,
 * and next row starts 27 bytes after comma (10 date + 15 remainder + 1 newline + 1 comma).
 */

declare(strict_types=1);

// ─────────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────────
const URL_COUNT  = 268;
const DATE_COUNT = 2008;
const ROW_COUNT  = 500_000;   // rows in synthetic test file
const READ_BUFFER = 1 << 22;  // 4MB, match your real worker
const DOMAIN_PREFIX_LEN = 25;
const TIMESTAMP_REMAINDER = 15; // chars after the 10-char date before \n

// ─────────────────────────────────────────────
// SYNTHETIC DATA GENERATION
// ─────────────────────────────────────────────

function generateData(int $urlCount, int $dateCount, int $rowCount): array
{
    // Build URL slugs (variable length 4–30 chars, like real slugs)
    $urlSlugs = [];
    for ($i = 0; $i < $urlCount; $i++) {
        $len = rand(4, 30);
        $urlSlugs[$i] = substr(str_repeat(md5((string)$i), 3), 0, $len);
    }

    // Build date strings (10 chars, YYYY-MM-DD format)
    $dateStrings = [];
    $base = strtotime('2019-01-01');
    for ($i = 0; $i < $dateCount; $i++) {
        $dateStrings[$i] = date('Y-m-d', $base + ($i * 86400));
    }

    // Domain prefix: exactly 25 chars
    $domainPrefix = str_pad('example.com/blog/', 25, '/');

    // Timestamp remainder: exactly 15 chars after date (e.g. "T12:34:56.000Z\n" but we pad)
    $tsRemainder = str_pad(' 12:34:56.000  ', TIMESTAMP_REMAINDER, ' ');

    // Build the file content
    $rows = [];
    for ($i = 0; $i < $rowCount; $i++) {
        $slug = $urlSlugs[array_rand($urlSlugs)];
        $date = $dateStrings[array_rand($dateStrings)];
        // Row: [25 domain][slug],[date 10][15 remainder]\n
        $rows[] = $domainPrefix . $slug . ',' . $date . $tsRemainder;
    }

    $fileContent = implode("\n", $rows) . "\n";

    return [
        'fileContent' => $fileContent,
        'urlSlugs'    => $urlSlugs,
        'dateStrings' => $dateStrings,
        'domainPrefix' => $domainPrefix,
    ];
}

// ─────────────────────────────────────────────
// BUILD LOOKUP TABLES
// ─────────────────────────────────────────────

function buildLookupTables(array $urlSlugs, array $dateStrings): array
{
    // urlTokens: slug string => int index
    $urlTokens = [];
    foreach ($urlSlugs as $id => $slug) {
        $urlTokens[$slug] = $id;
    }

    // dateTokens: date string => int index
    $dateTokens = [];
    foreach ($dateStrings as $id => $date) {
        $dateTokens[$date] = $id;
    }

    // dateChars (current approach): date string => packed uint16
    $dateChars = [];
    foreach ($dateStrings as $id => $date) {
        $dateChars[$date] = pack('v', $id);
    }

    // combined nested: urlSlug => [ dateStr => flat_index ]
    $combined = [];
    foreach ($urlSlugs as $uid => $slug) {
        $combined[$slug] = [];
        foreach ($dateStrings as $did => $date) {
            $combined[$slug][$date] = $uid * DATE_COUNT + $did;
        }
    }

    return compact('urlTokens', 'dateTokens', 'dateChars', 'combined');
}

// ─────────────────────────────────────────────
// STRATEGY A: Current (string buffer + unpack + array_count_values)
// ─────────────────────────────────────────────

function strategyA_current(string $chunk, int $chunkEnd, array $urlTokens, array $dateChars): array
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

    $counts = array_fill(0, URL_COUNT * DATE_COUNT, 0);
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
// STRATEGY B: Direct flat array increment (int key, lookup + multiply)
// ─────────────────────────────────────────────

function strategyB_directFlat(string $chunk, int $chunkEnd, array $urlTokens, array $dateTokens): array
{
    $counts = array_fill(0, URL_COUNT * DATE_COUNT, 0);
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
// STRATEGY C: Direct flat array increment (nested combined lookup, no multiply)
// ─────────────────────────────────────────────

function strategyC_combinedNested(string $chunk, int $chunkEnd, array $combined): array
{
    $counts = array_fill(0, URL_COUNT * DATE_COUNT, 0);
    $rowOffset = 0;

    while ($rowOffset < $chunkEnd) {
        $rowComma = strpos($chunk, ",", $rowOffset + 29);
        if ($rowComma === false || $rowComma + 26 > $chunkEnd) break;
        $counts[$combined[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)]
        [substr($chunk, $rowComma + 1, 10)]]++;
        $rowOffset = $rowComma + 27;
    }

    return $counts;
}

// ─────────────────────────────────────────────
// STRATEGY D: Direct per-URL nested array (URL-keyed outer, date-keyed inner)
//             Deferred flatten — avoids large flat array init cost
// ─────────────────────────────────────────────

function strategyD_nestedUrlDate(string $chunk, int $chunkEnd, array $urlTokens, array $dateTokens): array
{
    $counts = [];
    $rowOffset = 0;

    while ($rowOffset < $chunkEnd) {
        $rowComma = strpos($chunk, ",", $rowOffset + 29);
        if ($rowComma === false || $rowComma + 26 > $chunkEnd) break;
        $uid = $urlTokens[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)];
        $did = $dateTokens[substr($chunk, $rowComma + 1, 10)];
        $counts[$uid][$did] = ($counts[$uid][$did] ?? 0) + 1;
        $rowOffset = $rowComma + 27;
    }

    return $counts; // caller flattens — we time only the accumulation phase
}

// ─────────────────────────────────────────────
// STRATEGY E: String buffer accumulation only (no second pass)
//             Tests raw cost of current hot path without the unpack phase
//             (diagnostic, not a complete solution)
// ─────────────────────────────────────────────

function strategyE_bufferOnly(string $chunk, int $chunkEnd, array $urlTokens, array $dateChars): array
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

    return $buckets; // raw buffers — shows hot-path cost without second pass
}

// ─────────────────────────────────────────────
// RUNNER
// ─────────────────────────────────────────────

function bench(string $label, callable $fn, int $iterations = 5): void
{
    // Warmup
    $fn();

    $times = [];
    for ($i = 0; $i < $iterations; $i++) {
        $t = hrtime(true);
        $fn();
        $times[] = (hrtime(true) - $t) / 1e6; // ms
    }

    $min  = min($times);
    $max  = max($times);
    $mean = array_sum($times) / count($times);
    $med  = $times[(int)(count($times) / 2)];
    sort($times);

    printf(
        "%-45s  min=%7.2fms  median=%7.2fms  mean=%7.2fms  max=%7.2fms\n",
        $label, $min, $med, $mean, $max
    );
}

// ─────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────

echo "Generating synthetic data (" . number_format(ROW_COUNT) . " rows, " . URL_COUNT . " URLs, " . DATE_COUNT . " dates)...\n";
$data = generateData(URL_COUNT, DATE_COUNT, ROW_COUNT);
$tables = buildLookupTables($data['urlSlugs'], $data['dateStrings']);

$chunk    = $data['fileContent'];
$chunkEnd = strlen($chunk);

echo "Chunk size: " . number_format($chunkEnd / 1024 / 1024, 2) . " MB\n";
echo "Flat counts array size: " . number_format(URL_COUNT * DATE_COUNT) . " cells (~"
    . number_format(URL_COUNT * DATE_COUNT * 8 / 1024 / 1024, 2) . " MB at 8 bytes/int)\n\n";

// Verify all strategies return consistent results
echo "Verifying strategy consistency...\n";
$refResult = strategyA_current($chunk, $chunkEnd, $tables['urlTokens'], $tables['dateChars']);
$bResult   = strategyB_directFlat($chunk, $chunkEnd, $tables['urlTokens'], $tables['dateTokens']);
$cResult   = strategyC_combinedNested($chunk, $chunkEnd, $tables['combined']);

$totalRef = array_sum($refResult);
$totalB   = array_sum($bResult);
$totalC   = array_sum($cResult);

echo "  Strategy A total visits: " . number_format($totalRef) . "\n";
echo "  Strategy B total visits: " . number_format($totalB)   . " " . ($totalB === $totalRef ? "✓" : "✗ MISMATCH") . "\n";
echo "  Strategy C total visits: " . number_format($totalC)   . " " . ($totalC === $totalRef ? "✓" : "✗ MISMATCH") . "\n";

// For D we just check total count from the nested structure
$dResult = strategyD_nestedUrlDate($chunk, $chunkEnd, $tables['urlTokens'], $tables['dateTokens']);
$totalD = 0;
array_walk_recursive($dResult, function($v) use (&$totalD) { $totalD += $v; });
echo "  Strategy D total visits: " . number_format($totalD) . " " . ($totalD === $totalRef ? "✓" : "✗ MISMATCH") . "\n";

echo "\n";
echo str_repeat('─', 100) . "\n";
echo "BENCHMARK RESULTS (5 iterations each)\n";
echo str_repeat('─', 100) . "\n";

bench(
    'A: String buffer + unpack + array_count_values (current)',
    fn() => strategyA_current($chunk, $chunkEnd, $tables['urlTokens'], $tables['dateChars'])
);

bench(
    'B: Direct flat array increment (lookup + multiply)',
    fn() => strategyB_directFlat($chunk, $chunkEnd, $tables['urlTokens'], $tables['dateTokens'])
);

bench(
    'C: Direct flat array increment (combined nested lookup)',
    fn() => strategyC_combinedNested($chunk, $chunkEnd, $tables['combined'])
);

bench(
    'D: Per-URL nested array (deferred flatten, accumulation only)',
    fn() => strategyD_nestedUrlDate($chunk, $chunkEnd, $tables['urlTokens'], $tables['dateTokens'])
);

bench(
    'E: String buffer only, no second pass (diagnostic baseline)',
    fn() => strategyE_bufferOnly($chunk, $chunkEnd, $tables['urlTokens'], $tables['dateChars'])
);

echo str_repeat('─', 100) . "\n";

// ─────────────────────────────────────────────
// SECOND PASS COST ISOLATION
// ─────────────────────────────────────────────

echo "\nIsolating second-pass cost (unpack + array_count_values only):\n";
$buckets = strategyE_bufferOnly($chunk, $chunkEnd, $tables['urlTokens'], $tables['dateChars']);

bench(
    'A second-pass only (unpack + array_count_values)',
    function() use ($buckets) {
        $counts = array_fill(0, URL_COUNT * DATE_COUNT, 0);
        for ($s = 0; $s < URL_COUNT; $s++) {
            if ($buckets[$s] === '') continue;
            $base = $s * DATE_COUNT;
            foreach (array_count_values(unpack('v*', $buckets[$s])) as $dateId => $count) {
                $counts[$base + $dateId] = $count;
            }
        }
        return $counts;
    }
);

echo str_repeat('─', 100) . "\n";
echo "\nDone.\n";
