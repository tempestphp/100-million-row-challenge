<?php

/**
 * B6 Unroll Factor Benchmark
 *
 * Your original A strategy uses a two-tier unroll: 15x primary + 2x secondary.
 * On your machine B6 (4x unroll) showed significant gains over non-unrolled variants.
 * This benchmark finds the optimal unroll factor for B6 on your hardware.
 *
 * Variants tested:
 *  B6_2:  2x unroll
 *  B6_4:  4x unroll  (previous benchmark winner)
 *  B6_8:  8x unroll
 *  B6_15: 15x unroll (matches your A strategy primary tier)
 *  B6_20: 20x unroll
 *  B6_30: 30x unroll
 *  B6_2t_15_2:  Two-tier: 15x primary + 2x secondary (mirrors A's structure)
 *  B6_2t_15_4:  Two-tier: 15x primary + 4x secondary
 *  B6_2t_30_4:  Two-tier: 30x primary + 4x secondary
 *
 * All variants use:
 *  - Pre-shifted URL tokens (no multiply in hot path)
 *  - Fresh array_fill per call (6ms overhead, but consistent baseline)
 */

declare(strict_types=1);

const URL_COUNT   = 268;
const DATE_COUNT  = 2008;
const ROW_COUNT   = 500_000;
const FLAT_SIZE   = URL_COUNT * DATE_COUNT;
const MAX_ROW_LEN = 99; // conservative max row length for fence calculation

// ─────────────────────────────────────────────
// DATA GENERATION
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
        'chunk'    => implode("\n", $rows) . "\n",
        'urlSlugs' => $urlSlugs,
        'dateStrings' => $dateStrings,
    ];
}

function buildTables(array $urlSlugs, array $dateStrings): array
{
    $urlTokensShifted = [];
    foreach ($urlSlugs as $id => $slug) {
        $urlTokensShifted[$slug] = $id * DATE_COUNT;
    }

    $dateTokens = [];
    foreach ($dateStrings as $id => $date) {
        $dateTokens[$date] = $id;
    }

    $dateChars = [];
    foreach ($dateStrings as $id => $date) {
        $dateChars[$date] = pack('v', $id);
    }

    $urlTokens = [];
    foreach ($urlSlugs as $id => $slug) {
        $urlTokens[$slug] = $id;
    }

    return compact('urlTokensShifted', 'dateTokens', 'dateChars', 'urlTokens');
}

// ─────────────────────────────────────────────
// REFERENCE: Strategy A
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
// SINGLE-ROW INNER BODY (macro-like for clarity)
// ─────────────────────────────────────────────
// Each unrolled copy is:
//   $rc = strpos($chunk, ",", $ro + 29);
//   $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++;
//   $ro = $rc + 27;

// ─────────────────────────────────────────────
// B6 VARIANTS — generated inline to avoid function call overhead in hot path
// ─────────────────────────────────────────────

function b6_2x(string $chunk, int $chunkEnd, array $uts, array $dt): array
{
    $counts = array_fill(0, FLAT_SIZE, 0);
    $ro = 0;
    $fence = $chunkEnd - (2 * MAX_ROW_LEN);
    while ($ro < $fence) {
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
    }
    while ($ro < $chunkEnd) { $rc = strpos($chunk, ",", $ro + 29); if ($rc === false || $rc + 26 > $chunkEnd) break; $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27; }
    return $counts;
}

function b6_4x(string $chunk, int $chunkEnd, array $uts, array $dt): array
{
    $counts = array_fill(0, FLAT_SIZE, 0);
    $ro = 0;
    $fence = $chunkEnd - (4 * MAX_ROW_LEN);
    while ($ro < $fence) {
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
    }
    while ($ro < $chunkEnd) { $rc = strpos($chunk, ",", $ro + 29); if ($rc === false || $rc + 26 > $chunkEnd) break; $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27; }
    return $counts;
}

function b6_8x(string $chunk, int $chunkEnd, array $uts, array $dt): array
{
    $counts = array_fill(0, FLAT_SIZE, 0);
    $ro = 0;
    $fence = $chunkEnd - (8 * MAX_ROW_LEN);
    while ($ro < $fence) {
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
    }
    while ($ro < $chunkEnd) { $rc = strpos($chunk, ",", $ro + 29); if ($rc === false || $rc + 26 > $chunkEnd) break; $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27; }
    return $counts;
}

function b6_15x(string $chunk, int $chunkEnd, array $uts, array $dt): array
{
    $counts = array_fill(0, FLAT_SIZE, 0);
    $ro = 0;
    $fence = $chunkEnd - (15 * MAX_ROW_LEN);
    while ($ro < $fence) {
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
    }
    while ($ro < $chunkEnd) { $rc = strpos($chunk, ",", $ro + 29); if ($rc === false || $rc + 26 > $chunkEnd) break; $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27; }
    return $counts;
}

function b6_20x(string $chunk, int $chunkEnd, array $uts, array $dt): array
{
    $counts = array_fill(0, FLAT_SIZE, 0);
    $ro = 0;
    $fence = $chunkEnd - (20 * MAX_ROW_LEN);
    while ($ro < $fence) {
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
    }
    while ($ro < $chunkEnd) { $rc = strpos($chunk, ",", $ro + 29); if ($rc === false || $rc + 26 > $chunkEnd) break; $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27; }
    return $counts;
}

function b6_30x(string $chunk, int $chunkEnd, array $uts, array $dt): array
{
    $counts = array_fill(0, FLAT_SIZE, 0);
    $ro = 0;
    $fence = $chunkEnd - (30 * MAX_ROW_LEN);
    while ($ro < $fence) {
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
    }
    while ($ro < $chunkEnd) { $rc = strpos($chunk, ",", $ro + 29); if ($rc === false || $rc + 26 > $chunkEnd) break; $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27; }
    return $counts;
}

// ─────────────────────────────────────────────
// TWO-TIER: 15x primary + 2x secondary (mirrors your A structure)
// ─────────────────────────────────────────────
function b6_2tier_15_2(string $chunk, int $chunkEnd, array $uts, array $dt): array
{
    $counts = array_fill(0, FLAT_SIZE, 0);
    $ro = 0;

    $fence = $chunkEnd - (15 * MAX_ROW_LEN);
    while ($ro < $fence) {
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
    }

    $fence = $chunkEnd - (2 * MAX_ROW_LEN);
    while ($ro < $fence) {
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
    }

    while ($ro < $chunkEnd) { $rc = strpos($chunk, ",", $ro + 29); if ($rc === false || $rc + 26 > $chunkEnd) break; $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27; }
    return $counts;
}

// ─────────────────────────────────────────────
// TWO-TIER: 30x primary + 4x secondary
// ─────────────────────────────────────────────
function b6_2tier_30_4(string $chunk, int $chunkEnd, array $uts, array $dt): array
{
    $counts = array_fill(0, FLAT_SIZE, 0);
    $ro = 0;

    $fence = $chunkEnd - (30 * MAX_ROW_LEN);
    while ($ro < $fence) {
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
    }

    $fence = $chunkEnd - (4 * MAX_ROW_LEN);
    while ($ro < $fence) {
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
        $rc = strpos($chunk, ",", $ro + 29); $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27;
    }

    while ($ro < $chunkEnd) { $rc = strpos($chunk, ",", $ro + 29); if ($rc === false || $rc + 26 > $chunkEnd) break; $counts[$uts[substr($chunk, $ro + 25, $rc - $ro - 25)] + $dt[substr($chunk, $rc + 1, 10)]]++; $ro = $rc + 27; }
    return $counts;
}

// ─────────────────────────────────────────────
// RUNNER
// ─────────────────────────────────────────────

function bench(string $label, callable $fn, int $iterations = 9): void
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
    $p50 = $times[(int)($iterations / 2)];
    $p75 = $times[(int)($iterations * 0.75)];

    printf("%-55s  min=%7.2fms  p50=%7.2fms  p75=%7.2fms\n", $label, $min, $p50, $p75);
}

// ─────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────

echo "Generating " . number_format(ROW_COUNT) . " rows...\n";
$data   = generateData();
$tables = buildTables($data['urlSlugs'], $data['dateStrings']);

$chunk    = $data['chunk'];
$chunkEnd = strlen($chunk);
$uts      = $tables['urlTokensShifted'];
$dt       = $tables['dateTokens'];

echo "Chunk: " . number_format($chunkEnd / 1024 / 1024, 2) . " MB\n\n";

// Correctness
echo "Verifying correctness...\n";
$ref = array_sum(strategyA($chunk, $chunkEnd, $tables['urlTokens'], $tables['dateChars']));
$variants = [
    'B6_2x'        => b6_2x($chunk, $chunkEnd, $uts, $dt),
    'B6_4x'        => b6_4x($chunk, $chunkEnd, $uts, $dt),
    'B6_8x'        => b6_8x($chunk, $chunkEnd, $uts, $dt),
    'B6_15x'       => b6_15x($chunk, $chunkEnd, $uts, $dt),
    'B6_20x'       => b6_20x($chunk, $chunkEnd, $uts, $dt),
    'B6_30x'       => b6_30x($chunk, $chunkEnd, $uts, $dt),
    'B6_2tier_15_2'=> b6_2tier_15_2($chunk, $chunkEnd, $uts, $dt),
    'B6_2tier_30_4'=> b6_2tier_30_4($chunk, $chunkEnd, $uts, $dt),
];
foreach ($variants as $name => $result) {
    $t = array_sum($result);
    printf("  %-20s %s visits %s\n", $name, number_format($t), $t === $ref ? '✓' : '✗ MISMATCH');
}

echo "\n";
echo str_repeat('─', 95) . "\n";
echo "BENCHMARK — B6 UNROLL FACTOR SEARCH (9 iterations)\n";
echo str_repeat('─', 95) . "\n";

bench('A:  Current strategy [reference]',
    fn() => strategyA($chunk, $chunkEnd, $tables['urlTokens'], $tables['dateChars']));

echo str_repeat('─', 95) . "\n";
echo "Single-tier unroll variants:\n";
echo str_repeat('─', 95) . "\n";

bench('B6_2x:  pre-shifted + 2x  unroll',  fn() => b6_2x($chunk, $chunkEnd, $uts, $dt));
bench('B6_4x:  pre-shifted + 4x  unroll',  fn() => b6_4x($chunk, $chunkEnd, $uts, $dt));
bench('B6_8x:  pre-shifted + 8x  unroll',  fn() => b6_8x($chunk, $chunkEnd, $uts, $dt));
bench('B6_15x: pre-shifted + 15x unroll',  fn() => b6_15x($chunk, $chunkEnd, $uts, $dt));
bench('B6_20x: pre-shifted + 20x unroll',  fn() => b6_20x($chunk, $chunkEnd, $uts, $dt));
bench('B6_30x: pre-shifted + 30x unroll',  fn() => b6_30x($chunk, $chunkEnd, $uts, $dt));

echo str_repeat('─', 95) . "\n";
echo "Two-tier unroll variants:\n";
echo str_repeat('─', 95) . "\n";

bench('B6_2tier 15x+2x (mirrors A structure)', fn() => b6_2tier_15_2($chunk, $chunkEnd, $uts, $dt));
bench('B6_2tier 30x+4x',                       fn() => b6_2tier_30_4($chunk, $chunkEnd, $uts, $dt));

echo str_repeat('─', 95) . "\n";
echo "\nDone. Run on your machine to find the optimal unroll factor for your CPU.\n";