<?php

include __DIR__ . '/../vendor/autoload.php';

use App\Commands\Visit;

/**
 * URL Minimum Discriminating Column Finder
 *
 * Finds the smallest set of character positions (columns) needed to
 * uniquely identify each URL in a list.
 *
 * Supports "extended strings" — data appended after the URL (e.g. a date
 * column from a CSV row) so that positions beyond a short URL's length
 * can still contribute discriminating signal rather than returning a
 * uniform null marker.
 *
 * Input format (two modes):
 *
 *   Simple:   $entries = ['https://...', 'https://...'];
 *
 *   Extended: $entries = [
 *       ['url' => 'https://...', 'ext' => '|2024-01-15|other data'],
 *       ['url' => 'https://...', 'ext' => '|2024-03-22|other data'],
 *   ];
 *
 * The 'ext' string is appended directly to the URL to form the full
 * character sequence the algorithm searches over. Use whatever separator
 * your source data has (|, comma, tab, etc.) — or none at all.
 */

// ─────────────────────────────────────────────────────────────────────
//  Configuration — swap in your real data
// ─────────────────────────────────────────────────────────────────────

// Simple mode example (no extended data)
// $entries = [
//     'https://example.com/api/v1/users',
//     'https://example.com/api/v1/orders',
// ];

// Extended mode example — ext data appended beyond the URL
//$entries = [
//    ['url' => 'https://example.com/a',       'ext' => '|2024-01-15'],
//    ['url' => 'https://example.com/ab',      'ext' => '|2024-01-15'],
//    ['url' => 'https://example.com/abc',     'ext' => '|2024-01-15'],
//    ['url' => 'https://example.com/api/v1/users',    'ext' => '|2024-03-01'],
//    ['url' => 'https://example.com/api/v1/orders',   'ext' => '|2024-03-01'],
//    ['url' => 'https://example.com/api/v1/products', 'ext' => '|2024-03-01'],
//    ['url' => 'https://example.com/api/v2/users',    'ext' => '|2024-06-15'],
//    ['url' => 'https://example.com/api/v2/orders',   'ext' => '|2024-06-15'],
//    ['url' => 'https://example.com/api/v2/products', 'ext' => '|2024-06-15'],
//    ['url' => 'https://staging.example.com/api/v1/users',  'ext' => '|2024-03-01'],
//    ['url' => 'https://staging.example.com/api/v2/users',  'ext' => '|2024-06-15'],
//];

$entries = array_map(
    fn($v) => ['url' => substr($v->uri, 25), 'ext' => ',202#-##-##T##:##:##+00:00' . "\n" . 'https://stitcher.io'],
    Visit::all()
);

// ─────────────────────────────────────────────────────────────────────
//  Normalise input into { url, full } pairs
// ─────────────────────────────────────────────────────────────────────

/**
 * @return array{url: string, full: string}[]
 */
function normaliseEntries(array $entries): array
{
    $out = [];
    foreach ($entries as $entry) {
        if (is_string($entry)) {
            $out[] = ['url' => $entry, 'full' => $entry];
        } elseif (is_array($entry)) {
            $url  = $entry['url'] ?? throw new \InvalidArgumentException("Missing 'url' key.");
            $ext  = $entry['ext'] ?? '';
            $out[] = ['url' => $url, 'full' => $url . $ext];
        } else {
            throw new \InvalidArgumentException('Each entry must be a string or array.');
        }
    }
    return $out;
}

// ─────────────────────────────────────────────────────────────────────
//  Core algorithm
// ─────────────────────────────────────────────────────────────────────

/**
 * Extract a signature from a full string using only the given column positions.
 * Positions beyond the string length return a null byte (absent marker).
 */
function signature(string $full, array $columns): string
{
    $sig = '';
    foreach ($columns as $col) {
        $sig .= $full[$col] ?? "\x00";
    }
    return $sig;
}

/**
 * Build groups of entry indices that share the same signature.
 * Groups with >1 member are "ambiguous".
 *
 * @param  array{url: string, full: string}[] $entries
 * @return array<string, int[]>
 */
function buildGroups(array $entries, array $columns): array
{
    $groups = [];
    foreach ($entries as $i => $entry) {
        $sig = signature($entry['full'], $columns);
        $groups[$sig][] = $i;
    }
    return $groups;
}

/** Count URLs that are still ambiguous (share a signature with ≥1 other). */
function ambiguousCount(array $groups): int
{
    $count = 0;
    foreach ($groups as $members) {
        if (count($members) > 1) {
            $count += count($members);
        }
    }
    return $count;
}

/**
 * Greedy column selector.
 *
 * Each round picks the single column that most reduces the ambiguous set.
 * Tie-break: prefer lower (leftmost) column index.
 *
 * @param  array{url: string, full: string}[] $entries
 * @return array{
 *   columns: int[],
 *   urlColumns: int[],
 *   extColumns: int[],
 *   maxUrlLen: int
 * }
 */
function findMinDiscriminatingColumns(array $entries): array
{
    $urls = array_column($entries, 'url');
    if (count($urls) !== count(array_unique($urls))) {
        throw new \InvalidArgumentException('Input list contains duplicate URLs.');
    }
    if (count($entries) <= 1) {
        return ['columns' => [], 'urlColumns' => [], 'extColumns' => [], 'maxUrlLen' => 0];
    }

    $maxFullLen = max(array_map(fn($e) => strlen($e['full']), $entries));
    $maxUrlLen  = max(array_map(fn($e) => strlen($e['url']),  $entries));

    $remaining = range(0, $maxFullLen - 1);
    $chosen    = [];
    $groups    = buildGroups($entries, []);

    while (ambiguousCount($groups) > 0 && !empty($remaining)) {
        $bestCol       = null;
        $bestReduction = -1;
        $beforeCount   = ambiguousCount($groups);

        foreach ($remaining as $col) {
            $candidate = array_merge($chosen, [$col]);
            $newGroups = buildGroups($entries, $candidate);
            $reduction = $beforeCount - ambiguousCount($newGroups);

            if ($reduction > $bestReduction) {
                $bestReduction = $reduction;
                $bestCol       = $col;
            }
        }

        $chosen[]  = $bestCol;
        $remaining = array_values(array_diff($remaining, [$bestCol]));
        $groups    = buildGroups($entries, $chosen);
    }

    sort($chosen);

    return [
        'columns'    => $chosen,
        'urlColumns' => array_values(array_filter($chosen, fn($c) => $c < $maxUrlLen)),
        'extColumns' => array_values(array_filter($chosen, fn($c) => $c >= $maxUrlLen)),
        'maxUrlLen'  => $maxUrlLen,
    ];
}

// ─────────────────────────────────────────────────────────────────────
//  Run
// ─────────────────────────────────────────────────────────────────────

$entries = normaliseEntries($entries);
$result  = findMinDiscriminatingColumns($entries);

[
    'columns'    => $columns,
    'urlColumns' => $urlColumns,
    'extColumns' => $extColumns,
    'maxUrlLen'  => $maxUrlLen,
] = $result;

$maxFullLen = max(array_map(fn($e) => strlen($e['full']), $entries));
$maxUrlDisplayLen = max(array_map(fn($e) => strlen($e['url']),  $entries));

// ── Header ────────────────────────────────────────────────────────────
echo "URLs under analysis\n";
echo str_repeat('─', 70) . "\n";
foreach ($entries as $i => $e) {
    $hasExt = $e['full'] !== $e['url'];
    $suffix = $hasExt ? '  +ext: ' . substr($e['full'], strlen($e['url'])) : '';
    printf("  [%3d] %s%s\n", $i, $e['url'], $suffix);
}

// ── Summary ───────────────────────────────────────────────────────────
echo "\n";
echo "Minimum discriminating columns : " . count($columns) . "\n";
echo "  Within URL (positions)       : " . (empty($urlColumns) ? '(none)' : implode(', ', $urlColumns)) . "\n";
echo "  In extended data (positions) : " . (empty($extColumns) ? '(none)' : implode(', ', $extColumns)) . "\n";

// ── Visual marker under the longest full string ───────────────────────
$longestFull = array_reduce($entries, fn($c, $e) => strlen($e['full']) > strlen($c) ? $e['full'] : $c, '');
$marker = str_repeat(' ', strlen($longestFull));
foreach ($columns as $col) {
    if ($col < strlen($marker)) $marker[$col] = '^';
}
// Show a boundary marker where URL ends (if there is extended data)
$hasAnyExt = array_reduce($entries, fn($c, $e) => $c || $e['full'] !== $e['url'], false);

echo "\nColumn markers  (^ = selected" . ($hasAnyExt ? ", | = URL/ext boundary" : "") . ")\n";
echo str_repeat('─', 70) . "\n";
echo "  " . $longestFull . "\n";
if ($hasAnyExt) {
    // Mark boundary
    $boundary = str_repeat(' ', strlen($longestFull));
    $boundary[$maxUrlLen] = '|';
    echo "  " . $boundary . "\n";
}
echo "  " . substr($marker, 0, strlen($longestFull)) . "\n";

// ── Signatures ───────────────────────────────────────────────────────
echo "\nURL → minimal signature\n";
echo str_repeat('─', 70) . "\n";
$maxLabelLen = $maxUrlDisplayLen;
foreach ($entries as $e) {
    $sig = signature($e['full'], $columns);
    printf("  %-{$maxLabelLen}s  →  \"%s\"\n", $e['url'], addcslashes($sig, "\x00\n\r"));
}

// ── Uniqueness check ─────────────────────────────────────────────────
echo "\n";
$sigs   = array_map(fn($e) => signature($e['full'], $columns), $entries);
$unique = count($sigs) === count(array_unique($sigs));
echo "Uniqueness check: " . ($unique ? "✓ All signatures are unique" : "✗ COLLISION DETECTED — list may contain true duplicates") . "\n";

// ── Per-column breakdown ─────────────────────────────────────────────
echo "\nPer-column character breakdown\n";
echo str_repeat('─', 70) . "\n";
$labelW = max(6, $maxUrlDisplayLen);
printf("  %-6s  %-6s  %-{$labelW}s  %s\n", 'Col', 'Zone', 'URL', 'Char at col');
printf("  %s  %s  %s  %s\n", str_repeat('-',6), str_repeat('-',6), str_repeat('-',$labelW), str_repeat('-',11));
foreach ($entries as $e) {
    foreach ($columns as $col) {
        $zone = $col < strlen($e['url']) ? 'url' : 'ext';
        $char = $e['full'][$col] ?? '(none)';
        printf("  %-6d  %-6s  %-{$labelW}s  %s\n", $col, $zone, $e['url'], var_export($char, true));
    }
}