<?php

/**
 * Benchmark: strpos-per-row vs bulk newline discovery with hardcoded offsets
 *
 * Simulates the core hot-path from WorkerTokenizedSocketV2Trait::work()
 * using a synthetic window of fixed-format CSV-like rows.
 *
 * Row format (matches your production data assumptions):
 *   {DOMAIN_PREFIX}{URL_SLUG},{DATE_SUFFIX}\n
 *
 * Constants mirroring your trait:
 */
const DOMAIN_LENGTH = 19;   // adjust to match your actual constant
const DATE_WIDTH    = 10;   // chars for date field at end of row (before \n)
const DATE_LENGTH   = 2;    // bytes you actually store per date token
const MIN_LINE_LEN  = 40;   // your $minLineLength equivalent
const UNROLL        = 5;    // rows per unrolled iteration in the new path

// ---------------------------------------------------------------------------
// Synthetic data generation
// ---------------------------------------------------------------------------

function buildSyntheticWindow(int $rowCount, int $rowLen = 80): string
{
    // Fixed-length rows so offsets are predictable — worst case for strpos,
    // best case for hardcoded offsets. Vary $rowLen slightly for realism.
    $lines = [];
    for ($i = 0; $i < $rowCount; $i++) {
        $variance  = rand(-5, 5);
        $lineLen   = max(60, $rowLen + $variance);
        $domain    = str_repeat('d', DOMAIN_LENGTH);
        $dateSuffix = str_repeat('2', DATE_WIDTH);
        $slug      = str_repeat('x', $lineLen - DOMAIN_LENGTH - DATE_WIDTH - 2); // -2 for comma + \n
        $lines[]   = $domain . $slug . ',' . $dateSuffix;
    }
    return implode("\n", $lines) . "\n";
}

// ---------------------------------------------------------------------------
// Helpers shared by both strategies
// ---------------------------------------------------------------------------

function makeTokenMap(string $window): array
{
    // Build a simple urlTokens map: substring -> int index
    // In production this is pre-built; here we fake it with a hash.
    return []; // we'll bypass the map lookup for pure offset timing
}

// ---------------------------------------------------------------------------
// Strategy A: current approach — strpos per row, 5x unrolled
// ---------------------------------------------------------------------------

function strategyA_strpos(string $window, int $windowEnd, int $minLineLen): int
{
    $wStart = 0;
    $fence  = $windowEnd - (5 * 100);
    $count  = 0;

    while ($wStart < $fence) {
        $wEnd = strpos($window, "\n", $wStart + $minLineLen);
        $_a = substr($window, $wStart + DOMAIN_LENGTH, $wEnd - $wStart - DOMAIN_LENGTH - DATE_WIDTH - 1);
        $_b = substr($window, $wEnd - DATE_WIDTH, DATE_LENGTH);
        $wStart = $wEnd + 1; $count++;

        $wEnd = strpos($window, "\n", $wStart + $minLineLen);
        $_a = substr($window, $wStart + DOMAIN_LENGTH, $wEnd - $wStart - DOMAIN_LENGTH - DATE_WIDTH - 1);
        $_b = substr($window, $wEnd - DATE_WIDTH, DATE_LENGTH);
        $wStart = $wEnd + 1; $count++;

        $wEnd = strpos($window, "\n", $wStart + $minLineLen);
        $_a = substr($window, $wStart + DOMAIN_LENGTH, $wEnd - $wStart - DOMAIN_LENGTH - DATE_WIDTH - 1);
        $_b = substr($window, $wEnd - DATE_WIDTH, DATE_LENGTH);
        $wStart = $wEnd + 1; $count++;

        $wEnd = strpos($window, "\n", $wStart + $minLineLen);
        $_a = substr($window, $wStart + DOMAIN_LENGTH, $wEnd - $wStart - DOMAIN_LENGTH - DATE_WIDTH - 1);
        $_b = substr($window, $wEnd - DATE_WIDTH, DATE_LENGTH);
        $wStart = $wEnd + 1; $count++;

        $wEnd = strpos($window, "\n", $wStart + $minLineLen);
        $_a = substr($window, $wStart + DOMAIN_LENGTH, $wEnd - $wStart - DOMAIN_LENGTH - DATE_WIDTH - 1);
        $_b = substr($window, $wEnd - DATE_WIDTH, DATE_LENGTH);
        $wStart = $wEnd + 1; $count++;
    }

    // cleanup
    while ($wStart < $windowEnd) {
        $wEnd = strpos($window, "\n", $wStart + $minLineLen);
        if ($wEnd === false || $wEnd > $windowEnd) break;
        $_a = substr($window, $wStart + DOMAIN_LENGTH, $wEnd - $wStart - DOMAIN_LENGTH - DATE_WIDTH - 1);
        $_b = substr($window, $wEnd - DATE_WIDTH, DATE_LENGTH);
        $wStart = $wEnd + 1; $count++;
    }

    return $count;
}

function strategyZ_hoist(string $window, int $windowEnd, int $minLineLen): int
{
    $wStart = 0;
    $fence  = $windowEnd - (5 * 100);
    $count  = 0;

    $dl = DOMAIN_LENGTH;
    $dw = DATE_WIDTH;
    $dLen = DATE_LENGTH;

    while ($wStart < $fence) {
        $wEnd = strpos($window, "\n", $wStart + $minLineLen);
        $_a = substr($window, $wStart + $dl, $wEnd - $wStart - $dl - $dw - 1);
        $_b = substr($window, $wEnd - $dw, $dLen);
        $wStart = $wEnd + 1; $count++;

        $wEnd = strpos($window, "\n", $wStart + $minLineLen);
        $_a = substr($window, $wStart + $dl, $wEnd - $wStart - $dl - $dw - 1);
        $_b = substr($window, $wEnd - $dw, $dLen);
        $wStart = $wEnd + 1; $count++;

        $wEnd = strpos($window, "\n", $wStart + $minLineLen);
        $_a = substr($window, $wStart + $dl, $wEnd - $wStart - $dl - $dw - 1);
        $_b = substr($window, $wEnd - $dw, $dLen);
        $wStart = $wEnd + 1; $count++;

        $wEnd = strpos($window, "\n", $wStart + $minLineLen);
        $_a = substr($window, $wStart + $dl, $wEnd - $wStart - $dl - $dw - 1);
        $_b = substr($window, $wEnd - $dw, $dLen);
        $wStart = $wEnd + 1; $count++;

        $wEnd = strpos($window, "\n", $wStart + $minLineLen);
        $_a = substr($window, $wStart + $dl, $wEnd - $wStart - $dl - $dw - 1);
        $_b = substr($window, $wEnd - $dw, $dLen);
        $wStart = $wEnd + 1; $count++;
    }

    // cleanup
    while ($wStart < $windowEnd) {
        $wEnd = strpos($window, "\n", $wStart + $minLineLen);
        if ($wEnd === false || $wEnd > $windowEnd) break;
        $_a = substr($window, $wStart + $dl, $wEnd - $wStart - $dl - $dw - 1);
        $_b = substr($window, $wEnd - $dw, $dLen);
        $wStart = $wEnd + 1; $count++;
    }

    return $count;
}

// ---------------------------------------------------------------------------
// Strategy D: strpos-per-row, but hardcoded integer literals instead of constants
// Isolates: does constant resolution (self::CONST / PHP const lookup) cost anything?
// ---------------------------------------------------------------------------

function strategyD_strpos_hardcoded(string $window, int $windowEnd, int $minLineLen): int
{
    $wStart = 0;
    $fence  = $windowEnd - (5 * 100);
    $count  = 0;

    while ($wStart < $fence) {
        $wEnd = strpos($window, "\n", $wStart + $minLineLen);
        $_a = substr($window, $wStart + 19, $wEnd - $wStart - 30);  // 19=DOMAIN, 30=DOMAIN+DATE_WIDTH+1
        $_b = substr($window, $wEnd - 10, 2);
        $wStart = $wEnd + 1; $count++;

        $wEnd = strpos($window, "\n", $wStart + $minLineLen);
        $_a = substr($window, $wStart + 19, $wEnd - $wStart - 30);
        $_b = substr($window, $wEnd - 10, 2);
        $wStart = $wEnd + 1; $count++;

        $wEnd = strpos($window, "\n", $wStart + $minLineLen);
        $_a = substr($window, $wStart + 19, $wEnd - $wStart - 30);
        $_b = substr($window, $wEnd - 10, 2);
        $wStart = $wEnd + 1; $count++;

        $wEnd = strpos($window, "\n", $wStart + $minLineLen);
        $_a = substr($window, $wStart + 19, $wEnd - $wStart - 30);
        $_b = substr($window, $wEnd - 10, 2);
        $wStart = $wEnd + 1; $count++;

        $wEnd = strpos($window, "\n", $wStart + $minLineLen);
        $_a = substr($window, $wStart + 19, $wEnd - $wStart - 30);
        $_b = substr($window, $wEnd - 10, 2);
        $wStart = $wEnd + 1; $count++;
    }

    while ($wStart < $windowEnd) {
        $wEnd = strpos($window, "\n", $wStart + $minLineLen);
        if ($wEnd === false || $wEnd > $windowEnd) break;
        $_a = substr($window, $wStart + 19, $wEnd - $wStart - 30);
        $_b = substr($window, $wEnd - 10, 2);
        $wStart = $wEnd + 1; $count++;
    }

    return $count;
}

// ---------------------------------------------------------------------------
// Strategy B: bulk newline discovery via explode, hardcoded constant literals
//
// Finds ALL line boundaries in one pass, stores positions array, then
// the unrolled loop is pure array index arithmetic — no strpos in the hot path.
// ---------------------------------------------------------------------------

function findNewlinePositions(string $window, int $upTo): array
{
    $positions = [];
    $offset    = 0;
    while (($pos = strpos($window, "\n", $offset)) !== false && $pos <= $upTo) {
        $positions[] = $pos;
        $offset = $pos + 1;
    }
    return $positions;
}

function strategyB_bulk(string $window, int $windowEnd, int $minLineLen): int
{
    // One-shot newline scan
    $nls   = findNewlinePositions($window, $windowEnd);
    $total = count($nls);
    $count = 0;
    $i     = 0;

    // We need to reconstruct $wStart for each row.
    // nls[i] is the \n ending row i; row i starts at nls[i-1]+1 (or 0)
    // Pre-build start positions alongside nl positions for clarity:
    $starts    = [0];
    for ($j = 0; $j < $total - 1; $j++) {
        $starts[] = $nls[$j] + 1;
    }

    $fence = $total - UNROLL; // leave room for full unroll

    // Unroll x5 — hardcoded literal constants (19, 10, 2) instead of self::CONST
    while ($i < $fence) {
        $s0 = $starts[$i];   $e0 = $nls[$i];
        $_a = substr($window, $s0 + 19, $e0 - $s0 - 19 - 10 - 1);
        $_b = substr($window, $e0 - 10, 2);
        $i++; $count++;

        $s0 = $starts[$i];   $e0 = $nls[$i];
        $_a = substr($window, $s0 + 19, $e0 - $s0 - 19 - 10 - 1);
        $_b = substr($window, $e0 - 10, 2);
        $i++; $count++;

        $s0 = $starts[$i];   $e0 = $nls[$i];
        $_a = substr($window, $s0 + 19, $e0 - $s0 - 19 - 10 - 1);
        $_b = substr($window, $e0 - 10, 2);
        $i++; $count++;

        $s0 = $starts[$i];   $e0 = $nls[$i];
        $_a = substr($window, $s0 + 19, $e0 - $s0 - 19 - 10 - 1);
        $_b = substr($window, $e0 - 10, 2);
        $i++; $count++;

        $s0 = $starts[$i];   $e0 = $nls[$i];
        $_a = substr($window, $s0 + 19, $e0 - $s0 - 19 - 10 - 1);
        $_b = substr($window, $e0 - 10, 2);
        $i++; $count++;
    }

    // cleanup
    while ($i < $total) {
        $s0 = $starts[$i];   $e0 = $nls[$i];
        $_a = substr($window, $s0 + 19, $e0 - $s0 - 19 - 10 - 1);
        $_b = substr($window, $e0 - 10, 2);
        $i++; $count++;
    }

    return $count;
}

// ---------------------------------------------------------------------------
// Strategy C: bulk discovery via strpos loop (no array overhead for starts),
//             hardcoded literals — splitting the two variables to isolate
//             whether the positions array itself is the cost in B
// ---------------------------------------------------------------------------

function strategyC_nlarray_only(string $window, int $windowEnd): int
{
    // Builds only the nl positions, derives wStart inline from prev nl
    $nls    = findNewlinePositions($window, $windowEnd);
    $total  = count($nls);
    $count  = 0;
    $i      = 0;
    $wStart = 0;
    $fence  = $total - UNROLL;

    while ($i < $fence) {
        $wEnd = $nls[$i++];
        $_a = substr($window, $wStart + 19, $wEnd - $wStart - 30);
        $_b = substr($window, $wEnd - 10, 2);
        $wStart = $wEnd + 1; $count++;

        $wEnd = $nls[$i++];
        $_a = substr($window, $wStart + 19, $wEnd - $wStart - 30);
        $_b = substr($window, $wEnd - 10, 2);
        $wStart = $wEnd + 1; $count++;

        $wEnd = $nls[$i++];
        $_a = substr($window, $wStart + 19, $wEnd - $wStart - 30);
        $_b = substr($window, $wEnd - 10, 2);
        $wStart = $wEnd + 1; $count++;

        $wEnd = $nls[$i++];
        $_a = substr($window, $wStart + 19, $wEnd - $wStart - 30);
        $_b = substr($window, $wEnd - 10, 2);
        $wStart = $wEnd + 1; $count++;

        $wEnd = $nls[$i++];
        $_a = substr($window, $wStart + 19, $wEnd - $wStart - 30);
        $_b = substr($window, $wEnd - 10, 2);
        $wStart = $wEnd + 1; $count++;
    }

    while ($i < $total) {
        $wEnd = $nls[$i++];
        $_a = substr($window, $wStart + 19, $wEnd - $wStart - 30);
        $_b = substr($window, $wEnd - 10, 2);
        $wStart = $wEnd + 1; $count++;
    }

    return $count;
}

// ---------------------------------------------------------------------------
// Benchmark runner
// ---------------------------------------------------------------------------

function bench(string $label, callable $fn, int $iterations): void
{
    // Warmup
    for ($i = 0; $i < 3; $i++) $fn();

    $start = hrtime(true);
    for ($i = 0; $i < $iterations; $i++) $fn();
    $elapsed = hrtime(true) - $start;

    $ms      = $elapsed / 1e6;
    $perIter = $ms / $iterations;

    printf("  %-42s %8.2f ms total  %7.3f ms/iter\n", $label, $ms, $perIter);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

$rowCounts  = [1000, 5000, 20000];
$iterations = 50;

echo PHP_EOL;
echo "=======================================================================\n";
echo " Hot-path offset strategy benchmark\n";
echo " PHP " . PHP_VERSION . "  |  iterations=" . $iterations . " per window size\n";
echo "=======================================================================\n";

foreach ($rowCounts as $rowCount) {
    $window    = buildSyntheticWindow($rowCount);
    $windowEnd = strrpos($window, "\n");
    $windowKB  = round(strlen($window) / 1024, 1);

    echo "\n--- Window: {$rowCount} rows (~{$windowKB} KB) ---\n";

    bench(
        "A) strpos-per-row  (current, self::CONST)",
        fn() => strategyA_strpos($window, $windowEnd, MIN_LINE_LEN),
        $iterations
    );

    bench(
        "B) bulk nl scan + starts[] + hardcoded lits",
        fn() => strategyB_bulk($window, $windowEnd, MIN_LINE_LEN),
        $iterations
    );

    bench(
        "C) bulk nl scan, no starts[], hardcoded lits",
        fn() => strategyC_nlarray_only($window, $windowEnd),
        $iterations
    );

    bench(
        "D) strpos-per-row  (hardcoded int literals)",
        fn() => strategyD_strpos_hardcoded($window, $windowEnd, MIN_LINE_LEN),
        $iterations
    );

    bench(
        "Z) hoist-consts",
        fn() => strategyZ_hoist($window, $windowEnd, MIN_LINE_LEN),
        $iterations
    );

    // Sanity check row counts match
    $a = strategyA_strpos($window, $windowEnd, MIN_LINE_LEN);
    $c = strategyC_nlarray_only($window, $windowEnd);
    $d = strategyD_strpos_hardcoded($window, $windowEnd, MIN_LINE_LEN);
    $z = strategyZ_hoist($window, $windowEnd, MIN_LINE_LEN);
    echo "  [sanity] A={$a}  C={$c}  D={$d} Z={$z} " . ($a === $c && $a === $d && $a === $z ? "✓ all match" : "✗ MISMATCH") . "\n";
}

echo "\nNotes:\n";
echo "  - Strategy A = your current production pattern (strpos per row, self::CONST lookups)\n";
echo "  - Strategy B = one-shot nl scan, pre-built starts[] array, integer literals\n";
echo "  - Strategy C = one-shot nl scan, no starts[] (tracks wStart inline), integer literals\n";
echo "  - The delta between A and C isolates: strpos overhead + constant resolution\n";
echo "  - The delta between B and C isolates: cost of building the starts[] array\n";
echo "  - All substr() calls preserved so CPU work is equivalent\n";
echo PHP_EOL;