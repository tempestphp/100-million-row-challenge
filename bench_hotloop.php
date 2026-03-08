<?php

declare(strict_types=1);

ini_set('memory_limit', '512M');
gc_disable();

// ============================================================
// SETUP: Realistic data mimicking the actual CSV format
// Real format: https://stitcher.io/blog/path,YYYY-MM-DDTHH:MM:SS+00:00
// The tail after the path is always exactly 25 chars: ",YYYY-MM-DDTHH:MM:SS+00:00"
// Wait — the comma itself is NOT part of the 25: the timestamp is 25 chars,
// so the full suffix from comma = 26 chars (",YYYY-MM-DDTHH:MM:SS+00:00").
// commaPos = lineLen - 26  => char at commaPos is ','
// dateKey  = substr($line, commaPos + 3, 8) => "YY-MM-DD"
//   (skips comma + "20" prefix of the year, e.g. "2024" -> skip "20", take "24-MM-DD")
// ============================================================

$pathSegments = [
    '/blog/php-8-fibers',
    '/blog/new-in-php-84',
    '/blog/laravel-beyond-crud',
    '/blog/event-sourcing-in-practice',
    '/blog/responsive-images-done-right',
    '/blog/a-storm-in-a-teacup',
    '/blog/my-current-setup',
    '/blog/stitcher-io-2-0',
    '/blog/the-problem-with-hooks',
    '/blog/shorthand-comparisons-in-php',
    '/blog/on-using-exceptions-for-flow-control',
    '/blog/array-functions-in-php',
    '/blog/php-generics-and-why-we-need-them',
    '/blog/preloading-in-php-74',
    '/blog/readonly-classes-in-php-82',
    '/blog/fibers-are-not-async-php',
];

// Extend to ~268 paths
$paths = [];
foreach ($pathSegments as $seg) {
    $paths[] = $seg;
    for ($i = 2; $i <= 17; $i++) {
        $paths[] = $seg . '-' . $i;
    }
}
$paths = array_slice($paths, 0, 268);
$pathCount = count($paths);

// Build pathIds map (path => int)
$pathIds = [];
foreach ($paths as $id => $p) {
    $pathIds[$p] = $id;
}

// Build dateIds map ("YY-MM-DD" => int)
$dateIds = [];
$dateId = 0;
for ($y = 2020; $y <= 2026; $y++) {
    $yy = substr((string)$y, 2, 2);
    for ($m = 1; $m <= 12; $m++) {
        $mm = str_pad((string)$m, 2, '0', STR_PAD_LEFT);
        $maxDay = match ($m) {
            2 => ($y % 4 === 0) ? 29 : 28,
            4, 6, 9, 11 => 30,
            default => 31,
        };
        for ($d = 1; $d <= $maxDay; $d++) {
            $dd = str_pad((string)$d, 2, '0', STR_PAD_LEFT);
            $dateIds["$yy-$mm-$dd"] = $dateId++;
        }
    }
}
$DATE_COUNT = count($dateIds); // 2557
$dateKeyList = array_keys($dateIds);

echo "=== Hot Loop Benchmarks ===\n";
echo "paths=$pathCount, dateSlots=$DATE_COUNT, totalSlots=" . ($pathCount * $DATE_COUNT) . "\n\n";

// ============================================================
// BUILD FAKE CHUNK (100k lines, real format)
// Format: https://stitcher.io{PATH},YYYY-MM-DDTHH:MM:SS+00:00
// The suffix after path is always ",YYYY-MM-DDTHH:MM:SS+00:00" = 26 chars total
// Timestamp "YYYY-MM-DDTHH:MM:SS+00:00" = 25 chars
// commaPos = lineLen - 26
// dateKey  = substr($line, commaPos + 3, 8) => skips ",20" to get "YY-MM-DD"
// ============================================================

$LINES_PER_CHUNK = 100_000;
$dateKeyCount = count($dateKeyList);

// Fixed time portion — all same time is fine for benchmarking
$timeSuffix = 'T13:55:25+00:00'; // 15 chars

$lineArr = [];
for ($i = 0; $i < $LINES_PER_CHUNK; $i++) {
    $path    = $paths[$i % $pathCount];
    $dateKey = $dateKeyList[$i % min(200, $dateKeyCount)]; // "YY-MM-DD"
    // Reconstruct full 4-digit year for ISO timestamp: "20YY-MM-DD"
    $isoDate = '20' . $dateKey; // "YYYY-MM-DD"
    $lineArr[] = 'https://stitcher.io' . $path . ',' . $isoDate . $timeSuffix;
}

$chunk    = implode("\n", $lineArr) . "\n";
$chunkLen = strlen($chunk);

// Verify format is correct against the first line
$firstLine = $lineArr[0];
$lineLen   = strlen($firstLine);
$commaPos  = $lineLen - 26;
$extractedPath = substr($firstLine, 19, $commaPos - 19);
$extractedDate = substr($firstLine, $commaPos + 3, 8);
assert($extractedPath === $paths[0],        "Path extraction broken: got '$extractedPath'");
assert(isset($dateIds[$extractedDate]),      "Date extraction broken: got '$extractedDate'");
assert($firstLine[$commaPos] === ',',        "commaPos doesn't point at comma");
echo "Format verified: path='$extractedPath', date='$extractedDate'\n\n";

$ITERS = 30;

// ============================================================
// H1: CURRENT — strtok with strlen per line
// ============================================================
$t0 = hrtime(true);
for ($iter = 0; $iter < $ITERS; $iter++) {
    $counts = array_fill(0, $pathCount * $DATE_COUNT, 0);
    $line = strtok($chunk, "\n");
    while ($line !== false) {
        $lineLen  = strlen($line);
        $commaPos = $lineLen - 26;
        $path     = substr($line, 19, $commaPos - 19);
        $dateKey  = substr($line, $commaPos + 3, 8);
        $pathId   = $pathIds[$path] ?? -1;
        if ($pathId !== -1 && isset($dateIds[$dateKey])) {
            ++$counts[$pathId * $DATE_COUNT + $dateIds[$dateKey]];
        }
        $line = strtok("\n");
    }
}
$h1       = (hrtime(true) - $t0) / 1e6 / $ITERS;
$h1counts = $counts;
echo "H1  strtok+strlen [current]:              " . number_format($h1, 3) . "ms\n";

// ============================================================
// H2: strpos manual walk — work directly on the chunk string,
// no per-line copy that strtok produces
// ============================================================
$t0 = hrtime(true);
for ($iter = 0; $iter < $ITERS; $iter++) {
    $counts = array_fill(0, $pathCount * $DATE_COUNT, 0);
    $pos = 0;
    while (($nl = strpos($chunk, "\n", $pos)) !== false) {
        $lineLen  = $nl - $pos;
        $commaPos = $lineLen - 26;
        $path     = substr($chunk, $pos + 19, $commaPos - 19);
        $dateKey  = substr($chunk, $pos + $commaPos + 3, 8);
        $pathId   = $pathIds[$path] ?? -1;
        if ($pathId !== -1 && isset($dateIds[$dateKey])) {
            ++$counts[$pathId * $DATE_COUNT + $dateIds[$dateKey]];
        }
        $pos = $nl + 1;
    }
}
$h2       = (hrtime(true) - $t0) / 1e6 / $ITERS;
$h2counts = $counts;
echo "H2  strpos walk (no line copy):           " . number_format($h2, 3) . "ms  (" . number_format($h1 / $h2, 3) . "x)\n";

// ============================================================
// H3: explode("\n") + foreach
// Builds a full lines array upfront but foreach loop is tight
// ============================================================
$t0 = hrtime(true);
for ($iter = 0; $iter < $ITERS; $iter++) {
    $counts    = array_fill(0, $pathCount * $DATE_COUNT, 0);
    $linesArr  = explode("\n", $chunk);
    foreach ($linesArr as $line) {
        $lineLen = strlen($line);
        if ($lineLen < 27) continue;
        $commaPos = $lineLen - 26;
        $path     = substr($line, 19, $commaPos - 19);
        $dateKey  = substr($line, $commaPos + 3, 8);
        $pathId   = $pathIds[$path] ?? -1;
        if ($pathId !== -1 && isset($dateIds[$dateKey])) {
            ++$counts[$pathId * $DATE_COUNT + $dateIds[$dateKey]];
        }
    }
}
$h3       = (hrtime(true) - $t0) / 1e6 / $ITERS;
$h3counts = $counts;
echo "H3  explode+foreach:                      " . number_format($h3, 3) . "ms  (" . number_format($h1 / $h3, 3) . "x)\n";

// ============================================================
// H4: strtok with precomputed offsets from line end
// lineLen - 45 = path length (19 prefix + 26 tail = 45 fixed chars)
// lineLen - 23 = date start (commaPos + 3 = lineLen - 26 + 3 = lineLen - 23)
// Saves one subtraction and one addition vs H1
// ============================================================
$t0 = hrtime(true);
for ($iter = 0; $iter < $ITERS; $iter++) {
    $counts = array_fill(0, $pathCount * $DATE_COUNT, 0);
    $line = strtok($chunk, "\n");
    while ($line !== false) {
        $lineLen = strlen($line);
        $path    = substr($line, 19, $lineLen - 45);
        $dateKey = substr($line, $lineLen - 23, 8);
        $pathId  = $pathIds[$path] ?? -1;
        if ($pathId !== -1 && isset($dateIds[$dateKey])) {
            ++$counts[$pathId * $DATE_COUNT + $dateIds[$dateKey]];
        }
        $line = strtok("\n");
    }
}
$h4       = (hrtime(true) - $t0) / 1e6 / $ITERS;
$h4counts = $counts;
echo "H4  strtok+precomputed offsets:           " . number_format($h4, 3) . "ms  (" . number_format($h1 / $h4, 3) . "x)\n";

// ============================================================
// H5: strtok + precomputed offsets + dual isset (avoid ?? -1)
// isset($a, $b) short-circuits — if path not found, no date lookup
// ============================================================
$t0 = hrtime(true);
for ($iter = 0; $iter < $ITERS; $iter++) {
    $counts = array_fill(0, $pathCount * $DATE_COUNT, 0);
    $line = strtok($chunk, "\n");
    while ($line !== false) {
        $lineLen = strlen($line);
        $path    = substr($line, 19, $lineLen - 45);
        $dateKey = substr($line, $lineLen - 23, 8);
        if (isset($pathIds[$path], $dateIds[$dateKey])) {
            ++$counts[$pathIds[$path] * $DATE_COUNT + $dateIds[$dateKey]];
        }
        $line = strtok("\n");
    }
}
$h5       = (hrtime(true) - $t0) / 1e6 / $ITERS;
$h5counts = $counts;
echo "H5  strtok+precomp+dual-isset:            " . number_format($h5, 3) . "ms  (" . number_format($h1 / $h5, 3) . "x)\n";

// ============================================================
// H6: strpos walk + precomputed offsets + dual isset
// ============================================================
$t0 = hrtime(true);
for ($iter = 0; $iter < $ITERS; $iter++) {
    $counts = array_fill(0, $pathCount * $DATE_COUNT, 0);
    $pos = 0;
    while (($nl = strpos($chunk, "\n", $pos)) !== false) {
        $lineLen = $nl - $pos;
        $path    = substr($chunk, $pos + 19, $lineLen - 45);
        $dateKey = substr($chunk, $pos + $lineLen - 23, 8);
        if (isset($pathIds[$path], $dateIds[$dateKey])) {
            ++$counts[$pathIds[$path] * $DATE_COUNT + $dateIds[$dateKey]];
        }
        $pos = $nl + 1;
    }
}
$h6       = (hrtime(true) - $t0) / 1e6 / $ITERS;
$h6counts = $counts;
echo "H6  strpos+precomp+dual-isset:            " . number_format($h6, 3) . "ms  (" . number_format($h1 / $h6, 3) . "x)\n";

// ============================================================
// H7: Eliminate the $dateIds hash lookup entirely.
// Replace "YY-MM-DD" string lookup with arithmetic:
//   build ymFlat[yymm_int] = base_date_id, then add dd-1
// "YY-MM-DD" bytes at offsets d0..d0+7:
//   yy = (ord(d0)   *10 + ord(d0+1)) - ('0'*11) = bytes decoded
//   mm = (ord(d0+3) *10 + ord(d0+4)) - ('0'*11)
//   dd = (ord(d0+6) *10 + ord(d0+7)) - ('0'*11)
//   '0'*11 = 48*11 = 528
// ymFlat[yy*100 + mm] = cumulative day offset for that year-month
// ============================================================
$ymFlat = [];
$rid = 0;
for ($y = 2020; $y <= 2026; $y++) {
    $yy = $y - 2000;
    for ($m = 1; $m <= 12; $m++) {
        $ymFlat[$yy * 100 + $m] = $rid;
        $maxDay = match ($m) {
            2 => ($y % 4 === 0) ? 29 : 28,
            4, 6, 9, 11 => 30,
            default => 31,
        };
        $rid += $maxDay;
    }
}

$t0 = hrtime(true);
for ($iter = 0; $iter < $ITERS; $iter++) {
    $counts = array_fill(0, $pathCount * $DATE_COUNT, 0);
    $line = strtok($chunk, "\n");
    while ($line !== false) {
        $lineLen = strlen($line);
        $path    = substr($line, 19, $lineLen - 45);
        $pathId  = $pathIds[$path] ?? null;
        if ($pathId !== null) {
            $d0   = $lineLen - 23;
            $yymm = (ord($line[$d0])     * 10 + ord($line[$d0 + 1]) - 528) * 100
                  +  ord($line[$d0 + 3]) * 10 + ord($line[$d0 + 4]) - 528;
            $dd   =  ord($line[$d0 + 6]) * 10 + ord($line[$d0 + 7]) - 528;
            if (isset($ymFlat[$yymm])) {
                ++$counts[$pathId * $DATE_COUNT + $ymFlat[$yymm] + $dd - 1];
            }
        }
        $line = strtok("\n");
    }
}
$h7       = (hrtime(true) - $t0) / 1e6 / $ITERS;
$h7counts = $counts;
echo "H7  strtok+cached-pathId+ymFlat:          " . number_format($h7, 3) . "ms  (" . number_format($h1 / $h7, 3) . "x)\n";

// ============================================================
// H8: H7 but skip the ymFlat isset guard entirely
// All dates in the file are valid (2020-2026), so if the path
// is known the date is always valid too — no guard needed.
// ============================================================
$t0 = hrtime(true);
for ($iter = 0; $iter < $ITERS; $iter++) {
    $counts = array_fill(0, $pathCount * $DATE_COUNT, 0);
    $line = strtok($chunk, "\n");
    while ($line !== false) {
        $lineLen = strlen($line);
        $path    = substr($line, 19, $lineLen - 45);
        $pathId  = $pathIds[$path] ?? null;
        if ($pathId !== null) {
            $d0   = $lineLen - 23;
            $yymm = (ord($line[$d0])     * 10 + ord($line[$d0 + 1]) - 528) * 100
                  +  ord($line[$d0 + 3]) * 10 + ord($line[$d0 + 4]) - 528;
            $dd   =  ord($line[$d0 + 6]) * 10 + ord($line[$d0 + 7]) - 528;
            ++$counts[$pathId * $DATE_COUNT + $ymFlat[$yymm] + $dd - 1];
        }
        $line = strtok("\n");
    }
}
$h8       = (hrtime(true) - $t0) / 1e6 / $ITERS;
$h8counts = $counts;
echo "H8  strtok+no-date-guard:                 " . number_format($h8, 3) . "ms  (" . number_format($h1 / $h8, 3) . "x)\n";

// ============================================================
// H9: strpos walk + cached pathId + ymFlat (no date guard)
// Combines the best of H2 (no line copy) + H8 (no date hash)
// ============================================================
$t0 = hrtime(true);
for ($iter = 0; $iter < $ITERS; $iter++) {
    $counts = array_fill(0, $pathCount * $DATE_COUNT, 0);
    $pos = 0;
    while (($nl = strpos($chunk, "\n", $pos)) !== false) {
        $lineLen = $nl - $pos;
        $path    = substr($chunk, $pos + 19, $lineLen - 45);
        $pathId  = $pathIds[$path] ?? null;
        if ($pathId !== null) {
            $d0   = $pos + $lineLen - 23;
            $yymm = (ord($chunk[$d0])     * 10 + ord($chunk[$d0 + 1]) - 528) * 100
                  +  ord($chunk[$d0 + 3]) * 10 + ord($chunk[$d0 + 4]) - 528;
            $dd   =  ord($chunk[$d0 + 6]) * 10 + ord($chunk[$d0 + 7]) - 528;
            ++$counts[$pathId * $DATE_COUNT + $ymFlat[$yymm] + $dd - 1];
        }
        $pos = $nl + 1;
    }
}
$h9       = (hrtime(true) - $t0) / 1e6 / $ITERS;
$h9counts = $counts;
echo "H9  strpos+cached-pathId+no-date-guard:   " . number_format($h9, 3) . "ms  (" . number_format($h1 / $h9, 3) . "x)\n";

// ============================================================
// H10: strtok + ymFlat but read date bytes from CHUNK-end
// In strtok the line IS available, but can we use negative
// substr offsets to avoid computing lineLen - 23?
// substr($line, -23, 8) instead of substr($line, lineLen-23, 8)
// PHP negative offsets call strlen internally, so it's the same —
// but it may be slightly faster due to different code path.
// ============================================================
$t0 = hrtime(true);
for ($iter = 0; $iter < $ITERS; $iter++) {
    $counts = array_fill(0, $pathCount * $DATE_COUNT, 0);
    $line = strtok($chunk, "\n");
    while ($line !== false) {
        $lineLen = strlen($line);
        $path    = substr($line, 19, $lineLen - 45);
        $pathId  = $pathIds[$path] ?? null;
        if ($pathId !== null) {
            // Use negative offsets — PHP computes strlen internally but
            // may short-circuit some bounds checks
            $datePart = substr($line, -23, 8); // "YY-MM-DD"
            $yymm = (ord($datePart[0]) * 10 + ord($datePart[1]) - 528) * 100
                  +  ord($datePart[3]) * 10 + ord($datePart[4]) - 528;
            $dd   =  ord($datePart[6]) * 10 + ord($datePart[7]) - 528;
            ++$counts[$pathId * $DATE_COUNT + $ymFlat[$yymm] + $dd - 1];
        }
        $line = strtok("\n");
    }
}
$h10       = (hrtime(true) - $t0) / 1e6 / $ITERS;
$h10counts = $counts;
echo "H10 strtok+neg-offset+ymFlat:             " . number_format($h10, 3) . "ms  (" . number_format($h1 / $h10, 3) . "x)\n";

// ============================================================
// H11: Avoid the $pathId double hash lookup in H5's dual isset:
// isset($pathIds[$p], $dateIds[$d]) does TWO hash lookups for path
// (once for isset, once for value). Cache it instead.
// But also use ymFlat. Best single-pass combination.
// H7 already does this — this is just confirming it's the sweet spot.
// Try: look up pathId once, then look up dateId once (not ymFlat).
// ============================================================
$t0 = hrtime(true);
for ($iter = 0; $iter < $ITERS; $iter++) {
    $counts = array_fill(0, $pathCount * $DATE_COUNT, 0);
    $line = strtok($chunk, "\n");
    while ($line !== false) {
        $lineLen = strlen($line);
        $path    = substr($line, 19, $lineLen - 45);
        $pathId  = $pathIds[$path] ?? null;
        if ($pathId !== null) {
            $dateKey = substr($line, $lineLen - 23, 8);
            $dateId  = $dateIds[$dateKey] ?? null;
            if ($dateId !== null) {
                ++$counts[$pathId * $DATE_COUNT + $dateId];
            }
        }
        $line = strtok("\n");
    }
}
$h11       = (hrtime(true) - $t0) / 1e6 / $ITERS;
$h11counts = $counts;
echo "H11 strtok+cached-both-ids:               " . number_format($h11, 3) . "ms  (" . number_format($h1 / $h11, 3) . "x)\n";

// ============================================================
// H12: Like H11 but no date guard (trusted data)
// ============================================================
$t0 = hrtime(true);
for ($iter = 0; $iter < $ITERS; $iter++) {
    $counts = array_fill(0, $pathCount * $DATE_COUNT, 0);
    $line = strtok($chunk, "\n");
    while ($line !== false) {
        $lineLen = strlen($line);
        $path    = substr($line, 19, $lineLen - 45);
        $pathId  = $pathIds[$path] ?? null;
        if ($pathId !== null) {
            $dateKey = substr($line, $lineLen - 23, 8);
            ++$counts[$pathId * $DATE_COUNT + $dateIds[$dateKey]];
        }
        $line = strtok("\n");
    }
}
$h12       = (hrtime(true) - $t0) / 1e6 / $ITERS;
$h12counts = $counts;
echo "H12 strtok+cached-pathId+no-guards:       " . number_format($h12, 3) . "ms  (" . number_format($h1 / $h12, 3) . "x)\n";

// ============================================================
// H13: strpos walk + H12 approach
// ============================================================
$t0 = hrtime(true);
for ($iter = 0; $iter < $ITERS; $iter++) {
    $counts = array_fill(0, $pathCount * $DATE_COUNT, 0);
    $pos = 0;
    while (($nl = strpos($chunk, "\n", $pos)) !== false) {
        $lineLen = $nl - $pos;
        $path    = substr($chunk, $pos + 19, $lineLen - 45);
        $pathId  = $pathIds[$path] ?? null;
        if ($pathId !== null) {
            $dateKey = substr($chunk, $pos + $lineLen - 23, 8);
            ++$counts[$pathId * $DATE_COUNT + $dateIds[$dateKey]];
        }
        $pos = $nl + 1;
    }
}
$h13       = (hrtime(true) - $t0) / 1e6 / $ITERS;
$h13counts = $counts;
echo "H13 strpos+cached-pathId+no-guards:       " . number_format($h13, 3) . "ms  (" . number_format($h1 / $h13, 3) . "x)\n";

// ============================================================
// CORRECTNESS CHECK
// ============================================================
echo "\n--- Correctness (vs H1) ---\n";
$total = array_sum($h1counts);
echo "H1 total count: $total\n";
echo "H2  match: " . ($h2counts  === $h1counts ? 'YES' : 'NO (DIFF!)') . "\n";
echo "H3  match: " . ($h3counts  === $h1counts ? 'YES' : 'NO (DIFF!)') . "\n";
echo "H4  match: " . ($h4counts  === $h1counts ? 'YES' : 'NO (DIFF!)') . "\n";
echo "H5  match: " . ($h5counts  === $h1counts ? 'YES' : 'NO (DIFF!)') . "\n";
echo "H6  match: " . ($h6counts  === $h1counts ? 'YES' : 'NO (DIFF!)') . "\n";
echo "H7  match: " . ($h7counts  === $h1counts ? 'YES' : 'NO (DIFF!)') . "\n";
echo "H8  match: " . ($h8counts  === $h1counts ? 'YES' : 'NO (DIFF!)') . "\n";
echo "H9  match: " . ($h9counts  === $h1counts ? 'YES' : 'NO (DIFF!)') . "\n";
echo "H10 match: " . ($h10counts === $h1counts ? 'YES' : 'NO (DIFF!)') . "\n";
echo "H11 match: " . ($h11counts === $h1counts ? 'YES' : 'NO (DIFF!)') . "\n";
echo "H12 match: " . ($h12counts === $h1counts ? 'YES' : 'NO (DIFF!)') . "\n";
echo "H13 match: " . ($h13counts === $h1counts ? 'YES' : 'NO (DIFF!)') . "\n";

// ============================================================
// SUMMARY TABLE
// ============================================================
echo "\n=== SUMMARY ===\n\n";
printf("%-48s %10s %10s\n", "Method", "ms/iter", "Speedup");
echo str_repeat('-', 70) . "\n";
printf("%-48s %10.3f %10s\n",    "H1  strtok+strlen [CURRENT]",               $h1,  "1.000x");
printf("%-48s %10.3f %10.3fx\n", "H2  strpos walk (no line copy)",             $h2,  $h1 / $h2);
printf("%-48s %10.3f %10.3fx\n", "H3  explode+foreach",                        $h3,  $h1 / $h3);
printf("%-48s %10.3f %10.3fx\n", "H4  strtok+precomputed offsets",             $h4,  $h1 / $h4);
printf("%-48s %10.3f %10.3fx\n", "H5  strtok+precomp+dual-isset",              $h5,  $h1 / $h5);
printf("%-48s %10.3f %10.3fx\n", "H6  strpos+precomp+dual-isset",              $h6,  $h1 / $h6);
printf("%-48s %10.3f %10.3fx\n", "H7  strtok+cached-pathId+ymFlat",            $h7,  $h1 / $h7);
printf("%-48s %10.3f %10.3fx\n", "H8  strtok+no-date-guard",                   $h8,  $h1 / $h8);
printf("%-48s %10.3f %10.3fx\n", "H9  strpos+cached-pathId+no-date-guard",     $h9,  $h1 / $h9);
printf("%-48s %10.3f %10.3fx\n", "H10 strtok+neg-offset+ymFlat",               $h10, $h1 / $h10);
printf("%-48s %10.3f %10.3fx\n", "H11 strtok+cached-both-ids",                 $h11, $h1 / $h11);
printf("%-48s %10.3f %10.3fx\n", "H12 strtok+cached-pathId+no-guards",         $h12, $h1 / $h12);
printf("%-48s %10.3f %10.3fx\n", "H13 strpos+cached-pathId+no-guards",         $h13, $h1 / $h13);