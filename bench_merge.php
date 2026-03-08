<?php

$pathCount = 268;
$DATE_COUNT = 2555;
$totalSize = $pathCount * $DATE_COUNT;
$NUM_WORKERS = 16;

echo "=== Merge + JSON Benchmarks ===\n";
echo "totalSize=$totalSize, workers=$NUM_WORKERS\n\n";

// Build fake worker binary output
$workerData = [];
for ($w = 0; $w < $NUM_WORKERS; $w++) {
    $wc = array_fill(0, $totalSize, 0);
    for ($i = 0; $i < 50000; $i++) {
        $wc[rand(0, $totalSize - 1)]++;
    }
    $workerData[$w] = pack('V*', ...$wc);
}

// Build paths and dates for JSON output
$paths = [];
for ($p = 0; $p < $pathCount; $p++) {
    $paths[$p] = '/blog/article-' . $p . '-some-realistic-slug';
}

$allDates = [];
$id = 0;
for ($y = 2020; $y <= 2026; $y++) {
    for ($m = 1; $m <= 12; $m++) {
        $maxDay = match ($m) { 2 => 28, 4,6,9,11 => 30, default => 31 };
        for ($d = 1; $d <= $maxDay; $d++) {
            $allDates[$id++] = sprintf('%d-%02d-%02d', $y, $m, $d);
        }
    }
}

$iters = 50;
$outputFile = '/tmp/bench_out.json';

// ============================================================
// MERGE BENCHMARKS
// ============================================================

echo "--- MERGE ---\n\n";

// M1: Current approach - fill zeros, foreach unpack, j++ counter
$t0 = hrtime(true);
for ($iter = 0; $iter < $iters; $iter++) {
    $merged = array_fill(0, $totalSize, 0);
    foreach ($workerData as $data) {
        $wc = unpack('V*', $data);
        $j = 0;
        foreach ($wc as $v) {
            $merged[$j++] += $v;
        }
    }
}
$m1 = (hrtime(true) - $t0) / 1e6 / $iters;
echo "M1 fill+foreach j++:          " . number_format($m1, 2) . "ms  [CURRENT]\n";

// M2: Start with first worker unpacked, add rest with for($i)
$t0 = hrtime(true);
for ($iter = 0; $iter < $iters; $iter++) {
    $merged = array_values(unpack('V*', $workerData[0]));
    for ($w = 1; $w < $NUM_WORKERS; $w++) {
        $wc = array_values(unpack('V*', $workerData[$w]));
        for ($i = 0; $i < $totalSize; $i++) {
            $merged[$i] += $wc[$i];
        }
    }
}
$m2 = (hrtime(true) - $t0) / 1e6 / $iters;
echo "M2 start-with-first for(\$i):  " . number_format($m2, 2) . "ms  (" . number_format($m1 / $m2, 2) . "x)\n";

// M3: 1-indexed (skip array_values), direct for($i=1)
$t0 = hrtime(true);
for ($iter = 0; $iter < $iters; $iter++) {
    $merged = unpack('V*', $workerData[0]);
    for ($w = 1; $w < $NUM_WORKERS; $w++) {
        $wc = unpack('V*', $workerData[$w]);
        for ($i = 1; $i <= $totalSize; $i++) {
            $merged[$i] += $wc[$i];
        }
    }
}
$m3 = (hrtime(true) - $t0) / 1e6 / $iters;
echo "M3 1-indexed skip array_val:  " . number_format($m3, 2) . "ms  (" . number_format($m1 / $m3, 2) . "x)\n";

// M4: Parent processes its own chunk (no write/read for one worker)
//     15 workers write files, parent processes chunk 15 itself
$t0 = hrtime(true);
for ($iter = 0; $iter < $iters; $iter++) {
    // Parent already has its counts as a PHP array (no file round-trip)
    $merged = array_values(unpack('V*', $workerData[$NUM_WORKERS - 1]));
    // Merge 15 file-based workers
    for ($w = 0; $w < $NUM_WORKERS - 1; $w++) {
        $wc = array_values(unpack('V*', $workerData[$w]));
        for ($i = 0; $i < $totalSize; $i++) {
            $merged[$i] += $wc[$i];
        }
    }
}
$m4 = (hrtime(true) - $t0) / 1e6 / $iters;
echo "M4 parent owns last chunk:    " . number_format($m4, 2) . "ms  (" . number_format($m1 / $m4, 2) . "x)\n";

// M5: Isolate unpack cost vs addition cost
$t0 = hrtime(true);
for ($iter = 0; $iter < $iters; $iter++) {
    foreach ($workerData as $data) {
        $wc = unpack('V*', $data);
    }
}
$unpackOnly = (hrtime(true) - $t0) / 1e6 / $iters;

$preUnpacked = [];
foreach ($workerData as $data) {
    $preUnpacked[] = unpack('V*', $data);
}
$t0 = hrtime(true);
for ($iter = 0; $iter < $iters; $iter++) {
    $merged = array_fill(0, $totalSize, 0);
    foreach ($preUnpacked as $wc) {
        $j = 0;
        foreach ($wc as $v) {
            $merged[$j++] += $v;
        }
    }
}
$addOnly = (hrtime(true) - $t0) / 1e6 / $iters;
echo "\n  unpack only:    " . number_format($unpackOnly, 2) . "ms\n";
echo "  addition only:  " . number_format($addOnly, 2) . "ms\n";
echo "  (together = M1 because they're sequential)\n";

// ============================================================
// JSON OUTPUT BENCHMARKS
// ============================================================

echo "\n--- JSON OUTPUT ---\n\n";

// Get a realistic merged counts array
$merged = array_fill(0, $totalSize, 0);
foreach ($workerData as $data) {
    $wc = unpack('V*', $data);
    $j = 0;
    foreach ($wc as $v) {
        $merged[$j++] += $v;
    }
}

// Pre-compute reusable structures
$escapedPaths = [];
for ($p = 0; $p < $pathCount; $p++) {
    $escapedPaths[$p] = '"' . str_replace('/', '\/', $paths[$p]) . '": {';
}
$datePrefixes = [];
for ($d = 0; $d < $DATE_COUNT; $d++) {
    $datePrefixes[$d] = '        "' . $allDates[$d] . '": ';
}

$itersJson = 300;

// J1: Current - build nested PHP array + json_encode
$t0 = hrtime(true);
for ($iter = 0; $iter < $itersJson; $iter++) {
    $out = [];
    for ($p = 0; $p < $pathCount; $p++) {
        $base = $p * $DATE_COUNT;
        $pathDates = [];
        for ($d = 0; $d < $DATE_COUNT; $d++) {
            $c = $merged[$base + $d];
            if ($c > 0) {
                $pathDates[$allDates[$d]] = $c;
            }
        }
        if ($pathDates !== []) {
            $out[$paths[$p]] = $pathDates;
        }
    }
    file_put_contents($outputFile, json_encode($out, JSON_PRETTY_PRINT));
}
$j1 = (hrtime(true) - $t0) / 1e6 / $itersJson;
echo "J1 build array + json_encode:     " . number_format($j1, 2) . "ms  [CURRENT]\n";

// J2: Manual fwrite with 1MB write buffer, escape inline
$t0 = hrtime(true);
for ($iter = 0; $iter < $itersJson; $iter++) {
    $fh = fopen($outputFile, 'wb');
    stream_set_write_buffer($fh, 1_048_576);
    fwrite($fh, '{');
    $firstPath = true;
    for ($p = 0; $p < $pathCount; $p++) {
        $base = $p * $DATE_COUNT;
        $entries = [];
        for ($d = 0; $d < $DATE_COUNT; $d++) {
            $c = $merged[$base + $d];
            if ($c > 0) {
                $entries[] = '        "' . $allDates[$d] . '": ' . $c;
            }
        }
        if ($entries === []) continue;
        $sep = $firstPath ? "\n    " : ",\n    ";
        $firstPath = false;
        fwrite($fh, $sep . '"' . str_replace('/', '\/', $paths[$p]) . '": {' . "\n" . implode(",\n", $entries) . "\n    }");
    }
    fwrite($fh, "\n}");
    fclose($fh);
}
$j2 = (hrtime(true) - $t0) / 1e6 / $itersJson;
echo "J2 manual fwrite inline escape:   " . number_format($j2, 2) . "ms  (" . number_format($j1 / $j2, 2) . "x)\n";

// J3: Manual fwrite with pre-escaped paths + pre-built date prefixes
$t0 = hrtime(true);
for ($iter = 0; $iter < $itersJson; $iter++) {
    $fh = fopen($outputFile, 'wb');
    stream_set_write_buffer($fh, 1_048_576);
    fwrite($fh, '{');
    $firstPath = true;
    for ($p = 0; $p < $pathCount; $p++) {
        $base = $p * $DATE_COUNT;
        $entries = [];
        for ($d = 0; $d < $DATE_COUNT; $d++) {
            $c = $merged[$base + $d];
            if ($c > 0) {
                $entries[] = $datePrefixes[$d] . $c;
            }
        }
        if ($entries === []) continue;
        $sep = $firstPath ? "\n    " : ",\n    ";
        $firstPath = false;
        fwrite($fh, $sep . $escapedPaths[$p] . "\n" . implode(",\n", $entries) . "\n    }");
    }
    fwrite($fh, "\n}");
    fclose($fh);
}
$j3 = (hrtime(true) - $t0) / 1e6 / $itersJson;
echo "J3 pre-escaped + fwrite:          " . number_format($j3, 2) . "ms  (" . number_format($j1 / $j3, 2) . "x)\n";

// J4: Build full string in memory, single file_put_contents
$t0 = hrtime(true);
for ($iter = 0; $iter < $itersJson; $iter++) {
    $buf = '{';
    $firstPath = true;
    for ($p = 0; $p < $pathCount; $p++) {
        $base = $p * $DATE_COUNT;
        $entries = [];
        for ($d = 0; $d < $DATE_COUNT; $d++) {
            $c = $merged[$base + $d];
            if ($c > 0) {
                $entries[] = $datePrefixes[$d] . $c;
            }
        }
        if ($entries === []) continue;
        $sep = $firstPath ? "\n    " : ",\n    ";
        $firstPath = false;
        $buf .= $sep . $escapedPaths[$p] . "\n" . implode(",\n", $entries) . "\n    }";
    }
    $buf .= "\n}";
    file_put_contents($outputFile, $buf);
}
$j4 = (hrtime(true) - $t0) / 1e6 / $itersJson;
echo "J4 build string + single write:   " . number_format($j4, 2) . "ms  (" . number_format($j1 / $j4, 2) . "x)\n";

// J5: Pre-escaped + fwrite but use larger per-path buffer (reduce fwrite calls)
$t0 = hrtime(true);
for ($iter = 0; $iter < $itersJson; $iter++) {
    $fh = fopen($outputFile, 'wb');
    stream_set_write_buffer($fh, 1_048_576);
    fwrite($fh, '{');
    $firstPath = true;
    for ($p = 0; $p < $pathCount; $p++) {
        $base = $p * $DATE_COUNT;
        $buf = '';
        $hasEntry = false;
        for ($d = 0; $d < $DATE_COUNT; $d++) {
            $c = $merged[$base + $d];
            if ($c > 0) {
                $buf .= ($hasEntry ? ",\n" : '') . $datePrefixes[$d] . $c;
                $hasEntry = true;
            }
        }
        if (!$hasEntry) continue;
        $sep = $firstPath ? "\n    " : ",\n    ";
        $firstPath = false;
        fwrite($fh, $sep . $escapedPaths[$p] . "\n" . $buf . "\n    }");
    }
    fwrite($fh, "\n}");
    fclose($fh);
}
$j5 = (hrtime(true) - $t0) / 1e6 / $itersJson;
echo "J5 pre-escaped + concat+fwrite:   " . number_format($j5, 2) . "ms  (" . number_format($j1 / $j5, 2) . "x)\n";

// ============================================================
// SUMMARY
// ============================================================

echo "\n=== SUMMARY ===\n\n";
printf("%-40s %10s %10s\n", "Method", "ms/iter", "Speedup");
echo str_repeat('-', 62) . "\n";
printf("%-40s %10.2f %10s\n",  "MERGE: fill+foreach j++ [current]", $m1, "1.00x");
printf("%-40s %10.2f %10.2fx\n", "MERGE: start-with-first for(i)", $m2, $m1/$m2);
printf("%-40s %10.2f %10.2fx\n", "MERGE: 1-indexed skip array_values", $m3, $m1/$m3);
printf("%-40s %10.2f %10.2fx\n", "MERGE: parent owns last chunk", $m4, $m1/$m4);
echo "\n";
printf("%-40s %10.2f %10s\n",  "JSON: build array+json_encode [current]", $j1, "1.00x");
printf("%-40s %10.2f %10.2fx\n", "JSON: manual fwrite inline", $j2, $j1/$j2);
printf("%-40s %10.2f %10.2fx\n", "JSON: pre-escaped + fwrite", $j3, $j1/$j3);
printf("%-40s %10.2f %10.2fx\n", "JSON: build string + single write", $j4, $j1/$j4);
printf("%-40s %10.2f %10.2fx\n", "JSON: pre-escaped + concat+fwrite", $j5, $j1/$j5);

@unlink($outputFile);