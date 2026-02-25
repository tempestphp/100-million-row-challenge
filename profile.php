<?php

require_once __DIR__ . '/vendor/autoload.php';

$inputPath = $argv[1] ?? __DIR__ . '/data/data.csv';
$outputPath = __DIR__ . '/data/profile-output.json';

$t0 = hrtime(true);

// Phase 1: Read + Parse + Aggregate
$data = [];
$handle = fopen($inputPath, 'r');
$remainder = '';
$chunkSize = 1048576;

while (($chunk = fread($handle, $chunkSize)) !== false && $chunk !== '') {
    $chunk = $remainder . $chunk;
    $lastNewline = strrpos($chunk, "\n");
    if ($lastNewline === false) { $remainder = $chunk; continue; }
    $remainder = ($lastNewline + 1 < strlen($chunk)) ? substr($chunk, $lastNewline + 1) : '';
    $len = $lastNewline;
    $offset = 0;
    while ($offset < $len) {
        $lineEnd = strpos($chunk, "\n", $offset);
        $commaPos = $lineEnd - 26;
        $path = substr($chunk, $offset + 19, $commaPos - $offset - 19);
        $date = substr($chunk, $commaPos + 1, 10);
        if (!isset($data[$path][$date])) {
            $data[$path][$date] = 1;
        } else {
            $data[$path][$date]++;
        }
        $offset = $lineEnd + 1;
    }
}
if ($remainder !== '') {
    $lineLen = strlen($remainder);
    $commaPos = $lineLen - 25;
    $path = substr($remainder, 19, $commaPos - 19);
    $date = substr($remainder, $commaPos + 1, 10);
    if (!isset($data[$path][$date])) { $data[$path][$date] = 1; } else { $data[$path][$date]++; }
}
fclose($handle);

$t1 = hrtime(true);

// Phase 2: Sort
foreach ($data as &$dates) {
    ksort($dates);
}
unset($dates);

$t2 = hrtime(true);

// Phase 3: JSON encode
$json = json_encode($data, JSON_PRETTY_PRINT);

$t3 = hrtime(true);

// Phase 4: Write
file_put_contents($outputPath, $json);

$t4 = hrtime(true);

fprintf(STDERR, "Read+Parse+Aggregate: %.4fs\n", ($t1 - $t0) / 1e9);
fprintf(STDERR, "Sort:                 %.4fs\n", ($t2 - $t1) / 1e9);
fprintf(STDERR, "JSON encode:          %.4fs\n", ($t3 - $t2) / 1e9);
fprintf(STDERR, "File write:           %.4fs\n", ($t4 - $t3) / 1e9);
fprintf(STDERR, "Total:                %.4fs\n", ($t4 - $t0) / 1e9);
fprintf(STDERR, "\nPaths: %d\n", count($data));
$totalDates = 0;
foreach ($data as $d) $totalDates += count($d);
fprintf(STDERR, "Total date entries: %d\n", $totalDates);
