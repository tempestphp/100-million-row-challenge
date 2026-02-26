<?php

/**
 * POC: Arithmetic date ID computation variants.
 * Tests replacing the 2557-entry string-keyed dateChars hash lookup
 * with smaller/faster alternatives.
 *
 * Single-process, parse-only (no output), bucket-based counting.
 * Usage: php poc/claude-arith.php [data-file] [variant]
 * Variants: baseline, arith5, arith6, arithfull
 */

gc_disable();

$inputPath = $argv[1] ?? 'data/data.csv';
$variant = $argv[2] ?? 'baseline';

// ─── Arithmetic date generation (same as Parser.php) ───

$dateLookup = [];
$numDates = 0;

for ($y = 20; $y <= 26; $y++) {
    for ($m = 1; $m <= 12; $m++) {
        $maxD = match ($m) {
            2 => ($y % 4 === 0) ? 29 : 28,
            4, 6, 9, 11 => 30,
            default => 31,
        };
        $ms = ($m < 10 ? '0' : '') . $m;
        $ym = $y . '-' . $ms . '-';
        for ($d = 1; $d <= $maxD; $d++) {
            $key = $ym . (($d < 10 ? '0' : '') . $d);
            $dateLookup[$key] = $numDates;
            $numDates++;
        }
    }
}

// Baseline: 8-char string-keyed dateChars (current Parser.php approach)
$dateChars = [];
foreach ($dateLookup as $key => $id) {
    $dateChars[$key] = chr($id & 0xFF) . chr($id >> 8);
}

// Variant arith5: 5-char "YY-MM" → base offset, then + day
$ymBase = [];
$total = 0;
for ($y = 20; $y <= 26; $y++) {
    for ($m = 1; $m <= 12; $m++) {
        $ms = ($m < 10 ? '0' : '') . $m;
        $ymKey = (($y < 10 ? '0' : '') . $y) . '-' . $ms;
        $ymBase[$ymKey] = $total;
        $maxD = match ($m) {
            2 => ($y % 4 === 0) ? 29 : 28,
            4, 6, 9, 11 => 30,
            default => 31,
        };
        $total += $maxD;
    }
}
// Int-indexed dateId → 2-byte char
$dateIdChars = [];
for ($i = 0; $i < $numDates; $i++) {
    $dateIdChars[$i] = chr($i & 0xFF) . chr($i >> 8);
}

// Variant arith6: 6-byte key without dashes (YYMMDD)
$dateChars6 = [];
foreach ($dateLookup as $key => $id) {
    // $key is "YY-MM-DD", strip dashes → "YYMMDD"
    $key6 = $key[0] . $key[1] . $key[3] . $key[4] . $key[6] . $key[7];
    $dateChars6[$key6] = chr($id & 0xFF) . chr($id >> 8);
}

// Variant arithfull: cumulative days table indexed by integer
$cumDays = [];
$total2 = 0;
for ($y = 20; $y <= 26; $y++) {
    for ($m = 1; $m <= 12; $m++) {
        $cumDays[($y - 20) * 12 + ($m - 1)] = $total2;
        $maxD = match ($m) {
            2 => ($y % 4 === 0) ? 29 : 28,
            4, 6, 9, 11 => 30,
            default => 31,
        };
        $total2 += $maxD;
    }
}

// ─── Slug discovery ───

$fileSize = filesize($inputPath);
$slugIndex = [];
$numSlugs = 0;

$fh = fopen($inputPath, 'rb');
stream_set_read_buffer($fh, 0);
$disc = fread($fh, min(8_388_608, $fileSize));
fclose($fh);

$pos = 0;
$len = strlen($disc);
while ($pos < $len) {
    $nlPos = strpos($disc, "\n", $pos);
    if ($nlPos === false) break;
    $slug = substr($disc, $pos + 25, $nlPos - $pos - 51);
    if (!isset($slugIndex[$slug])) {
        $slugIndex[$slug] = $numSlugs;
        $numSlugs++;
    }
    $pos = $nlPos + 1;
}
unset($disc);

$buckets = array_fill(0, $numSlugs, '');

// ─── Parse-only benchmark ───

$t = microtime(true);

$fh = fopen($inputPath, 'rb');
stream_set_read_buffer($fh, 0);
$remaining = $fileSize;
$bufSize = 8_388_608;

while ($remaining > 0) {
    $raw = fread($fh, $remaining > $bufSize ? $bufSize : $remaining);
    $len = strlen($raw);
    if ($len === 0) break;
    $remaining -= $len;

    $end = strrpos($raw, "\n");
    if ($end === false) break;

    $tail = $len - $end - 1;
    if ($tail > 0) {
        fseek($fh, -$tail, SEEK_CUR);
        $remaining += $tail;
    }

    $p = 0;

    if ($variant === 'baseline') {
        // Current approach: 8-char string key hash lookup
        $fence = $end - 720;
        while ($p < $fence) {
            $nl = strpos($raw, "\n", $p + 52);
            $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateChars[substr($raw, $nl - 23, 8)];
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateChars[substr($raw, $nl - 23, 8)];
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateChars[substr($raw, $nl - 23, 8)];
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateChars[substr($raw, $nl - 23, 8)];
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateChars[substr($raw, $nl - 23, 8)];
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateChars[substr($raw, $nl - 23, 8)];
            $p = $nl + 1;
        }
        while ($p < $end) {
            $nl = strpos($raw, "\n", $p + 52);
            if ($nl === false) break;
            $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateChars[substr($raw, $nl - 23, 8)];
            $p = $nl + 1;
        }

    } elseif ($variant === 'arith5') {
        // 5-char "YY-MM" lookup (84 entries) + day ord arithmetic
        $fence = $end - 720;
        while ($p < $fence) {
            $nl = strpos($raw, "\n", $p + 52);
            $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateIdChars[$ymBase[substr($raw, $nl - 23, 5)] + (ord($raw[$nl - 17]) - 48) * 10 + ord($raw[$nl - 16]) - 49];
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateIdChars[$ymBase[substr($raw, $nl - 23, 5)] + (ord($raw[$nl - 17]) - 48) * 10 + ord($raw[$nl - 16]) - 49];
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateIdChars[$ymBase[substr($raw, $nl - 23, 5)] + (ord($raw[$nl - 17]) - 48) * 10 + ord($raw[$nl - 16]) - 49];
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateIdChars[$ymBase[substr($raw, $nl - 23, 5)] + (ord($raw[$nl - 17]) - 48) * 10 + ord($raw[$nl - 16]) - 49];
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateIdChars[$ymBase[substr($raw, $nl - 23, 5)] + (ord($raw[$nl - 17]) - 48) * 10 + ord($raw[$nl - 16]) - 49];
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateIdChars[$ymBase[substr($raw, $nl - 23, 5)] + (ord($raw[$nl - 17]) - 48) * 10 + ord($raw[$nl - 16]) - 49];
            $p = $nl + 1;
        }
        while ($p < $end) {
            $nl = strpos($raw, "\n", $p + 52);
            if ($nl === false) break;
            $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateIdChars[$ymBase[substr($raw, $nl - 23, 5)] + (ord($raw[$nl - 17]) - 48) * 10 + ord($raw[$nl - 16]) - 49];
            $p = $nl + 1;
        }

    } elseif ($variant === 'arith6') {
        // 6-byte key without dashes (YYMMDD) — same table size, shorter hash
        $fence = $end - 720;
        while ($p < $fence) {
            $nl = strpos($raw, "\n", $p + 52);
            $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateChars6[$raw[$nl - 23] . $raw[$nl - 22] . $raw[$nl - 20] . $raw[$nl - 19] . $raw[$nl - 17] . $raw[$nl - 16]];
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateChars6[$raw[$nl - 23] . $raw[$nl - 22] . $raw[$nl - 20] . $raw[$nl - 19] . $raw[$nl - 17] . $raw[$nl - 16]];
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateChars6[$raw[$nl - 23] . $raw[$nl - 22] . $raw[$nl - 20] . $raw[$nl - 19] . $raw[$nl - 17] . $raw[$nl - 16]];
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateChars6[$raw[$nl - 23] . $raw[$nl - 22] . $raw[$nl - 20] . $raw[$nl - 19] . $raw[$nl - 17] . $raw[$nl - 16]];
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateChars6[$raw[$nl - 23] . $raw[$nl - 22] . $raw[$nl - 20] . $raw[$nl - 19] . $raw[$nl - 17] . $raw[$nl - 16]];
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateChars6[$raw[$nl - 23] . $raw[$nl - 22] . $raw[$nl - 20] . $raw[$nl - 19] . $raw[$nl - 17] . $raw[$nl - 16]];
            $p = $nl + 1;
        }
        while ($p < $end) {
            $nl = strpos($raw, "\n", $p + 52);
            if ($nl === false) break;
            $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateChars6[$raw[$nl - 23] . $raw[$nl - 22] . $raw[$nl - 20] . $raw[$nl - 19] . $raw[$nl - 17] . $raw[$nl - 16]];
            $p = $nl + 1;
        }

    } elseif ($variant === 'arithfull') {
        // Full arithmetic: 6 ord() calls + cumDays table + dateIdChars
        $fence = $end - 720;
        while ($p < $fence) {
            $nl = strpos($raw, "\n", $p + 52);
            $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateIdChars[$cumDays[((ord($raw[$nl - 23]) - 48) * 10 + ord($raw[$nl - 22]) - 68) * 12 + (ord($raw[$nl - 20]) - 48) * 10 + ord($raw[$nl - 19]) - 49] + (ord($raw[$nl - 17]) - 48) * 10 + ord($raw[$nl - 16]) - 49];
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateIdChars[$cumDays[((ord($raw[$nl - 23]) - 48) * 10 + ord($raw[$nl - 22]) - 68) * 12 + (ord($raw[$nl - 20]) - 48) * 10 + ord($raw[$nl - 19]) - 49] + (ord($raw[$nl - 17]) - 48) * 10 + ord($raw[$nl - 16]) - 49];
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateIdChars[$cumDays[((ord($raw[$nl - 23]) - 48) * 10 + ord($raw[$nl - 22]) - 68) * 12 + (ord($raw[$nl - 20]) - 48) * 10 + ord($raw[$nl - 19]) - 49] + (ord($raw[$nl - 17]) - 48) * 10 + ord($raw[$nl - 16]) - 49];
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateIdChars[$cumDays[((ord($raw[$nl - 23]) - 48) * 10 + ord($raw[$nl - 22]) - 68) * 12 + (ord($raw[$nl - 20]) - 48) * 10 + ord($raw[$nl - 19]) - 49] + (ord($raw[$nl - 17]) - 48) * 10 + ord($raw[$nl - 16]) - 49];
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateIdChars[$cumDays[((ord($raw[$nl - 23]) - 48) * 10 + ord($raw[$nl - 22]) - 68) * 12 + (ord($raw[$nl - 20]) - 48) * 10 + ord($raw[$nl - 19]) - 49] + (ord($raw[$nl - 17]) - 48) * 10 + ord($raw[$nl - 16]) - 49];
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateIdChars[$cumDays[((ord($raw[$nl - 23]) - 48) * 10 + ord($raw[$nl - 22]) - 68) * 12 + (ord($raw[$nl - 20]) - 48) * 10 + ord($raw[$nl - 19]) - 49] + (ord($raw[$nl - 17]) - 48) * 10 + ord($raw[$nl - 16]) - 49];
            $p = $nl + 1;
        }
        while ($p < $end) {
            $nl = strpos($raw, "\n", $p + 52);
            if ($nl === false) break;
            $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateIdChars[$cumDays[((ord($raw[$nl - 23]) - 48) * 10 + ord($raw[$nl - 22]) - 68) * 12 + (ord($raw[$nl - 20]) - 48) * 10 + ord($raw[$nl - 19]) - 49] + (ord($raw[$nl - 17]) - 48) * 10 + ord($raw[$nl - 16]) - 49];
            $p = $nl + 1;
        }
    }
}

fclose($fh);

// Verify: count total entries in buckets
$totalEntries = 0;
for ($s = 0; $s < $numSlugs; $s++) {
    $totalEntries += strlen($buckets[$s]) >> 1; // each entry is 2 bytes
}

$elapsed = microtime(true) - $t;
fprintf(STDERR, "%-12s %.3fs  (single-process parse-only, %s entries)\n", $variant . ':', $elapsed, number_format($totalEntries));
