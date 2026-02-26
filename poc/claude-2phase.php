<?php

/**
 * POC: Two-phase pipeline — workers emit compact binary (slugId, dateId) pairs,
 * parent does vectorized aggregation pass.
 *
 * Hypothesis: tighter parse loop (no bucket concat) could offset IPC increase.
 * Reality check: IPC volume goes from ~2.7MB to ~40MB per worker.
 *
 * Single-process, parse-only benchmark.
 * Usage: php poc/claude-2phase.php [data-file] [variant]
 * Variants: baseline (buckets), 2phase (emit pairs + aggregate)
 */

gc_disable();

$inputPath = $argv[1] ?? 'data/data.csv';
$variant = $argv[2] ?? 'baseline';

// ─── Date + slug setup (same as Parser.php) ───

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

$dateChars = [];
foreach ($dateLookup as $key => $id) {
    $dateChars[$key] = chr($id & 0xFF) . chr($id >> 8);
}

// Slug chars for two-phase (slug ID → 2-byte encoding)
$slugCharsArr = [];

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
        $slugCharsArr[$numSlugs] = chr($numSlugs & 0xFF) . chr($numSlugs >> 8);
        $numSlugs++;
    }
    $pos = $nlPos + 1;
}
unset($disc);

// ─── Parse-only benchmark ───

$t = microtime(true);

$fh = fopen($inputPath, 'rb');
stream_set_read_buffer($fh, 0);
$remaining = $fileSize;
$bufSize = 8_388_608;

if ($variant === 'baseline') {
    // Current: bucket accumulation
    $buckets = array_fill(0, $numSlugs, '');

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
    }

    fclose($fh);

    // Convert buckets → counts (timed as part of parse phase)
    $counts = array_fill(0, $numSlugs * $numDates, 0);
    for ($s = 0; $s < $numSlugs; $s++) {
        if ($buckets[$s] === '') continue;
        $base = $s * $numDates;
        foreach (unpack('v*', $buckets[$s]) as $dateId) {
            $counts[$base + $dateId]++;
        }
    }
    $totalEntries = array_sum($counts);

} elseif ($variant === '2phase') {
    // Two-phase: emit packed (slugId, dateId) pairs into a flat binary string,
    // then aggregate in a second pass
    $pairs = '';

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
        $fence = $end - 720;
        while ($p < $fence) {
            $nl = strpos($raw, "\n", $p + 52);
            $pairs .= $slugCharsArr[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] . $dateChars[substr($raw, $nl - 23, 8)];
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $pairs .= $slugCharsArr[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] . $dateChars[substr($raw, $nl - 23, 8)];
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $pairs .= $slugCharsArr[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] . $dateChars[substr($raw, $nl - 23, 8)];
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $pairs .= $slugCharsArr[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] . $dateChars[substr($raw, $nl - 23, 8)];
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $pairs .= $slugCharsArr[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] . $dateChars[substr($raw, $nl - 23, 8)];
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $pairs .= $slugCharsArr[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] . $dateChars[substr($raw, $nl - 23, 8)];
            $p = $nl + 1;
        }
        while ($p < $end) {
            $nl = strpos($raw, "\n", $p + 52);
            if ($nl === false) break;
            $pairs .= $slugCharsArr[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] . $dateChars[substr($raw, $nl - 23, 8)];
            $p = $nl + 1;
        }
    }

    fclose($fh);

    // Phase 2: aggregate
    $counts = array_fill(0, $numSlugs * $numDates, 0);
    $pLen = strlen($pairs);
    for ($i = 0; $i < $pLen; $i += 4) {
        $sid = ord($pairs[$i]) | (ord($pairs[$i + 1]) << 8);
        $did = ord($pairs[$i + 2]) | (ord($pairs[$i + 3]) << 8);
        $counts[$sid * $numDates + $did]++;
    }
    $totalEntries = array_sum($counts);

} elseif ($variant === '2phase-chunked') {
    // Two-phase but chunked: emit pairs per-chunk to avoid 400MB single string
    $counts = array_fill(0, $numSlugs * $numDates, 0);

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

        // Phase 1: emit pairs for this chunk
        $pairs = '';
        $p = 0;
        $fence = $end - 720;
        while ($p < $fence) {
            $nl = strpos($raw, "\n", $p + 52);
            $pairs .= $slugCharsArr[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] . $dateChars[substr($raw, $nl - 23, 8)];
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $pairs .= $slugCharsArr[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] . $dateChars[substr($raw, $nl - 23, 8)];
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $pairs .= $slugCharsArr[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] . $dateChars[substr($raw, $nl - 23, 8)];
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $pairs .= $slugCharsArr[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] . $dateChars[substr($raw, $nl - 23, 8)];
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $pairs .= $slugCharsArr[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] . $dateChars[substr($raw, $nl - 23, 8)];
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $pairs .= $slugCharsArr[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] . $dateChars[substr($raw, $nl - 23, 8)];
            $p = $nl + 1;
        }
        while ($p < $end) {
            $nl = strpos($raw, "\n", $p + 52);
            if ($nl === false) break;
            $pairs .= $slugCharsArr[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] . $dateChars[substr($raw, $nl - 23, 8)];
            $p = $nl + 1;
        }

        // Phase 2: aggregate this chunk
        $pLen = strlen($pairs);
        for ($i = 0; $i < $pLen; $i += 4) {
            $sid = ord($pairs[$i]) | (ord($pairs[$i + 1]) << 8);
            $did = ord($pairs[$i + 2]) | (ord($pairs[$i + 3]) << 8);
            $counts[$sid * $numDates + $did]++;
        }
    }

    fclose($fh);
    $totalEntries = array_sum($counts);

} elseif ($variant === 'direct') {
    // Direct counting (no buckets, no two-phase) — regression test
    $counts = array_fill(0, $numSlugs * $numDates, 0);

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
        $fence = $end - 720;
        while ($p < $fence) {
            $nl = strpos($raw, "\n", $p + 52);
            $counts[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)] * $numDates + $dateLookup[substr($raw, $nl - 23, 8)]]++;
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $counts[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)] * $numDates + $dateLookup[substr($raw, $nl - 23, 8)]]++;
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $counts[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)] * $numDates + $dateLookup[substr($raw, $nl - 23, 8)]]++;
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $counts[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)] * $numDates + $dateLookup[substr($raw, $nl - 23, 8)]]++;
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $counts[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)] * $numDates + $dateLookup[substr($raw, $nl - 23, 8)]]++;
            $p = $nl + 1;
            $nl = strpos($raw, "\n", $p + 52);
            $counts[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)] * $numDates + $dateLookup[substr($raw, $nl - 23, 8)]]++;
            $p = $nl + 1;
        }
        while ($p < $end) {
            $nl = strpos($raw, "\n", $p + 52);
            if ($nl === false) break;
            $counts[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)] * $numDates + $dateLookup[substr($raw, $nl - 23, 8)]]++;
            $p = $nl + 1;
        }
    }

    fclose($fh);
    $totalEntries = array_sum($counts);
}

$elapsed = microtime(true) - $t;
fprintf(STDERR, "%-16s %.3fs  (single-process, %s entries)\n", $variant . ':', $elapsed, number_format($totalEntries));
