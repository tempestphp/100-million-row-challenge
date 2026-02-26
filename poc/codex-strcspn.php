<?php

declare(strict_types=1);

use App\Commands\Visit;

require __DIR__ . '/../vendor/autoload.php';

if ($argc < 2) {
    fwrite(STDERR, "Usage: php poc/codex-strcspn.php <input.csv> [runs]\n");
    exit(1);
}

$inputPath = $argv[1];
$runs = isset($argv[2]) ? max(1, (int) $argv[2]) : 2;

gc_disable();

[$dateChars] = buildDateChars();
[$slugIndex, $numSlugs] = buildSlugs();

$strposTimes = [];
$strcspnTimes = [];

for ($i = 0; $i < $runs; $i++) {
    $t0 = hrtime(true);
    $linesA = crunchStrpos($inputPath, $slugIndex, $dateChars, $numSlugs);
    $t1 = hrtime(true);
    $linesB = crunchStrcspn($inputPath, $slugIndex, $dateChars, $numSlugs);
    $t2 = hrtime(true);

    $msA = ($t1 - $t0) / 1_000_000;
    $msB = ($t2 - $t1) / 1_000_000;
    $strposTimes[] = $msA;
    $strcspnTimes[] = $msB;

    printf("run=%d strpos=%.3fms strcspn=%.3fms lines=%d/%d\n", $i + 1, $msA, $msB, $linesA, $linesB);
}

sort($strposTimes);
sort($strcspnTimes);
printf("strpos  best=%.3fms median=%.3fms\n", $strposTimes[0], $strposTimes[intdiv(count($strposTimes), 2)]);
printf("strcspn best=%.3fms median=%.3fms\n", $strcspnTimes[0], $strcspnTimes[intdiv(count($strcspnTimes), 2)]);

function buildDateChars(): array
{
    $lookup = [];
    $count = 0;

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
                $lookup[$key] = pack('v', $count);
                $count++;
            }
        }
    }

    return [$lookup, $count];
}

function buildSlugs(): array
{
    $slugIndex = [];
    $id = 0;

    foreach (Visit::all() as $visit) {
        $slug = substr($visit->uri, 25);
        if (!isset($slugIndex[$slug])) {
            $slugIndex[$slug] = $id;
            $id++;
        }
    }

    return [$slugIndex, $id];
}

function crunchStrpos(string $path, array $slugIndex, array $dateChars, int $numSlugs): int
{
    $buckets = array_fill(0, $numSlugs, '');
    $fh = fopen($path, 'rb');
    stream_set_read_buffer($fh, 0);

    $bufSize = 8_388_608;
    $lines = 0;

    while (true) {
        $raw = fread($fh, $bufSize);
        if ($raw === '') break;
        $len = strlen($raw);
        $end = strrpos($raw, "\n");
        if ($end === false) continue;

        $tail = $len - $end - 1;
        if ($tail > 0) fseek($fh, -$tail, SEEK_CUR);

        $p = 0;
        while ($p < $end) {
            $nl = strpos($raw, "\n", $p + 52);
            if ($nl === false) break;
            $sid = $slugIndex[substr($raw, $p + 25, $nl - $p - 51)];
            $buckets[$sid] .= $dateChars[substr($raw, $nl - 23, 8)];
            $p = $nl + 1;
            $lines++;
        }
    }

    fclose($fh);
    return $lines;
}

function crunchStrcspn(string $path, array $slugIndex, array $dateChars, int $numSlugs): int
{
    $buckets = array_fill(0, $numSlugs, '');
    $fh = fopen($path, 'rb');
    stream_set_read_buffer($fh, 0);

    $bufSize = 8_388_608;
    $lines = 0;

    while (true) {
        $raw = fread($fh, $bufSize);
        if ($raw === '') break;
        $len = strlen($raw);
        $end = strrpos($raw, "\n");
        if ($end === false) continue;

        $tail = $len - $end - 1;
        if ($tail > 0) fseek($fh, -$tail, SEEK_CUR);

        $p = 0;
        while ($p < $end) {
            $offset = strcspn($raw, "\n", $p + 52);
            $nl = $p + 52 + $offset;
            if ($nl > $end) break;
            $sid = $slugIndex[substr($raw, $p + 25, $nl - $p - 51)];
            $buckets[$sid] .= $dateChars[substr($raw, $nl - 23, 8)];
            $p = $nl + 1;
            $lines++;
        }
    }

    fclose($fh);
    return $lines;
}
