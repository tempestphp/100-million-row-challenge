<?php

declare(strict_types=1);

use App\Commands\Visit;

require __DIR__ . '/../vendor/autoload.php';

if ($argc < 2) {
    fwrite(STDERR, "Usage: php poc/codex-date-arith.php <input.csv> [runs]\n");
    exit(1);
}

$inputPath = $argv[1];
$runs = isset($argv[2]) ? max(1, (int) $argv[2]) : 2;

gc_disable();

[$dateCharsHash, $dateBytesById, $yearBase, $monthBase] = buildDateData();
[$slugIndex, $numSlugs] = buildSlugs();

$hashTimes = [];
$arithTimes = [];

for ($i = 0; $i < $runs; $i++) {
    $t0 = hrtime(true);
    $linesA = crunchDateHash($inputPath, $slugIndex, $dateCharsHash, $numSlugs);
    $t1 = hrtime(true);
    $linesB = crunchDateArith($inputPath, $slugIndex, $dateBytesById, $yearBase, $monthBase, $numSlugs);
    $t2 = hrtime(true);

    $hashMs = ($t1 - $t0) / 1_000_000;
    $arithMs = ($t2 - $t1) / 1_000_000;
    $hashTimes[] = $hashMs;
    $arithTimes[] = $arithMs;

    printf("run=%d hash=%.3fms arith=%.3fms lines=%d/%d\n", $i + 1, $hashMs, $arithMs, $linesA, $linesB);
}

sort($hashTimes);
sort($arithTimes);
printf("hash  best=%.3fms median=%.3fms\n", $hashTimes[0], $hashTimes[intdiv(count($hashTimes), 2)]);
printf("arith best=%.3fms median=%.3fms\n", $arithTimes[0], $arithTimes[intdiv(count($arithTimes), 2)]);

function buildDateData(): array
{
    $dateCharsHash = [];
    $dateBytesById = [];

    $yearBase = [0];
    for ($y = 0; $y < 7; $y++) {
        $year = 2020 + $y;
        $yearBase[$y + 1] = $yearBase[$y] + (($year % 4 === 0) ? 366 : 365);
    }

    $monthNormal = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    $monthLeap = [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335];

    $monthBase = [];
    for ($y = 0; $y < 7; $y++) {
        $monthBase[$y] = (($y === 0 || $y === 4) ? $monthLeap : $monthNormal);
    }

    $id = 0;
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
                $packed = pack('v', $id);
                $dateCharsHash[$key] = $packed;
                $dateBytesById[$id] = $packed;
                $id++;
            }
        }
    }

    return [$dateCharsHash, $dateBytesById, $yearBase, $monthBase];
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

function crunchDateHash(string $path, array $slugIndex, array $dateCharsHash, int $numSlugs): int
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
            $buckets[$sid] .= $dateCharsHash[substr($raw, $nl - 23, 8)];
            $p = $nl + 1;
            $lines++;
        }
    }

    fclose($fh);
    return $lines;
}

function crunchDateArith(
    string $path,
    array $slugIndex,
    array $dateBytesById,
    array $yearBase,
    array $monthBase,
    int $numSlugs,
): int {
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

            $y = ((ord($raw[$nl - 23]) - 48) * 10 + (ord($raw[$nl - 22]) - 48)) - 20;
            $m = (ord($raw[$nl - 20]) - 48) * 10 + (ord($raw[$nl - 19]) - 48);
            $d = (ord($raw[$nl - 17]) - 48) * 10 + (ord($raw[$nl - 16]) - 48);

            $dateId = $yearBase[$y] + $monthBase[$y][$m - 1] + $d - 1;
            $buckets[$sid] .= $dateBytesById[$dateId];

            $p = $nl + 1;
            $lines++;
        }
    }

    fclose($fh);
    return $lines;
}
