<?php

declare(strict_types=1);

use App\Commands\Visit;

use function array_fill;
use function count;
use function fclose;
use function file;
use function fopen;
use function fread;
use function fseek;
use function fwrite;
use function gc_disable;
use function hrtime;
use function intdiv;
use function max;
use function pack;
use function printf;
use function stream_set_read_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function substr_compare;

use const SEEK_CUR;

require __DIR__ . '/../vendor/autoload.php';

if ($argc < 2) {
    fwrite(STDERR, "Usage: php poc/codex-phf.php <input.csv> [runs]\n");
    exit(1);
}

$inputPath = $argv[1];
$runs = isset($argv[2]) ? max(1, (int) $argv[2]) : 3;

gc_disable();

[$dateChars, $numDates] = buildDateChars();
[$slugIndex, $slugLabels] = buildSlugs();
$numSlugs = count($slugLabels);
$dispatch = buildDispatch($slugLabels);

$hashTimes = [];
$phfTimes = [];

for ($i = 0; $i < $runs; $i++) {
    $t0 = hrtime(true);
    $linesA = crunchHash($inputPath, $slugIndex, $dateChars, $numSlugs, $numDates);
    $t1 = hrtime(true);
    $linesB = crunchDispatch($inputPath, $dispatch, $dateChars, $numSlugs, $numDates);
    $t2 = hrtime(true);

    $hashMs = ($t1 - $t0) / 1_000_000;
    $phfMs = ($t2 - $t1) / 1_000_000;
    $hashTimes[] = $hashMs;
    $phfTimes[] = $phfMs;

    printf(
        "run=%d hash=%.3fms dispatch=%.3fms lines=%d/%d\n",
        $i + 1,
        $hashMs,
        $phfMs,
        $linesA,
        $linesB,
    );
}

sort($hashTimes);
sort($phfTimes);

printf(
    "hash  best=%.3fms median=%.3fms\n",
    $hashTimes[0],
    $hashTimes[intdiv(count($hashTimes), 2)],
);
printf(
    "dispatch best=%.3fms median=%.3fms\n",
    $phfTimes[0],
    $phfTimes[intdiv(count($phfTimes), 2)],
);

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
    $slugLabels = [];
    $id = 0;

    foreach (Visit::all() as $visit) {
        $slug = substr($visit->uri, 25);
        if (!isset($slugIndex[$slug])) {
            $slugIndex[$slug] = $id;
            $slugLabels[$id] = $slug;
            $id++;
        }
    }

    return [$slugIndex, $slugLabels];
}

function buildDispatch(array $slugLabels): array
{
    $dispatch = [];

    foreach ($slugLabels as $id => $slug) {
        $len = strlen($slug);
        $first = $slug[0];
        $dispatch[$len][$first][] = [$slug, $id];
    }

    return $dispatch;
}

function crunchHash(string $path, array $slugIndex, array $dateChars, int $numSlugs, int $numDates): int
{
    $buckets = array_fill(0, $numSlugs, '');
    $fh = fopen($path, 'rb');
    stream_set_read_buffer($fh, 0);

    $bufSize = 8_388_608;
    $prefix = 25;
    $lines = 0;

    while (true) {
        $raw = fread($fh, $bufSize);
        if ($raw === '') {
            break;
        }

        $len = strlen($raw);
        $end = strrpos($raw, "\n");
        if ($end === false) {
            continue;
        }

        $tail = $len - $end - 1;
        if ($tail > 0) {
            fseek($fh, -$tail, SEEK_CUR);
        }

        $p = 0;
        while ($p < $end) {
            $nl = strpos($raw, "\n", $p + 52);
            if ($nl === false) {
                break;
            }
            $buckets[$slugIndex[substr($raw, $p + $prefix, $nl - $p - 51)]] .= $dateChars[substr($raw, $nl - 23, 8)];
            $p = $nl + 1;
            $lines++;
        }
    }

    fclose($fh);

    return $lines;
}

function crunchDispatch(string $path, array $dispatch, array $dateChars, int $numSlugs, int $numDates): int
{
    $buckets = array_fill(0, $numSlugs, '');
    $fh = fopen($path, 'rb');
    stream_set_read_buffer($fh, 0);

    $bufSize = 8_388_608;
    $prefix = 25;
    $lines = 0;

    while (true) {
        $raw = fread($fh, $bufSize);
        if ($raw === '') {
            break;
        }

        $len = strlen($raw);
        $end = strrpos($raw, "\n");
        if ($end === false) {
            continue;
        }

        $tail = $len - $end - 1;
        if ($tail > 0) {
            fseek($fh, -$tail, SEEK_CUR);
        }

        $p = 0;
        while ($p < $end) {
            $nl = strpos($raw, "\n", $p + 52);
            if ($nl === false) {
                break;
            }

            $start = $p + $prefix;
            $slugLen = $nl - $p - 51;
            $first = $raw[$start];

            $id = -1;
            $candidates = $dispatch[$slugLen][$first] ?? null;
            if ($candidates !== null) {
                foreach ($candidates as [$slug, $sid]) {
                    if (substr_compare($raw, $slug, $start, $slugLen) === 0) {
                        $id = $sid;
                        break;
                    }
                }
            }

            if ($id >= 0) {
                $buckets[$id] .= $dateChars[substr($raw, $nl - 23, 8)];
            }

            $p = $nl + 1;
            $lines++;
        }
    }

    fclose($fh);

    return $lines;
}
