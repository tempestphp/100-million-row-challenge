<?php

declare(strict_types=1);

use App\Commands\Visit;

require __DIR__ . '/../vendor/autoload.php';

if ($argc < 2) {
    fwrite(STDERR, "Usage: php poc/codex-phf-switch.php <input.csv> [runs]\n");
    exit(1);
}

$inputPath = $argv[1];
$runs = isset($argv[2]) ? max(1, (int) $argv[2]) : 2;

gc_disable();

[$dateChars] = buildDateChars();
[$slugIndex, $slugLabels] = buildSlugs();
$numSlugs = count($slugLabels);
$mapSlug = buildSlugSwitchMapper($slugLabels);

$hashTimes = [];
$switchTimes = [];

for ($i = 0; $i < $runs; $i++) {
    $t0 = hrtime(true);
    $linesA = crunchHash($inputPath, $slugIndex, $dateChars, $numSlugs);
    $t1 = hrtime(true);
    $linesB = crunchSwitch($inputPath, $mapSlug, $dateChars, $numSlugs);
    $t2 = hrtime(true);

    $hashMs = ($t1 - $t0) / 1_000_000;
    $switchMs = ($t2 - $t1) / 1_000_000;
    $hashTimes[] = $hashMs;
    $switchTimes[] = $switchMs;

    printf("run=%d hash=%.3fms switch=%.3fms lines=%d/%d\n", $i + 1, $hashMs, $switchMs, $linesA, $linesB);
}

sort($hashTimes);
sort($switchTimes);
printf("hash   best=%.3fms median=%.3fms\n", $hashTimes[0], $hashTimes[intdiv(count($hashTimes), 2)]);
printf("switch best=%.3fms median=%.3fms\n", $switchTimes[0], $switchTimes[intdiv(count($switchTimes), 2)]);

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

function buildSlugSwitchMapper(array $slugLabels): callable
{
    $src = 'return static function(string $slug): int { switch ($slug) {';
    foreach ($slugLabels as $id => $slug) {
        $src .= 'case ' . var_export($slug, true) . ': return ' . $id . ';';
    }
    $src .= 'default: return -1; } };';

    /** @var callable $mapper */
    $mapper = eval($src);

    return $mapper;
}

function crunchHash(string $path, array $slugIndex, array $dateChars, int $numSlugs): int
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
            $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateChars[substr($raw, $nl - 23, 8)];
            $p = $nl + 1;
            $lines++;
        }
    }

    fclose($fh);
    return $lines;
}

function crunchSwitch(string $path, callable $mapSlug, array $dateChars, int $numSlugs): int
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
            $slug = substr($raw, $p + 25, $nl - $p - 51);
            $id = $mapSlug($slug);
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
