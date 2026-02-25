<?php

declare(strict_types=1);

use App\Commands\Visit;
use Random\Engine\Xoshiro256StarStar;
use Random\Randomizer;

require __DIR__ . '/vendor/autoload.php';

$iterations = 100_000_000;
$outputPath = __DIR__ . '/data/data.csv';
$seed = 1;

$randomizer = new Randomizer(new Xoshiro256StarStar($seed));

$uris = array_map(static fn (Visit $v) => $v->uri, Visit::all());
$uriCount = count($uris);

$now = time();
$fiveYearsInSeconds = 60 * 60 * 24 * 365 * 5;

$datePoolSize = 10_000;
$datePool = [];
for ($d = 0; $d < $datePoolSize; $d++) {
    $datePool[$d] = date('c', $now - $randomizer->getInt(0, $fiveYearsInSeconds));
}

$handle = fopen($outputPath, 'w');
if ($handle === false) {
    fwrite(STDERR, "Could not open output file: {$outputPath}\n");
    exit(1);
}

stream_set_write_buffer($handle, 1024 * 1024);

$bufferSize = 10_000;
$buffer = '';
$progressInterval = 100_000;

for ($i = 1; $i <= $iterations; $i++) {
    $buffer .= $uris[$randomizer->getInt(0, $uriCount - 1)]
        . ','
        . $datePool[$randomizer->getInt(0, $datePoolSize - 1)]
        . "\n";

    if ($i % $bufferSize === 0) {
        fwrite($handle, $buffer);
        $buffer = '';

        if ($i % $progressInterval === 0) {
            fwrite(STDOUT, 'Generated ' . number_format($i) . " rows\n");
        }
    }
}

if ($buffer !== '') {
    fwrite($handle, $buffer);
}

fclose($handle);

fwrite(STDOUT, "Done: {$outputPath}\n");

