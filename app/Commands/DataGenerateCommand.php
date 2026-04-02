<?php

namespace App\Commands;

use Random\Engine\Xoshiro256StarStar;
use Random\Randomizer;
use Tempest\Console\ConsoleCommand;
use Tempest\Console\HasConsole;
use Tempest\Console\Middleware\ForceMiddleware;
use function Tempest\Intl\Number\parse_int;

final class DataGenerateCommand
{
    use HasConsole;

    private ?Randomizer $randomizer = null;

    #[ConsoleCommand(middleware: [ForceMiddleware::class])]
    public function __invoke(
        int|string $iterations = 1_000_000,
        string $outputPath = __DIR__ . '/../../data/data.csv',
        int $seed = 1772177204,
    ): void
    {
        $this->randomizer = $seed === 0
            ? new Randomizer(new Xoshiro256StarStar())
            : new Randomizer(new Xoshiro256StarStar($seed));

        $iterations = parse_int(str_replace([',', '_'], '', $iterations));

        if (! $this->confirm(sprintf(
            'Generating data for %s iterations in %s. Continue?',
            number_format($iterations),
            $outputPath,
        ), default: true)) {
            $this->error('Cancelled');

            return;
        }

        $uris = array_map(fn (Visit $v) => $v->uri, Visit::all());
        $uriCount = count($uris);

        $now = $seed === 0 ? time() : $seed;
        $fiveYearsInSeconds = 60 * 60 * 24 * 365 * 5;

        $datePoolSize = 10_000;
        $datePool = [];
        for ($d = 0; $d < $datePoolSize; $d++) {
            $datePool[$d] = date('c', $now - $this->random(0, $fiveYearsInSeconds));
        }

        $handle = fopen($outputPath, 'w');
        stream_set_write_buffer($handle, 1024 * 1024);

        $bufferSize = 10_000;
        $buffer = '';
        $progressInterval = 100_000;

        for ($i = 1; $i <= $iterations; $i++) {
            $buffer .= $uris[$this->random(0, $uriCount - 1)] . ',' . $datePool[$this->random(0, $datePoolSize - 1)] . "\n";

            if ($i % $bufferSize === 0) {
                fwrite($handle, $buffer);
                $buffer = '';

                if ($i % $progressInterval === 0) {
                    $this->info('Generated ' . number_format($i) . ' rows');
                }
            }
        }

        if ($buffer !== '') {
            fwrite($handle, $buffer);
        }

        fclose($handle);

        if ($seed !== 0) {
            $this->info('Seed: ' . $seed);
        }

        $this->success("Done: {$outputPath}");
    }

    private function random(int $min, int $max): int
    {
        return $this->randomizer->getInt($min, $max);
    }
}
