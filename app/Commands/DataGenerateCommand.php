<?php

namespace App\Commands;

use Tempest\Console\ConsoleCommand;
use Tempest\Console\HasConsole;
use Tempest\Console\Middleware\ForceMiddleware;
use function Tempest\Intl\Number\parse_int;

final class DataGenerateCommand
{
    use HasConsole;

    #[ConsoleCommand(middleware: [ForceMiddleware::class])]
    public function __invoke(
        int|string $iterations = 1_000_000,
        string $outputPath = __DIR__.'/../../data/data.csv',
    ): void {
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

        $now = time();
        $fiveYearsInSeconds = 60 * 60 * 24 * 365 * 5;

        $datePoolSize = 10_000;
        $datePool = [];
        for ($d = 0; $d < $datePoolSize; $d++) {
            $datePool[$d] = date('c', $now - mt_rand(0, $fiveYearsInSeconds));
        }

        $handle = fopen($outputPath, 'w');
        stream_set_write_buffer($handle, 1024 * 1024);

        $bufferSize = 10_000;
        $buffer = '';
        $progressInterval = 100_000;

        for ($i = 1; $i <= $iterations; $i++) {
            $buffer .= $uris[mt_rand(0, $uriCount - 1)].','.$datePool[mt_rand(0, $datePoolSize - 1)]."\n";

            if ($i % $bufferSize === 0) {
                fwrite($handle, $buffer);
                $buffer = '';

                if ($i % $progressInterval === 0) {
                    $this->info('Generated '.number_format($i).' rows');
                }
            }
        }

        if ($buffer !== '') {
            fwrite($handle, $buffer);
        }

        fclose($handle);

        $this->success('Done');
    }
}
