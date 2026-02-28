<?php

declare(strict_types=1);

namespace App;

use Closure;
use Fiber;

use const PHP_URL_PATH;

use function assert;
use function count;
use function fclose;
use function feof;
use function fgetcsv;
use function fopen;
use function fwrite;
use function gc_disable;
use function is_resource;
use function ksort;
use function mb_substr;
use function parse_url;
use function str_replace;
use function stream_set_blocking;
use function stream_set_read_buffer;
use function stream_set_write_buffer;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        #BlackLivesMatter

        $main = new Fiber(fn (): mixed => $this->writePrettyJSON(
            $outputPath,
            $this->parseVisits($inputPath)
        ));

        $main->start();
    }

    private function async(callable $callable): mixed
    {
        $fiber = new Fiber(static function (Closure $closure): mixed {
            Fiber::suspend();

            return $closure();
        });

        $fiber->start($callable);

        while ($fiber->isSuspended()) {
            $fiber->resume();
            Fiber::suspend();
        }

        return $fiber->getReturn();
    }

    private function parseVisits(string $inputPath): array
    {
        return $this->async(static function () use ($inputPath) {
            $handle = fopen($inputPath, 'rb');
            assert(is_resource($handle), "Unable to open input file: {$inputPath}");

            stream_set_blocking($handle, false);
            stream_set_read_buffer($handle, 0);

            $visits = [];
            while (! feof($handle)) {
                $line = fgetcsv($handle, escape: ',');
                if (false === $line) {
                    continue;
                }

                [$url, $datetime] = $line;

                $path = str_replace(['/'], ['\\/'], parse_url($url, PHP_URL_PATH));

                $date = mb_substr($datetime, 0, 10);

                $visits[$path][$date] ??= 0;

                ++$visits[$path][$date];

                Fiber::suspend();
            }

            fclose($handle);

            return $visits;
        });
    }

    private function writePrettyJSON(string $outputPath, array $visits): void
    {
        $this->async(static function () use ($visits, $outputPath): void {
            $handle = fopen($outputPath, 'wb');
            assert(is_resource($handle), "Unable to open output file: {$outputPath}");

            stream_set_blocking($handle, false);
            stream_set_write_buffer($handle, 0);

            $pathCount = count($visits);
            $pathIndex = 0;

            fwrite($handle, "{\n");
            foreach ($visits as $path => $dates) {
                fwrite($handle, '    "' . $path . "\": {\n");

                $dateIndex = 0;
                ksort($dates);
                $dateCount = count($dates);

                foreach ($dates as $date => $count) {
                    fwrite($handle, '        "' . $date . "\": {$count}");

                    match (true) {
                        ++$dateIndex < $dateCount => fwrite($handle, ",\n"),
                        default => fwrite($handle, "\n"),
                    };

                    Fiber::suspend();
                }

                fwrite($handle, '    }');

                if (++$pathIndex < $pathCount) {
                    fwrite($handle, ",\n");
                } else {
                    fwrite($handle, "\n");
                }

                Fiber::suspend();

                unset($visits[$path]);

            }
            fwrite($handle, '}');
            fclose($handle);
        });
    }
}
