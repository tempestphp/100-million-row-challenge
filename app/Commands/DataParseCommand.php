<?php

namespace App\Commands;

use App\Parser;
use Tempest\Console\ConsoleCommand;
use Tempest\Console\HasConsole;

final class DataParseCommand
{
    use HasConsole;

    #[ConsoleCommand]
    public function __invoke(
        string $inputPath = __DIR__ . '/../../data/data.csv',
        string $outputPath = __DIR__ . '/../../data/data.json',
        ?int $cores = null,
    ): void {
        $defaultInput = __DIR__ . '/../../data/data.csv';
        $defaultOutput = __DIR__ . '/../../data/data.json';

        if ($cores === null && \preg_match('/^\d+$/', $inputPath) === 1) {
            $cores = (int)$inputPath;
            $inputPath = $defaultInput;
            $outputPath = $defaultOutput;
        } elseif ($cores === null && \preg_match('/^\d+$/', $outputPath) === 1) {
            $cores = (int)$outputPath;
            $outputPath = $defaultOutput;
        }

        if ($cores !== null && $cores > 0) {
            self::limitCpuCores($cores);
        }

        ini_set('max_execution_time', 60 * 5);
        $startTime = microtime(true);

        (new Parser())->parse($inputPath, $outputPath);
        $endTime = microtime(true);
        $executionTime = $endTime - $startTime;

        $this->success($executionTime);
    }

    private static function limitCpuCores(int $cores): void
    {
        $pid = \getmypid();
        if (!\is_int($pid) || $pid <= 0) {
            return;
        }

        $allowed = \pcntl_getcpuaffinity($pid);
        if (!\is_array($allowed) || $allowed === []) {
            return;
        }

        \sort($allowed, \SORT_NUMERIC);
        $target = \array_slice($allowed, 0, \min($cores, \count($allowed)));
        if ($target === []) {
            return;
        }

        \pcntl_setcpuaffinity($pid, $target);
    }
}
