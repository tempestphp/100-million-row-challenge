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
    ): void {
        ini_set('max_execution_time', 60 * 5);

        $startTime = microtime(true);

        new Parser()->parse($inputPath, $outputPath);

        $endTime = microtime(true);
        $executionTime = $endTime - $startTime;

        $this->success($executionTime);
    }
}