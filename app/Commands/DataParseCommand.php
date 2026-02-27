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
        bool $store = false,
        ?string $name = null,
        int $workers = 4,
    ): void {
        // Spec: 2 vCPUs, 1.5GB RAM max. GC disabled for hot path (no cyclic refs).
        \ini_set('memory_limit', '1536M');
        if (\function_exists('gc_disable')) {
            \gc_disable();
        }
        $startTime = \microtime(true);

        new Parser()->parse($inputPath, $outputPath);

        $endTime = \microtime(true);
        $executionTime = $endTime - $startTime;

        $name ??= \exec('git branch --show-current');

        $leaderBoardEntry = time() . ',' . $name . ',' . $executionTime;

        if ($store) {
            $leaderBoardFile = fopen(__DIR__ . '/../../leaderboard.csv', 'a');
            fwrite($leaderBoardFile, $leaderBoardEntry . PHP_EOL);
            fclose($leaderBoardFile);
            $this->success('Written to leaderboard.csv');
        }

        $this->success($leaderBoardEntry);
    }
}