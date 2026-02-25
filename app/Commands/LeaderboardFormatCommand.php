<?php

namespace App\Commands;

use Tempest\Console\ConsoleCommand;
use Tempest\Console\HasConsole;

final readonly class LeaderboardFormatCommand
{
    use HasConsole;

    #[ConsoleCommand]
    public function __invoke(): void
    {
        $path = __DIR__ . '/../../leaderboard.csv';
        $handle = fopen($path, 'r');
        $data = [];

        while($line = fgetcsv($handle, escape: ',')) {
            if ($line[0] === 'entry_date') {
                continue;
            }

            [$submissionTime, $branch, $benchmarkTime] = $line;

            if (! isset($data[$branch]) || $data[$branch]['benchmarkTime'] > $benchmarkTime) {
                $data[$branch] = [
                    'submissionTime' => $submissionTime,
                    'branch' => $branch,
                    'benchmarkTime' => $benchmarkTime,
                ];
            }
        }

        usort($data, fn ($a, $b) => $a['benchmarkTime'] <=> $b['benchmarkTime']);

        $data = [['entry_date','branch_name','time'], ...$data];

        $leaderboard = implode(
            PHP_EOL,
            array_map(fn ($row) => implode(',', $row), $data)
        );

        $leaderboard .= PHP_EOL;

        file_put_contents($path, $leaderboard);

        $this->success('Done');
    }
}