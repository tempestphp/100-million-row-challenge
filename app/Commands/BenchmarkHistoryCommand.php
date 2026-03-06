<?php

namespace App\Commands;

use Tempest\Console\ConsoleCommand;
use Tempest\Console\HasConsole;

// This is a (almost) completely AI-generated class, please ignore.
final class BenchmarkHistoryCommand
{
    use HasConsole;

    #[ConsoleCommand]
    public function __invoke(): void
    {
        $this->processLeaderboard(
            leaderboardPath: 'leaderboard.csv',
            historyFile: __DIR__ . '/../../leaderboard-history.csv',
            finalistsFile: __DIR__ . '/../../leaderboard-finalists.csv',
        );

        $this->success('Done.');
    }

    private function processLeaderboard(string $leaderboardPath, string $historyFile, string $finalistsFile): void
    {
        $gitLog = shell_exec("git log --format='%H|%ci' --since='2026-03-01' --follow -- {$leaderboardPath}");

        if (empty($gitLog)) {
            $this->error("No git history found for {$leaderboardPath}");
            return;
        }

        $commits = array_filter(explode("\n", trim($gitLog)));
        $commits = array_reverse($commits);

        $historyData = [];

        foreach ($commits as $commitLine) {
            $this->info("Processing commit: {$commitLine}");
            [$commitHash, $commitDate] = explode('|', $commitLine, 2);

            $diff = shell_exec("git show {$commitHash} -- {$leaderboardPath} 2>/dev/null");

            if (empty($diff)) {
                continue;
            }

            $lines = explode("\n", $diff);

            foreach ($lines as $line) {
                if (!str_starts_with($line, '+') || str_starts_with($line, '+++')) {
                    continue;
                }

                $line = substr($line, 1);

                if (empty($line) || str_starts_with($line, 'entry_date,')) {
                    continue;
                }

                $parts = explode(',', str_replace(';', ',', $line));

                if (count($parts) >= 3) {
                    $historyData[] = [
                        'entry_date' => date('Y-m-d H:i:s', strtotime($commitDate)),
                        'branch_name' => $parts[1],
                        'time' => $parts[2],
                    ];
                }
            }
        }

        usort($historyData, fn($a, $b) => strcmp($a['entry_date'], $b['entry_date']));

        $excludedBranches = ['brendt', 'ghostwriter'];
        $currentBestByBranch = [];
        $bestCheckpoint = [];
        $checkpointReachedAt = [];

        foreach ($historyData as $entry) {
            $branch = $entry['branch_name'];

            if (in_array($branch, $excludedBranches)) {
                continue;
            }

            $time = (float) $entry['time'];

            if (!isset($currentBestByBranch[$branch]) || $time < $currentBestByBranch[$branch]) {
                $currentBestByBranch[$branch] = $time;

                $checkpoint = floor($time * 100) / 100;

                if (!isset($bestCheckpoint[$branch]) || $checkpoint < $bestCheckpoint[$branch]) {
                    $bestCheckpoint[$branch] = $checkpoint;
                    $checkpointReachedAt[$branch] = $entry['entry_date'];
                }
            }
        }

        $allCheckpoints = array_unique(array_values($bestCheckpoint));
        sort($allCheckpoints);
        $topCheckpoints = array_slice($allCheckpoints, 0, 3);

        $candidates = [];

        foreach ($bestCheckpoint as $branch => $cp) {
            if (!in_array($cp, $topCheckpoints)) {
                continue;
            }

            $candidates[] = [
                'branch' => $branch,
                'checkpoint' => $cp,
                'time' => $currentBestByBranch[$branch],
                'reached_at' => $checkpointReachedAt[$branch],
            ];
        }

        usort($candidates, fn($a, $b) =>
            ($a['checkpoint'] <=> $b['checkpoint']) ?: strcmp($a['reached_at'], $b['reached_at']));

        $finalists = array_slice($candidates, 0, 3);
        $topBranches = array_column($finalists, 'branch');
        sort($topBranches);

        $currentBestByBranch = [];
        $normalizedData = [];

        foreach ($historyData as $entry) {
            $branch = $entry['branch_name'];

            if (!in_array($branch, $topBranches)) {
                continue;
            }

            $time = (float) $entry['time'];

            if (!isset($currentBestByBranch[$branch]) || $time < $currentBestByBranch[$branch]) {
                $currentBestByBranch[$branch] = $time;
            }

            foreach ($currentBestByBranch as $branchName => $bestTime) {
                $normalizedData[] = [
                    'entry_date' => $entry['entry_date'],
                    'branch_name' => $branchName,
                    'time' => $bestTime,
                ];
            }
        }

        $pivotedData = [];

        foreach ($normalizedData as $entry) {
            $date = $entry['entry_date'];

            if (!isset($pivotedData[$date])) {
                $pivotedData[$date] = [];
            }

            $pivotedData[$date][$entry['branch_name']] = $entry['time'];
        }

        $fp = fopen($historyFile, 'w');
        fputs($fp, 'entry_date,' . implode(',', $topBranches) . "\n");

        $previousValues = [];

        foreach ($pivotedData as $date => $branches) {
            $currentValues = [];

            foreach ($topBranches as $branch) {
                $currentValues[$branch] = $branches[$branch] ?? '';
            }

            if ($currentValues === $previousValues) {
                continue;
            }

            $previousValues = $currentValues;

            $row = [$date];

            foreach ($topBranches as $branch) {
                $row[] = $currentValues[$branch];
            }

            fputs($fp, implode(',', $row) . "\n");
        }

        fclose($fp);

        $fp = fopen($finalistsFile, 'w');
        fputs($fp, "rank,branch_name,checkpoint,time,reached_at\n");

        $rank = 1;

        foreach ($finalists as $finalist) {
            fputs($fp, implode(',', [
                $rank++,
                $finalist['branch'],
                number_format($finalist['checkpoint'], 2),
                $finalist['time'],
                $finalist['reached_at'],
            ]) . "\n");
        }

        fclose($fp);
    }
}
