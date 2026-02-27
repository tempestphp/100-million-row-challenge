<?php

namespace App\Commands;

use Tempest\Cache\Cache;
use Tempest\Console\ConsoleCommand;
use Tempest\Console\HasConsole;
use Tempest\DateTime\Duration;
use Tempest\HttpClient\HttpClient;
use Throwable;
use function Tempest\env;

final class BenchmarkRunCommand
{
    use HasConsole;

    private const string CACHE_KEY = 'prs';

    private ?string $token;
    private bool $verify = true;
    private bool $persist = false;

    public function __construct(
        private readonly HttpClient $http,
        private readonly Cache $cache,
    )
    {
        $this->token = env('GITHUB_TOKEN');
    }

    #[ConsoleCommand]
    public function __invoke(
        ?int $pr = null,
        bool $daemon = false,
        bool $persist = false,
        bool $cache = false,
        bool $verify = true,
    ): void
    {
        if (! $this->token) {
            $this->error('GitHub token not found.');
            return;
        }

        $this->persist = $persist;
        $this->verify = $verify;

        if ($cache === false) {
            $this->cache->remove(self::CACHE_KEY);
        }

        if ($daemon) {
            $this->warning('Daemon mode enabled. Press ctrl+c to stop.');

            while (true) {
                if ($cache === false) {
                    $this->cache->remove(self::CACHE_KEY);
                }

                try {
                    $this->run($pr);
                } catch (Throwable $e) {
                    $this->error($e->getMessage());
                }

                $memory = memory_get_usage(true);
                $formattedMemory = number_format($memory / 1024 / 1024, 2) . 'MB';
                $this->writeWithTimestamp("<style=\"fg-blue\">[{$formattedMemory}]</style> Sleeping for 10 seconds…");
                sleep(10);
            }
        } else {
            $this->run($pr);
        }
    }

    private function run(?int $pr = null): void
    {
        $this->info('Starting run…');

        $prs = $this->fetchOpenPRs();
        $prsToProcess = $this->filterPRs($prs, $pr);

        if ($prsToProcess === []) {
            $this->error('No PRs to process.');
            return;
        }

        $this->info('Found ' . count($prsToProcess) . ' PR(s) to benchmark.');

        // Process each PR
        foreach ($prsToProcess as $prData) {
            $prNumber = $prData['number'];
            $prTitle = $prData['title'];

            $this->prInfo($prNumber, "Starting benchmark for {$prTitle}…");

            try {
                $result = $this->processPR($prData);
                $this->addLeaderboardResult('leaderboard.csv', $prNumber, $prTitle, $result);

                $isSingleThread = false;

                foreach ($prData['labels'] ?? [] as $label) {
                    $isSingleThread = $label['name'] === '🚂 single thread';
                    if ($isSingleThread) {
                        break;
                    }
                }

                if ($isSingleThread) {
                    $this->addLeaderboardResult('leaderboard-single-thread.csv', $prNumber, $prTitle, $result);
                }


                $this->githubRemoveLabel($prNumber, 'bench_needed');
            } finally {
                // Always remove the verified label
                $this->githubRemoveLabel($prNumber, 'verified');
            }
        }

        $this->success('Done');
    }

    private function fetchOpenPRs(): array
    {
        return $this->cache->resolve(self::CACHE_KEY, function () {
            $allPrs = [];
            $page = 1;
            $perPage = 100;

            do {
                $response = $this->http->get(
                    uri: "https://api.github.com/repos/tempestphp/100-million-row-challenge/pulls?state=open&per_page={$perPage}&page={$page}",
                    headers: [
                        'Authorization' => 'Bearer ' . $this->token,
                        'User-Agent' => 'Tempest-Benchmark',
                        'Accept' => 'application/vnd.github.v3+json'
                    ],
                );

                if (! $response->status->isSuccessful()) {
                    $this->error('Failed to fetch PRs from GitHub. HTTP Code: ' . $response->status->name);
                    break;
                }

                $prs = json_decode($response->body, true) ?? [];

                $allPrs = array_merge($allPrs, $prs);
                $page++;
            } while ($prs !== []);

            return $allPrs;
        }, Duration::minutes(10));
    }

    private function filterPRs(array $prs, ?int $filter): array
    {
        $filtered = [];

        foreach ($prs as $pr) {
            $hasVerifiedLabel = false;

            foreach ($pr['labels'] ?? [] as $label) {
                if ($label['name'] === 'verified') {
                    $hasVerifiedLabel = true;
                    break;
                }
            }

            if ($this->verify && ! $hasVerifiedLabel) {
                continue;
            }

            if ($filter !== null) {
                if ($pr['number'] !== $filter) {
                    continue;
                } else {
                    $filtered[] = $pr;
                    return $filtered;
                }
            }

            if ($pr['draft'] ?? false) {
                continue;
            }

            $filtered[] = $pr;
        }

        return $filtered;
    }

    private function githubComment(int $prNumber, string $message): void
    {
        if (! $this->persist) {
            $this->prWarning($prNumber, "Skipping GitHub comment as --persist is not enabled.");
            return;
        }

        $response = $this->http->post(
            "https://api.github.com/repos/tempestphp/100-million-row-challenge/issues/{$prNumber}/comments",
            headers: [
                'Authorization' => 'Bearer ' . $this->token,
                'User-Agent' => 'Tempest-Benchmark',
                'Accept' => 'application/vnd.github.v3+json',
                'Content-Type' => 'application/json'
            ],
            body: json_encode(['body' => $message]),
        );

        if ($response->status->isSuccessful()) {
            $this->prWarning($prNumber, "GitHub comment posted.");
        } else {
            $this->prError($prNumber, "GitHub comment failed.");
        }
    }

    private function githubRemoveLabel(int $prNumber, string $label): void
    {
        if (! $this->persist) {
            $this->prWarning($prNumber, "Skipping GitHub label removal as --persist is not enabled.");
            return;
        }

        $response = $this->http->delete(
            uri: "https://api.github.com/repos/tempestphp/100-million-row-challenge/issues/{$prNumber}/labels/" . urlencode($label),
            headers: [
                'Authorization' => 'Bearer ' . $this->token,
                'User-Agent' => 'Tempest-Benchmark',
                'Accept' => 'application/vnd.github.v3+json'
            ],
        );

        if ($response->status->isSuccessful()) {
            $this->prWarning($prNumber, "GitHub label removed.");
        } else {
            $this->prError($prNumber, "GitHub label removal failed.");
        }
    }

    private function processPR(array $pr): ?float
    {
        $prNumber = $pr['number'];
        $cloneUrl = $pr['head']['repo']['clone_url'] ?? null;
        $branch = $pr['head']['ref'];

        $this->prLine($prNumber, "Processing…");

        // Clone the PR
        $benchmarkDir = __DIR__ . '/../../.benchmark/pr-' . $prNumber;

        if (is_dir($benchmarkDir)) {
            $this->prLine($prNumber, "Removing existing benchmark directory…");
            exec("rm -rf " . escapeshellarg($benchmarkDir));
        }

        $this->prLine($prNumber, "Cloning…");
        exec("git clone --branch " . escapeshellarg($branch) . " " . escapeshellarg($cloneUrl) . " " . escapeshellarg($benchmarkDir) . " 2>&1", $output, $returnCode);

        if ($returnCode !== 0) {
            $this->prError($prNumber, "Failed to clone");
            $this->githubComment($prNumber, 'Benchmarking failed: Unable to clone repository');
            return null;
        }

        // Composer install
        $this->prLine($prNumber, 'Running composer install…');
        exec("cd " . escapeshellarg($benchmarkDir) . " && composer install --no-dev --optimize-autoloader 2>&1", $output, $returnCode);

        if ($returnCode !== 0) {
            $this->prError($prNumber, "Failed to run composer install");
            $this->githubComment($prNumber, 'Benchmarking failed: Composer install failed');
            return null;
        }

        // Run benchmark with Hyperfine
        $resultFile = __DIR__ . '/../../.benchmark/result-' . $prNumber . '.json';
        $actualPath = __DIR__ . '/../../data/real-data-actual.json';
        $parseCommand = sprintf(
            'php -dmax_execution_time=300 tempest data:parse --input-path="%s" --output-path="%s"',
            escapeshellarg(__DIR__ . '/../../data/real-data.csv'),
            escapeshellarg($actualPath),
        );

        $command = sprintf(
            "hyperfine --warmup 0 --runs 1 --show-output --export-json %s 'cd %s && %s'",
            escapeshellarg($resultFile),
            escapeshellarg($benchmarkDir),
            $parseCommand,
        );

        $this->prLine($prNumber, $command);

        exec($command, $output, $returnCode);

        if ($returnCode !== 0 || ! file_exists($resultFile)) {
            $cleanedOutput = implode(PHP_EOL, preg_replace(
                '/\x1B(?:[@-Z\\-_]|\[[0-?]*[ -\/]*[@-~])/',
                '',
                $output
            ));

            $this->prError($prNumber, "Failed to run benchmark");
            $this->githubComment($prNumber, <<<MD
            Failed to run benchmark:
            
            ```
            $cleanedOutput
            ```
            MD);

            return null;
        }

        // Parse results
        $results = json_decode(file_get_contents($resultFile), true);
        $meanTime = $results['results'][0]['mean'] ?? null;

        if ($meanTime === null) {
            $this->prError($prNumber, "Failed to parse benchmark results");
            $this->githubComment($prNumber, 'Benchmarking failed: Unable to parse results');
            return null;
        }

        if ($meanTime < 20) {
            // Second run for fast PRs
            $command = sprintf(
                "hyperfine --warmup 2 --runs 5 --export-json %s 'cd %s && %s'",
                escapeshellarg($resultFile),
                escapeshellarg($benchmarkDir),
                $parseCommand,
            );

            $this->prLine($prNumber, $command);

            exec($command, $output, $returnCode);

            if ($returnCode !== 0 || ! file_exists($resultFile)) {
                $this->prError($prNumber, "Failed to run benchmark");
                $this->githubComment($prNumber, 'Benchmarking failed');
                return null;
            }

            // Parse results
            $results = json_decode(file_get_contents($resultFile), true);
            $meanTime = $results['results'][0]['mean'] ?? null;
        }

        // Verify results
        $expectedPath = __DIR__ . '/../../data/real-data-expected.json';

        $actual = file_get_contents($actualPath);
        $expected = file_get_contents($expectedPath);

        if ($actual !== $expected) {
            $this->prError($prNumber, "Validation failed!");
            $this->githubComment($prNumber, "Benchmarking failed: Parsed result did not match expected result");
            return null;
        }

        // Post results
        $this->prSuccess($prNumber, "Benchmark complete: {$meanTime}s");
        $this->githubComment($prNumber, "Benchmarking complete! Mean execution time: **{$meanTime}s**");

        // Clean up
        exec("rm -rf " . escapeshellarg($benchmarkDir));

        return $meanTime;
    }

    private function addLeaderboardResult(string $file, int $prNumber, string $branch, ?float $newTime): void
    {
        if (! $newTime) {
            return;
        }

        // Pull from main
        $repoDir = __DIR__ . '/../..';

        if ($this->persist) {
            $gitPull = "cd " . escapeshellarg($repoDir) . " && git pull origin main";
            $this->prLine($prNumber, $gitPull);
            exec($gitPull, $output, $returnCode);

            if ($returnCode !== 0) {
                $this->prError($prNumber, "Git pull failed");
                return;
            }
        }

        // Update leaderboard
        $path = __DIR__ . '/../../' . $file;
        $handle = fopen($path, 'r');
        $data = [];

        while ($line = fgetcsv($handle, escape: '\\')) {
            if ($line[0] === 'entry_date') {
                continue;
            }

            [$submissionTime, $currentBranch, $currentTime] = $line;

            if ($currentBranch === $branch && $newTime < $currentTime) {
                $data[$currentBranch] = [
                    'submissionTime' => $submissionTime,
                    'branch' => $currentBranch,
                    'benchmarkTime' => $newTime,
                ];

                $messages = [
                    "You've improved your result! Have a cookie: 🍪",
                    "You've improved your result! Nice!",
                    "Can you make it even faster?? 🏎️💨",
                    "Benchmark says: yes. CPU says: please stop. 😅",
                    "Milliseconds were harmed in the making of this improvement. ⏱️",
                    "That’s a nice drop in mean time. Keep going—there’s still juice left. 🧃",
                    "Yes, this is an automated message to tell you you've improved your result. Have a star: ⭐️",
                    "I think there's room for _one_ more improvement… 👀",
                    "That's a mean time you've got there. 🥁",
                    "Mean time? More like *meme* time. You're cooking. 🍳",
                    "You shaved off time so clean it should be in a barbershop. 💈",
                    "Leaderboard be like _cheff's kiss_ 🤌",
                    "Mean time goes down, confidence goes up",
                    "Local maximum? Never heard of her. 📉",
                    "You just made the CPU do less cardio. 🫀",
                    "Mean time decreased. We love a humble average. 🙇",
                    "You didn’t optimize. You *performed violence* (on latency). 🔪",
                ];

                $this->githubComment($prNumber, $messages[array_rand($messages)] . "<br>🏆 [{$file}](https://github.com/tempestphp/100-million-row-challenge/blob/main/{$file})");
            } else {
                $data[$currentBranch] = [
                    'submissionTime' => $submissionTime,
                    'branch' => $currentBranch,
                    'benchmarkTime' => $currentTime,
                ];
            }
        }

        if (! isset($data[$branch])) {
            $data[$branch] = [
                'submissionTime' => time(),
                'branch' => $branch,
                'benchmarkTime' => $newTime,
            ];
        }

        fclose($handle);

        usort($data, fn ($a, $b) => $a['benchmarkTime'] <=> $b['benchmarkTime']);

        $data = [['entry_date', 'branch_name', 'time'], ...$data];

        $leaderboard = implode(
            PHP_EOL,
            array_map(fn ($row) => implode(',', $row), $data),
        );

        $leaderboard .= PHP_EOL;

        file_put_contents($path, $leaderboard);

        // Add changes
        if ($this->persist) {
            $gitAdd = "cd " . escapeshellarg($repoDir) . " && git add " . escapeshellarg($file);
            $this->prLine($prNumber, $gitAdd);
            exec($gitAdd, $output, $returnCode);

            if ($returnCode !== 0) {
                $this->prWarning($prNumber, "Git add failed");
                return;
            }
        }

        // Commit changes
        if ($this->persist) {
            $gitCommit = "cd " . escapeshellarg($repoDir) . " && git commit -m 'Update leaderboard'";
            $this->prLine($prNumber, $gitCommit);
            exec($gitCommit, $output, $returnCode);

            if ($returnCode !== 0) {
                $this->prWarning($prNumber, "Nothing to commit");
                return;
            }
        }

        // Push changes
        if ($this->persist) {
            $gitPush = "cd " . escapeshellarg($repoDir) . " && git push";
            $this->prLine($prNumber, $gitPush);
            exec($gitPush, $output, $returnCode);

            if ($returnCode !== 0) {
                $this->prError($prNumber, "Git push failed");
                return;
            }
        } else {
            $this->prWarning($prNumber, "Skipping leaderboard update as --persist is not enabled.");
        }

        $this->prSuccess($prNumber, "{$file} updated!");
    }

    private function prLine(int $prNumber, string $message): void
    {
        $this->writeWithTimestamp("<style=\"fg-blue\">[#$prNumber]</style> $message");
    }

    private function prInfo(int $prNumber, string $message): void
    {
        $this->writeWithTimestamp("<style=\"bold fg-blue\">[#$prNumber] $message</style>");
    }

    private function prSuccess(int $prNumber, string $message): void
    {
        $this->writeWithTimestamp("<style=\"bold fg-green\">[#$prNumber] $message</style>");
    }

    private function prError(int $prNumber, string $message): void
    {
        $this->writeWithTimestamp("<style=\"bold fg-red\">[#$prNumber] $message</style>");
    }

    private function prWarning(int $prNumber, string $message): void
    {
        $this->writeWithTimestamp("<style=\"bold fg-yellow\">[#$prNumber] $message</style>");
    }

    private function writeWithTimestamp(string $message): void
    {
        $time = date('Y-m-d H:i:s');

        $this->writeln("<style=\"dim\">[{$time}]</style> {$message}");
    }
}