<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        if (function_exists('pcntl_fork') && function_exists('pcntl_waitpid')) {
            $this->parseParallel($inputPath, $outputPath, 8);

            return;
        }

        $this->parseSingle($inputPath, $outputPath);
    }

    private function parseSingle(string $inputPath, string $outputPath): void
    {
        $handle = fopen($inputPath, 'r');
        $visits = [];
    
        while (($line = fgets($handle)) !== false) {
            $separatorPosition = strlen($line) - 27;
            $path = substr($line, 19, $separatorPosition - 19);
            $date = substr($line, $separatorPosition + 1, 10);
            
            if (isset($visits[$path][$date])) {
                ++$visits[$path][$date];
            } else {
                $visits[$path][$date] = 1;
            }
        }

        foreach ($visits as &$dailyVisits) {
            ksort($dailyVisits);
        }

        file_put_contents($outputPath, json_encode($visits, JSON_PRETTY_PRINT));
    }

    private function parseParallel(string $inputPath, string $outputPath, int $workers): void
    {
        $fileSize = filesize($inputPath);
        if ($fileSize === false || $workers < 2) {
            $this->parseSingle($inputPath, $outputPath);

            return;
        }

        $chunkSize = (int) ceil($fileSize / $workers);
        $pids = [];
        $tempFiles = [];

        for ($i = 0; $i < $workers; $i++) {
            $tempFile = sys_get_temp_dir() . "/parser_{$i}.tmp";
            $tempFiles[] = $tempFile;

            $pid = pcntl_fork();

            if ($pid === -1) {
                $this->parseSingle($inputPath, $outputPath);

                return;
            }

            if ($pid === 0) {
                $start = $i * $chunkSize;
                $end = min($fileSize, ($i + 1) * $chunkSize);
                $isLast = $i === $workers - 1;

                $this->processRange($inputPath, $tempFile, $start, $end, $isLast);
                exit(0);
            }

            $pids[] = $pid;
        }

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $visits = [];
        foreach ($tempFiles as $tempFile) {
            if (! file_exists($tempFile)) {
                continue;
            }

            $chunk = unserialize(file_get_contents($tempFile));
            unlink($tempFile);

            if (! is_array($chunk)) {
                continue;
            }

            foreach ($chunk as $path => $dates) {
                if (! isset($visits[$path])) {
                    $visits[$path] = $dates;
                    continue;
                }

                foreach ($dates as $date => $count) {
                    if (isset($visits[$path][$date])) {
                        $visits[$path][$date] += $count;
                    } else {
                        $visits[$path][$date] = $count;
                    }
                }
            }

            unset($chunk);
        }

        foreach ($visits as &$dailyVisits) {
            ksort($dailyVisits);
        }

        file_put_contents($outputPath, json_encode($visits, JSON_PRETTY_PRINT));
    }

    private function processRange(string $inputPath, string $tempFile, int $start, int $end, bool $isLast): void
    {
        $handle = fopen($inputPath, 'r');
        if ($handle === false) {
            return;
        }

        fseek($handle, $start);
        if ($start > 0) {
            fgets($handle);
        }

        $visits = [];

        while (($lineStart = ftell($handle)) !== false && ($line = fgets($handle)) !== false) {
            if (! $isLast && $lineStart >= $end) {
                break;
            }

            $separatorPosition = strlen($line) - 27;
            $path = substr($line, 19, $separatorPosition - 19);
            $date = substr($line, $separatorPosition + 1, 10);

            if (isset($visits[$path][$date])) {
                ++$visits[$path][$date];
            } else {
                $visits[$path][$date] = 1;
            }
        }

        file_put_contents($tempFile, serialize($visits));
    }
}
