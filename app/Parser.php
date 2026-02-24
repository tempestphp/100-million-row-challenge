<?php

namespace App;

final class Parser
{
    private const int DOMAIN_PREFIX_LENGTH    = 19;
    private const int TIMESTAMP_SUFFIX_LENGTH = 15;
    private const int LINE_OVERHEAD           = 34;
    private const int READ_BUFFER_SIZE        = 8388608; // 8 MB

    public function parse(string $inputPath, string $outputPath): void
    {
        ini_set('memory_limit', '512M');

        $fileSize = filesize($inputPath);
        $workerCount  = $this->getCpuCount();
        $boundaries   = $this->findChunkBoundaries($inputPath, $fileSize, $workerCount);
        $mergedCounts = $this->forkAndProcess($inputPath, $boundaries, $workerCount);
        $grouped      = $this->groupByUrlAndDate($mergedCounts);

        unset($mergedCounts);

        foreach ($grouped as &$dates) {
            ksort($dates);
        }

        file_put_contents($outputPath, json_encode($grouped, JSON_PRETTY_PRINT));
    }

    private function getCpuCount(): int
    {
        return (int)(trim(shell_exec('nproc 2>/dev/null') ?: shell_exec('sysctl -n hw.ncpu 2>/dev/null') ?: '2'));
    }

    private function findChunkBoundaries(string $filePath, int $fileSize, int $workerCount): array
    {
        $boundaries = [0];
        $handle     = fopen($filePath, 'r');

        for ($i = 1; $i < $workerCount; $i++) {
            fseek($handle, (int) ($fileSize / $workerCount * $i));
            fgets($handle);
            $boundaries[] = ftell($handle);
        }

        $boundaries[] = $fileSize;
        fclose($handle);

        return $boundaries;
    }

    private function forkAndProcess(string $inputPath, array $boundaries, int $workerCount): array
    {
        $tmpFiles = $pids = [];

        for ($i = 0; $i < $workerCount - 1; $i++) {
            $tmpFiles[$i] = tempnam(sys_get_temp_dir(), 'parse_');
            $pid = pcntl_fork();

            if ($pid === 0) {
                $counts = $this->processChunk($inputPath, $boundaries[$i], $boundaries[$i + 1]);
                file_put_contents($tmpFiles[$i], serialize($counts));
                exit;
            }

            $pids[$i] = $pid;
        }

        $parentCounts = $this->processChunk($inputPath, $boundaries[$workerCount - 1], $boundaries[$workerCount]);

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        return $this->mergeCounts($tmpFiles, $parentCounts);
    }

    private function mergeCounts(array $tmpFiles, array $parentCounts): array
    {
        if ($tmpFiles === []) {
            return $parentCounts;
        }

        $counts = unserialize(file_get_contents($tmpFiles[0]));
        unlink($tmpFiles[0]);

        set_error_handler(fn() => null);

        for ($i = 1, $n = count($tmpFiles); $i < $n; $i++) {
            $childCounts = unserialize(file_get_contents($tmpFiles[$i]));
            unlink($tmpFiles[$i]);

            foreach ($childCounts as $key => $count) {
                $counts[$key] += $count;
            }
            unset($childCounts);
        }

        foreach ($parentCounts as $key => $count) {
            $counts[$key] += $count;
        }

        restore_error_handler();

        return $counts;
    }

    private function groupByUrlAndDate(array $counts): array
    {
        $grouped = [];

        foreach ($counts as $key => $count) {
            $separatorPos = strrpos($key, ',');
            $url = substr($key, 0, $separatorPos);
            $date = substr($key, $separatorPos + 1);
            $grouped[$url][$date] = $count;
        }

        return $grouped;
    }

    private function processChunk(string $filePath, int $start, int $end): array
    {
        $counts = [];
        $handle = fopen($filePath, 'r');
        fseek($handle, $start);

        $bytesRemaining = $end - $start;
        $leftover = '';

        set_error_handler(fn() => null);

        while ($bytesRemaining > 0) {
            $buffer = fread($handle, min(self::READ_BUFFER_SIZE, $bytesRemaining));
            if ($buffer === false) break;
            $bytesRemaining -= strlen($buffer);

            if ($leftover !== '') {
                $buffer = $leftover . $buffer;
                $leftover = '';
            }

            $lastNewline = strrpos($buffer, "\n");
            if ($lastNewline === false) {
                $leftover = $buffer;
                continue;
            }

            if ($lastNewline < strlen($buffer) - 1) {
                $leftover = substr($buffer, $lastNewline + 1);
            }

            $pos = 0;
            while ($pos < $lastNewline) {
                $newlinePos = strpos($buffer, "\n", $pos);
                $counts[substr($buffer, $pos + self::DOMAIN_PREFIX_LENGTH, $newlinePos - $pos - self::LINE_OVERHEAD)]++;
                $pos = $newlinePos + 1;
            }
        }

        if ($leftover !== '') {
            $counts[substr($leftover, self::DOMAIN_PREFIX_LENGTH, -self::TIMESTAMP_SUFFIX_LENGTH)]++;
        }

        restore_error_handler();
        fclose($handle);

        return $counts;
    }
}
