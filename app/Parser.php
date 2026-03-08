<?php

namespace App;

final class Parser
{
    private const int PREFIX_LENGTH    = 25;
    private const int LINE_OVERHEAD    = 51;
    private const int TIMESTAMP_TAIL   = 25;
    private const int READ_BUFFER_SIZE = 4_194_304;
    private const int MIN_WORKERS      = 6;

    public function parse(string $inputPath, string $outputPath): void
    {
        ini_set('memory_limit', '256M');

        $fileSize    = filesize($inputPath);
        $workerCount = max($this->getCpuCount(), self::MIN_WORKERS);
        $boundaries  = $this->findChunkBoundaries($inputPath, $fileSize, $workerCount);

        [$paths, $dates, $counts] = $this->forkAndProcess($inputPath, $boundaries, $workerCount);

        $this->writeJson($outputPath, $paths, $dates, $counts);
    }

    private function getCpuCount(): int
    {
        return (int) trim(shell_exec('nproc 2>/dev/null') ?: shell_exec('sysctl -n hw.ncpu 2>/dev/null') ?: '2');
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
        $tmpDir = sys_get_temp_dir();
        $myPid  = getmypid();
        $tmpFiles = $pids = [];

        for ($i = 0; $i < $workerCount - 1; $i++) {
            $tmpFiles[$i] = $tmpDir . '/parse_' . $myPid . '_' . $i;
            $pid = pcntl_fork();

            if ($pid === 0) {
                $state = $this->processChunk($inputPath, $boundaries[$i], $boundaries[$i + 1]);
                $payload = function_exists('igbinary_serialize') ? igbinary_serialize($state) : serialize($state);
                file_put_contents($tmpFiles[$i], $payload);
                exit;
            }

            $pids[$i] = $pid;
        }

        $parentState = $this->processChunk($inputPath, $boundaries[$workerCount - 1], $boundaries[$workerCount]);

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        return $this->mergeAll($tmpFiles, $parentState);
    }

    private function processChunk(string $filePath, int $start, int $end): array
    {
        $pathIds = $paths = $dateIds = $dates = $counts = [];
        $nextPathId = $nextDateId = 0;

        $pre     = self::PREFIX_LENGTH;
        $strip   = self::LINE_OVERHEAD;
        $tail    = self::TIMESTAMP_TAIL;
        $bufSize = self::READ_BUFFER_SIZE;

        $offset = $start;
        $leftover = '';

        while ($offset < $end) {
            $readLen = min($bufSize, $end - $offset);
            $buffer = file_get_contents($filePath, false, null, $offset, $readLen);
            if ($buffer === false) break;
            $offset += $readLen;

            if ($leftover !== '') {
                $buffer = $leftover . $buffer;
                $leftover = '';
            }

            $lastNl = strrpos($buffer, "\n");
            if ($lastNl === false) {
                $leftover = $buffer;
                continue;
            }

            if ($lastNl < strlen($buffer) - 1) {
                $leftover = substr($buffer, $lastNl + 1);
            }

            $pos = 0;
            while ($pos < $lastNl) {
                $nlPos = strpos($buffer, "\n", $pos);

                $path = substr($buffer, $pos + $pre, $nlPos - $pos - $strip);
                $date = substr($buffer, $nlPos - $tail, 10);

                if (!isset($pathIds[$path])) {
                    $pathIds[$path] = $nextPathId;
                    $paths[$nextPathId] = $path;
                    $counts[$nextPathId] = array_fill(0, $nextDateId, 0);
                    $pathId = $nextPathId++;
                } else {
                    $pathId = $pathIds[$path];
                }

                if (!isset($dateIds[$date])) {
                    $dateIds[$date] = $nextDateId;
                    $dates[$nextDateId] = $date;
                    $dateId = $nextDateId++;

                    for ($c = 0; $c < $nextPathId; $c++) {
                        $counts[$c][] = 0;
                    }
                } else {
                    $dateId = $dateIds[$date];
                }

                $counts[$pathId][$dateId]++;
                $pos = $nlPos + 1;
            }
        }

        return [$paths, $dates, $counts];
    }

    private function mergeAll(array $tmpFiles, array $parentState): array
    {
        $mergedPaths = $mergedPathIds = $mergedDates = $mergedDateIds = $mergedCounts = [];
        $nextPathId = $nextDateId = 0;

        foreach ($tmpFiles as $tmpFile) {
            $raw = file_get_contents($tmpFile);
            unlink($tmpFile);
            $state = function_exists('igbinary_unserialize') ? igbinary_unserialize($raw) : unserialize($raw);
            $this->mergeState($mergedPaths, $mergedPathIds, $nextPathId, $mergedDates, $mergedDateIds, $nextDateId, $mergedCounts, $state);
        }

        $this->mergeState($mergedPaths, $mergedPathIds, $nextPathId, $mergedDates, $mergedDateIds, $nextDateId, $mergedCounts, $parentState);

        return [$mergedPaths, $mergedDates, $mergedCounts];
    }

    private function mergeState(
        array &$mergedPaths,
        array &$mergedPathIds,
        int   &$nextPathId,
        array &$mergedDates,
        array &$mergedDateIds,
        int   &$nextDateId,
        array &$mergedCounts,
        array  $state,
    ): void {
        [$paths, $dates, $counts] = $state;

        $dateMap = [];
        foreach ($dates as $dateId => $date) {
            if (isset($mergedDateIds[$date])) {
                $dateMap[$dateId] = $mergedDateIds[$date];
            } else {
                $mergedDateIds[$date] = $nextDateId;
                $mergedDates[$nextDateId] = $date;
                $dateMap[$dateId] = $nextDateId;
                $nextDateId++;

                foreach ($mergedCounts as &$pathCounts) {
                    $pathCounts[] = 0;
                }
                unset($pathCounts);
            }
        }

        foreach ($paths as $pathId => $path) {
            if (isset($mergedPathIds[$path])) {
                $mergedPathId = $mergedPathIds[$path];
            } else {
                $mergedPathIds[$path] = $nextPathId;
                $mergedPaths[$nextPathId] = $path;
                $mergedCounts[$nextPathId] = array_fill(0, $nextDateId, 0);
                $mergedPathId = $nextPathId++;
            }

            foreach ($counts[$pathId] as $dateId => $count) {
                if ($count === 0) continue;
                $mergedCounts[$mergedPathId][$dateMap[$dateId]] += $count;
            }
        }
    }

    private function writeJson(string $outputPath, array $paths, array $dates, array $counts): void
    {
        $sortedDates = $dates;
        asort($sortedDates);
        $orderedDateIds = array_keys($sortedDates);

        $out = fopen($outputPath, 'wb');
        $buf = '{';
        $firstPath = true;

        foreach ($paths as $pathId => $path) {
            if (!$firstPath) {
                $buf .= ',';
            }
            $firstPath = false;

            $buf .= "\n    \"\\/blog\\/" . $path . '": {';

            $firstDate = true;
            $pathCounts = $counts[$pathId];

            foreach ($orderedDateIds as $dateId) {
                $count = $pathCounts[$dateId] ?? 0;
                if ($count === 0) continue;

                if ($firstDate) {
                    $buf .= "\n";
                    $firstDate = false;
                } else {
                    $buf .= ",\n";
                }

                $buf .= "        \"{$dates[$dateId]}\": {$count}";
            }

            $buf .= "\n    }";

            if (strlen($buf) > 65536) {
                fwrite($out, $buf);
                $buf = '';
            }
        }

        $buf .= "\n}";
        fwrite($out, $buf);
        fclose($out);
    }
}
