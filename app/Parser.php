<?php
declare(strict_types=1);

namespace App;

final class Parser
{
    private const int READ_CHUNK_SIZE = 8_388_608;
    private const int URI_PREFIX_LENGTH = 19;
    private const int DATE_LENGTH = 10;
    private const int DATE_OFFSET_FROM_NL = 25;
    private const int MIN_LINE_LENGTH = 45;

    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);
        $numWorkers = $this->resolveWorkerCount();

        $boundaries = $this->calculateBoundaries(
            $inputPath,
            $fileSize,
            $numWorkers,
        );

        $tmpDir = sys_get_temp_dir();
        $myPid = getmypid();
        $tmpFiles = [];
        $pids = [];

        for ($i = 0; $i < $numWorkers; $i++) {
            $tmpFile = $tmpDir . '/parse_' . $myPid . '_' . $i;
            $tmpFiles[$i] = $tmpFile;

            $pid = pcntl_fork();

            if ($pid === 0) {
                $data = $this->parseRangeState(
                    $inputPath,
                    $boundaries[$i],
                    $boundaries[$i + 1],
                );

                file_put_contents($tmpFile, $this->encodeWorkerData($data));

                exit(0);
            }

            if ($pid < 0) {
                throw new \RuntimeException('Unable to fork parser worker');
            }

            $pids[$i] = $pid;
        }

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $mergedPaths = [];
        $mergedPathIds = [];
        $mergedDates = [];
        $mergedDateIds = [];
        $mergedCounts = [];

        foreach ($tmpFiles as $tmpFile) {
            $data = $this->decodeWorkerData(file_get_contents($tmpFile));
            unlink($tmpFile);

            if (!is_array($data) || count($data) !== 3) {
                throw new \RuntimeException('Unable to unserialize parser worker result');
            }

            $this->mergeState(
                $mergedPaths,
                $mergedPathIds,
                $mergedDates,
                $mergedDateIds,
                $mergedCounts,
                $data,
            );
        }

        $merged = $this->materializeState($mergedPaths, $mergedDates, $mergedCounts);

        $this->writeOutput($outputPath, $merged);
    }

    private function resolveWorkerCount(): int
    {
        $cpuCount = $this->detectCpuCount();

        return max(2, min(16, $cpuCount - 2));
    }

    private function detectCpuCount(): int
    {
        static $cpuCount = null;

        if ($cpuCount !== null) {
            return $cpuCount;
        }

        $output = '';

        if (PHP_OS_FAMILY === 'Darwin') {
            $output = trim((string) shell_exec('sysctl -n hw.logicalcpu 2>/dev/null'));
        }

        if ($output === '' && PHP_OS_FAMILY === 'Linux') {
            $output = trim((string) shell_exec('nproc 2>/dev/null'));

            if ($output === '') {
                $output = trim((string) shell_exec('getconf _NPROCESSORS_ONLN 2>/dev/null'));
            }
        }

        $cpuCount = (int) $output;

        if ($cpuCount < 1) {
            $cpuCount = 1;
        }

        return $cpuCount;
    }

    private function encodeWorkerData(array $data): string
    {
        if (function_exists('igbinary_serialize')) {
            return (string) igbinary_serialize($data);
        }

        return serialize($data);
    }

    private function decodeWorkerData(string|false $payload): mixed
    {
        if ($payload === false) {
            return null;
        }

        if (function_exists('igbinary_unserialize')) {
            return igbinary_unserialize($payload);
        }

        return unserialize($payload);
    }

    private function calculateBoundaries(
        string $inputPath,
        int $fileSize,
        int $numWorkers,
    ): array {
        $chunkSize = (int) ($fileSize / $numWorkers);
        $boundaries = [0];

        $handle = fopen($inputPath, 'rb');

        for ($i = 1; $i < $numWorkers; $i++) {
            fseek($handle, $i * $chunkSize);
            fgets($handle);
            $boundaries[] = ftell($handle);
        }

        fclose($handle);

        $boundaries[] = $fileSize;

        return $boundaries;
    }

    private function parseRangeState(string $inputPath, int $start, int $end): array
    {
        $pathIds = [];
        $paths = [];
        $dateIds = [];
        $dates = [];
        $counts = [];

        $handle = fopen($inputPath, 'rb');
        fseek($handle, $start);

        $toRead = $end - $start;
        $remainder = '';

        while ($toRead > 0) {
            $chunk = fread($handle, min(self::READ_CHUNK_SIZE, $toRead));

            if ($chunk === '' || $chunk === false) {
                break;
            }

            $toRead -= strlen($chunk);

            if ($remainder !== '') {
                $chunk = $remainder . $chunk;
                $remainder = '';
            }

            $lastNl = strrpos($chunk, "\n");

            if ($lastNl === false) {
                $remainder = $chunk;
                continue;
            }

            $chunkLen = strlen($chunk);

            if ($lastNl < $chunkLen - 1) {
                $remainder = substr($chunk, $lastNl + 1);
            }

            $pos = 0;

            while ($pos < $lastNl) {
                $nlPos = strpos($chunk, "\n", $pos);

                if ($nlPos === false) {
                    $nlPos = $lastNl;
                }

                $lineLen = $nlPos - $pos;
                $path = substr(
                    $chunk,
                    $pos + self::URI_PREFIX_LENGTH,
                    $lineLen - self::MIN_LINE_LENGTH,
                );
                $date = substr(
                    $chunk,
                    $nlPos - self::DATE_OFFSET_FROM_NL,
                    self::DATE_LENGTH,
                );

                if (isset($pathIds[$path])) {
                    $pathId = $pathIds[$path];
                } else {
                    $pathId = count($paths);
                    $pathIds[$path] = $pathId;
                    $paths[$pathId] = $path;
                    $counts[$pathId] = array_fill(0, count($dates), 0);
                }

                if (isset($dateIds[$date])) {
                    $dateId = $dateIds[$date];
                } else {
                    $dateId = count($dates);
                    $dateIds[$date] = $dateId;
                    $dates[$dateId] = $date;

                    foreach ($counts as &$pathCounts) {
                        $pathCounts[] = 0;
                    }
                    unset($pathCounts);
                }

                $counts[$pathId][$dateId]++;

                $pos = $nlPos + 1;
            }
        }

        if ($remainder !== '') {
            $len = strlen($remainder);

            if ($len > self::MIN_LINE_LENGTH) {
                $path = substr(
                    $remainder,
                    self::URI_PREFIX_LENGTH,
                    $len - self::MIN_LINE_LENGTH,
                );
                $date = substr(
                    $remainder,
                    $len - self::DATE_OFFSET_FROM_NL,
                    self::DATE_LENGTH,
                );

                if (isset($pathIds[$path])) {
                    $pathId = $pathIds[$path];
                } else {
                    $pathId = count($paths);
                    $pathIds[$path] = $pathId;
                    $paths[$pathId] = $path;
                    $counts[$pathId] = array_fill(0, count($dates), 0);
                }

                if (isset($dateIds[$date])) {
                    $dateId = $dateIds[$date];
                } else {
                    $dateId = count($dates);
                    $dateIds[$date] = $dateId;
                    $dates[$dateId] = $date;

                    foreach ($counts as &$pathCounts) {
                        $pathCounts[] = 0;
                    }
                    unset($pathCounts);
                }

                $counts[$pathId][$dateId]++;
            }
        }

        fclose($handle);

        return [$paths, $dates, $counts];
    }

    private function mergeState(
        array &$mergedPaths,
        array &$mergedPathIds,
        array &$mergedDates,
        array &$mergedDateIds,
        array &$mergedCounts,
        array $state,
    ): void {
        [$paths, $dates, $counts] = $state;

        $dateMap = [];

        foreach ($dates as $dateId => $date) {
            if (isset($mergedDateIds[$date])) {
                $mergedDateId = $mergedDateIds[$date];
            } else {
                $mergedDateId = count($mergedDates);
                $mergedDateIds[$date] = $mergedDateId;
                $mergedDates[$mergedDateId] = $date;

                foreach ($mergedCounts as &$pathCounts) {
                    $pathCounts[] = 0;
                }
                unset($pathCounts);
            }

            $dateMap[$dateId] = $mergedDateId;
        }

        foreach ($paths as $pathId => $path) {
            if (isset($mergedPathIds[$path])) {
                $mergedPathId = $mergedPathIds[$path];
            } else {
                $mergedPathId = count($mergedPaths);
                $mergedPathIds[$path] = $mergedPathId;
                $mergedPaths[$mergedPathId] = $path;
                $mergedCounts[$mergedPathId] = array_fill(0, count($mergedDates), 0);
            }

            foreach ($counts[$pathId] as $dateId => $count) {
                if ($count === 0) {
                    continue;
                }

                $mergedDateId = $dateMap[$dateId];
                $mergedCounts[$mergedPathId][$mergedDateId] += $count;
            }
        }
    }

    private function materializeState(array $paths, array $dates, array $counts): array
    {
        $sortedDates = $dates;
        asort($sortedDates);
        $orderedDateIds = array_keys($sortedDates);

        $result = [];

        foreach ($paths as $pathId => $path) {
            $dateCounts = [];
            $pathCounts = $counts[$pathId];

            foreach ($orderedDateIds as $dateId) {
                $count = $pathCounts[$dateId];

                if ($count === 0) {
                    continue;
                }

                $dateCounts[$dates[$dateId]] = $count;
            }

            $result[$path] = $dateCounts;
        }

        return $result;
    }

    private function writeOutput(string $outputPath, array $data): void
    {
        $out = fopen($outputPath, 'wb');
        fwrite($out, '{');

        $firstPath = true;

        foreach ($data as $path => $dates) {
            if (!$firstPath) {
                fwrite($out, ',');
            }

            $firstPath = false;
            $escapedPath = str_replace('/', '\\/', $path);

            $entries = [];

            foreach ($dates as $date => $count) {
                $entries[] = "        \"{$date}\": {$count}";
            }

            fwrite($out, "\n    \"{$escapedPath}\": {\n" . implode(",\n", $entries) . "\n    }");
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
