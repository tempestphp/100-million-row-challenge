<?php

namespace App;

final class Parser
{
    private const DOMAIN_LEN = 19;       // strlen('https://stitcher.io')
    private const DATE_LEN = 10;         // strlen('YYYY-MM-DD')
    private const TIMESTAMP_TAIL = 25;   // strlen('YYYY-MM-DDTHH:MM:SS+00:00')
    private const LINE_OVERHEAD = 45;    // DOMAIN_LEN + 1 (comma) + TIMESTAMP_TAIL
    private const READ_CHUNK = 1_048_576; // 1MB
    private const WRITE_CHUNK = 1_048_576;
    private const WORKERS = 4;

    /** @var string[] */
    private array $paths = [];
    /** @var array<string, int> */
    private array $pathIds = [];
    /** @var string[] */
    private array $dates = [];
    /** @var array<string, int> */
    private array $dateIds = [];
    /** @var array<int, array<int, int>> */
    private array $counts = [];

    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);

        if ($fileSize > 5_000_000 && function_exists('pcntl_fork')) {
            $this->parseParallel($inputPath, $outputPath, $fileSize);
        } else {
            $this->processRange($inputPath, 0, $fileSize);
            $this->writeJson($outputPath);
        }
    }

    private function parseParallel(string $inputPath, string $outputPath, int $fileSize): void
    {
        $workers = self::WORKERS;
        $boundaries = $this->findBoundaries($inputPath, $fileSize, $workers);

        $pids = [];
        $tmpFiles = [];
        $myPid = getmypid();

        // Fork workers for all chunks except the last
        for ($i = 0; $i < $workers - 1; $i++) {
            $tmpFile = sys_get_temp_dir() . '/p_' . $myPid . '_' . $i;
            $tmpFiles[$i] = $tmpFile;

            $pid = pcntl_fork();
            if ($pid === 0) {
                // Child
                $this->processRange($inputPath, $boundaries[$i], $boundaries[$i + 1]);
                $state = [
                    $this->paths,
                    $this->pathIds,
                    $this->dates,
                    $this->dateIds,
                    $this->counts,
                ];
                file_put_contents($tmpFile, function_exists('igbinary_serialize')
                    ? igbinary_serialize($state)
                    : serialize($state));
                exit(0);
            }
            $pids[] = $pid;
        }

        // Parent processes the last chunk
        $this->processRange($inputPath, $boundaries[$workers - 1], $boundaries[$workers]);

        // Wait for all children
        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        // Merge results from children (in order, to preserve first-appearance URL order)
        // First, save parent state as the "merged" base
        $mergedPaths = $this->paths;
        $mergedPathIds = $this->pathIds;
        $mergedDates = $this->dates;
        $mergedDateIds = $this->dateIds;
        $mergedCounts = $this->counts;

        // We need to merge children in reverse order, then parent last
        // Actually: children processed chunks 0..N-2, parent processed chunk N-1
        // For first-appearance order: chunk 0 first, then chunk 1, ..., then parent
        // So we need children's data merged first, then parent's on top

        // Re-initialize
        $this->paths = [];
        $this->pathIds = [];
        $this->dates = [];
        $this->dateIds = [];
        $this->counts = [];

        // Merge children first (chunks 0 to workers-2)
        for ($i = 0; $i < $workers - 1; $i++) {
            $raw = file_get_contents($tmpFiles[$i]);
            $state = function_exists('igbinary_unserialize')
                ? igbinary_unserialize($raw)
                : unserialize($raw);
            unlink($tmpFiles[$i]);
            $this->mergeState($state[0], $state[1], $state[2], $state[3], $state[4]);
        }

        // Merge parent's data (last chunk)
        $this->mergeState($mergedPaths, $mergedPathIds, $mergedDates, $mergedDateIds, $mergedCounts);

        $this->writeJson($outputPath);
    }

    private function mergeState(
        array $otherPaths,
        array $otherPathIds,
        array $otherDates,
        array $otherDateIds,
        array $otherCounts,
    ): void {
        $dateCount = count($this->dates);

        // Map other date IDs to merged date IDs
        $dateMap = [];
        foreach ($otherDates as $otherId => $date) {
            if (isset($this->dateIds[$date])) {
                $dateMap[$otherId] = $this->dateIds[$date];
            } else {
                $newId = $dateCount++;
                $this->dates[$newId] = $date;
                $this->dateIds[$date] = $newId;
                $dateMap[$otherId] = $newId;
            }
        }

        // Map other path IDs and merge counts
        foreach ($otherPaths as $otherId => $path) {
            if (isset($this->pathIds[$path])) {
                $mergedPathId = $this->pathIds[$path];
            } else {
                $mergedPathId = count($this->paths);
                $this->paths[$mergedPathId] = $path;
                $this->pathIds[$path] = $mergedPathId;
                $this->counts[$mergedPathId] = [];
            }

            $otherPathCounts = $otherCounts[$otherId];
            $merged = &$this->counts[$mergedPathId];

            foreach ($otherPathCounts as $otherDateId => $count) {
                if ($count === 0) continue;
                $mergedDateId = $dateMap[$otherDateId];
                $merged[$mergedDateId] = ($merged[$mergedDateId] ?? 0) + $count;
            }

            unset($merged);
        }
    }

    private function findBoundaries(string $inputPath, int $fileSize, int $workers): array
    {
        $boundaries = [0];
        $fp = fopen($inputPath, 'rb');

        for ($i = 1; $i < $workers; $i++) {
            $offset = (int)($fileSize * $i / $workers);
            fseek($fp, $offset);
            fgets($fp); // align to next line boundary
            $boundaries[] = ftell($fp);
        }

        fclose($fp);
        $boundaries[] = $fileSize;
        return $boundaries;
    }

    private function processRange(string $inputPath, int $start, int $end): void
    {
        $fp = fopen($inputPath, 'rb');
        stream_set_read_buffer($fp, 0);
        fseek($fp, $start);

        $remaining = $end - $start;
        $left = '';

        while ($remaining > 0) {
            $toRead = min(self::READ_CHUNK, $remaining);
            $chunk = fread($fp, $toRead);
            if ($chunk === false || $chunk === '') break;
            $remaining -= strlen($chunk);

            if ($left !== '') {
                $chunk = $left . $chunk;
                $left = '';
            }

            $len = strlen($chunk);
            $lastNl = strrpos($chunk, "\n");

            if ($lastNl === false) {
                $left = $chunk;
                continue;
            }

            if ($lastNl < $len - 1) {
                $left = substr($chunk, $lastNl + 1);
            }

            $pos = 0;
            while ($pos < $lastNl) {
                $nlPos = strpos($chunk, "\n", $pos);
                if ($nlPos === false) break;

                // Extract path: between domain end and (nlPos - LINE_OVERHEAD + DOMAIN_LEN)
                $lineLen = $nlPos - $pos;
                $pathLen = $lineLen - self::LINE_OVERHEAD;
                $path = substr($chunk, $pos + self::DOMAIN_LEN, $pathLen);

                // Extract date: 10 chars starting at TIMESTAMP_TAIL before newline
                $date = substr($chunk, $nlPos - self::TIMESTAMP_TAIL, self::DATE_LEN);

                // Get or create path ID
                if (isset($this->pathIds[$path])) {
                    $pid = $this->pathIds[$path];
                } else {
                    $pid = count($this->paths);
                    $this->paths[$pid] = $path;
                    $this->pathIds[$path] = $pid;
                    $this->counts[$pid] = [];
                }

                // Get or create date ID
                if (isset($this->dateIds[$date])) {
                    $did = $this->dateIds[$date];
                } else {
                    $did = count($this->dates);
                    $this->dates[$did] = $date;
                    $this->dateIds[$date] = $did;
                    // Expand all existing path count arrays
                    foreach ($this->counts as &$pc) {
                        $pc[$did] = 0;
                    }
                    unset($pc);
                }

                $this->counts[$pid][$did] = ($this->counts[$pid][$did] ?? 0) + 1;

                $pos = $nlPos + 1;
            }
        }

        if ($left !== '') {
            $lineLen = strlen($left);
            if ($lineLen > self::LINE_OVERHEAD) {
                $pathLen = $lineLen - self::LINE_OVERHEAD;
                $path = substr($left, self::DOMAIN_LEN, $pathLen);
                $date = substr($left, $lineLen - self::TIMESTAMP_TAIL, self::DATE_LEN);

                if (isset($this->pathIds[$path])) {
                    $pid = $this->pathIds[$path];
                } else {
                    $pid = count($this->paths);
                    $this->paths[$pid] = $path;
                    $this->pathIds[$path] = $pid;
                    $this->counts[$pid] = [];
                }

                if (isset($this->dateIds[$date])) {
                    $did = $this->dateIds[$date];
                } else {
                    $did = count($this->dates);
                    $this->dates[$did] = $date;
                    $this->dateIds[$date] = $did;
                    foreach ($this->counts as &$pc) {
                        $pc[$did] = 0;
                    }
                    unset($pc);
                }

                $this->counts[$pid][$did] = ($this->counts[$pid][$did] ?? 0) + 1;
            }
        }

        fclose($fp);
    }

    private function writeJson(string $outputPath): void
    {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, self::WRITE_CHUNK);

        // Sort dates and build sorted date index
        $dateCount = count($this->dates);
        $sortedDateIds = range(0, $dateCount - 1);
        usort($sortedDateIds, fn(int $a, int $b) => $this->dates[$a] <=> $this->dates[$b]);

        $buf = "{\n";
        $pathCount = count($this->paths);
        $firstPath = true;

        for ($p = 0; $p < $pathCount; $p++) {
            $counts = $this->counts[$p];
            $escapedPath = str_replace('/', '\\/', $this->paths[$p]);

            if (!$firstPath) {
                $buf .= ",\n";
            }
            $firstPath = false;

            $buf .= "    \"{$escapedPath}\": {\n";

            $firstDate = true;
            foreach ($sortedDateIds as $did) {
                $count = $counts[$did] ?? 0;
                if ($count === 0) continue;

                if (!$firstDate) {
                    $buf .= ",\n";
                }
                $firstDate = false;

                $buf .= "        \"{$this->dates[$did]}\": {$count}";
            }

            $buf .= "\n    }";

            if (strlen($buf) > self::WRITE_CHUNK) {
                fwrite($out, $buf);
                $buf = '';
            }
        }

        $buf .= "\n}";
        fwrite($out, $buf);
        fclose($out);
    }
}
