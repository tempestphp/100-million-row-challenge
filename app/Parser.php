<?php

namespace App;

use App\Commands\Visit;

final class Parser
{
    private const int WORKER_COUNT = 8;

    private array $routeMap = [];
    private array $routeList = [];
    private array $dateChars = [];
    private array $dateList = [];
    private int $dateCount = 2200;
    private int $pathCount = 0;

    public function __construct()
    {
        $this->buildRouteMap();
    }

    private function workerFile(int $i): string
    {
        return sys_get_temp_dir() . "/100m_worker_{$i}.bin";
    }

    private function buildRouteMap(): void
    {
        foreach (Visit::all() as $id => $visit) {
            $slug = substr($visit->uri, 25);
            $path = substr($visit->uri, 19);
            $this->routeMap[$slug] = $id;
            $this->routeList[$id] = $path;
        }
        $this->pathCount = count($this->routeList);

        $epoch = strtotime('2021-01-01');
        for ($d = 0; $d < $this->dateCount; $d++) {
            $full = date('Y-m-d', $epoch + $d * 86400);
            $this->dateChars[substr($full, 3)] = pack('v', $d);
            $this->dateList[$d] = $full;
        }
    }

    /**
     * Quick scan of first ~200KB to determine URL encounter order
     */
    private function discoverOrder(string $inputPath): array
    {
        $handle = fopen($inputPath, 'rb');
        $chunk = fread($handle, 204800);
        fclose($handle);

        $seen = [];
        $lastNl = strrpos($chunk, "\n");
        $p = 0;

        while ($p < $lastNl) {
            $c = strpos($chunk, ",", $p);
            if ($c === false) break;

            $slug = substr($chunk, $p + 25, $c - $p - 25);
            if (!isset($seen[$slug]) && isset($this->routeMap[$slug])) {
                $seen[$slug] = $this->routeMap[$slug];
            }

            $nl = strpos($chunk, "\n", $c);
            if ($nl === false) {
                break;
            }
            $p = $nl + 1;
        }

        $order = array_values($seen);

        // Append any routes not seen in first 200KB
        $inOrder = array_flip($order);
        for ($i = 0; $i < $this->pathCount; $i++) {
            if (!isset($inOrder[$i])) {
                $order[] = $i;
            }
        }

        return $order;
    }

    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);
        $routeOrder = $this->discoverOrder($inputPath);

        // Align chunks to newlines
        $boundaries = [0];
        $handle = fopen($inputPath, 'rb');
        for ($i = 1; $i < self::WORKER_COUNT; $i++) {
            fseek($handle, (int)(($fileSize * $i) / self::WORKER_COUNT));
            fgets($handle);
            $boundaries[] = ftell($handle);
        }
        $boundaries[] = $fileSize;
        fclose($handle);

        $pids = [];

        for ($i = 0; $i < self::WORKER_COUNT; $i++) {
            $pid = pcntl_fork();

            if ($pid === -1) {
                throw new \RuntimeException('Fork failed');
            }

            if ($pid === 0) {
                $buckets = $this->performTask($inputPath, $boundaries[$i], $boundaries[$i + 1]);
                $this->writeBuckets($i, $buckets);
                exit(0);
            }

            $pids[] = $pid;
        }

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        // Merge
        $counts = array_fill(0, $this->pathCount * $this->dateCount, 0);

        for ($i = 0; $i < self::WORKER_COUNT; $i++) {
            $path = $this->workerFile($i);
            $raw = file_get_contents($path);
            unlink($path);

            $childCounts = unpack('V*', $raw);
            $j = 0;
            foreach ($childCounts as $val) {
                $counts[$j++] += $val;
            }
        }

        // Build results
        $results = [];
        foreach ($routeOrder as $p) {
            $route = $this->routeList[$p];
            $base = $p * $this->dateCount;

            for ($d = 0; $d < $this->dateCount; $d++) {
                $n = $counts[$base + $d];
                if ($n === 0) {
                    continue;
                }
                $results[$route][$this->dateList[$d]] = $n;
            }
        }

        file_put_contents($outputPath, json_encode($results, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT));
    }

    private function performTask(string $inputPath, int $start, int $end): array
    {
        $handle = fopen($inputPath, 'rb');
        fseek($handle, $start);

        $pathIds = &$this->routeMap;
        $dateChars = &$this->dateChars;

        $buckets = array_fill(0, $this->pathCount, '');

        $bytesProcessed = 0;
        $toProcess = $end - $start;

        while ($bytesProcessed < $toProcess) {
            $remaining = $toProcess - $bytesProcessed;
            $chunk = fread($handle, min($remaining, 131072));
            if (!$chunk) {
                break;
            }

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) {
                break;
            }

            $tail = strlen($chunk) - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
            }
            $bytesProcessed += $lastNl + 1;

            $p = 0;

            while ($p < $lastNl) {
                $c = strpos($chunk, ",", $p);
                if ($c === false || $c >= $lastNl) {
                    break;
                }

                // Extract slug: skip 25-char URL prefix
                $slug = substr($chunk, $p + 25, $c - $p - 25);
                $pathId = $pathIds[$slug] ?? null;

                if ($pathId !== null) {
                    $dateKey = substr($chunk, $c + 4, 7);
                    if (isset($dateChars[$dateKey])) {
                        $buckets[$pathId] .= $dateChars[$dateKey];
                    }
                }

                $nl = strpos($chunk, "\n", $c);
                if ($nl === false) {
                    break;
                }
                $p = $nl + 1;
            }
        }

        fclose($handle);
        return $buckets;
    }

    private function writeBuckets(int $workerId, array &$buckets): void
    {
        $counts = array_fill(0, $this->pathCount * $this->dateCount, 0);

        $base = 0;
        foreach ($buckets as $bucket) {
            if ($bucket !== '') {
                foreach (array_count_values(unpack('v*', $bucket)) as $dateId => $n) {
                    $counts[$base + $dateId] += $n;
                }
            }
            $base += $this->dateCount;
        }

        file_put_contents($this->workerFile($workerId), pack('V*', ...$counts));
    }
}