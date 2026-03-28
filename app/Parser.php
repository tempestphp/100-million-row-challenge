<?php

declare(strict_types=1);

namespace App;

use App\Commands\Visit;

use const SEEK_CUR;

final class Parser
{
    private const int CHUNK_SIZE = 163_840;
    private const int DISCOVER_SIZE = 2_097_152;
    private const int WORKERS = 10;

    public function parse(string $inputPath, string $outputPath): void
    {
        \gc_disable();
        \ini_set('memory_limit', '-1');

        $fileSize = \filesize($inputPath);

        // Date lookup: "YY-MM-DD" => 2-byte binary string
        $dateChars = [];
        $dates = [];
        $dateCount = 0;
        for ($y = 21; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => $y === 24 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $ms = ($m < 10 ? '0' : '') . $m;
                $ym = $y . '-' . $ms . '-';
                for ($d = 1; $d <= $maxD; $d++) {
                    $key = $ym . (($d < 10 ? '0' : '') . $d);
                    $dateChars[$key] = \chr($dateCount & 0xFF) . \chr($dateCount >> 8);
                    $dates[$dateCount] = $key;
                    $dateCount++;
                }
            }
        }

        // Discover paths from first 2MB
        $handle = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($handle, 0);
        $raw = \fread($handle, self::DISCOVER_SIZE);
        \fclose($handle);

        $pathIds = [];
        $paths = [];
        $pathCount = 0;
        $pos = 25;
        $lastNl = \strrpos($raw, "\n");

        while ($pos < $lastNl) {
            $c = \strpos($raw, ',', $pos);
            if ($c === false) break;
            $slug = \substr($raw, $pos, $c - $pos);
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug] = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }
            $pos = $c + 52;
        }
        unset($raw);

        foreach (Visit::all() as $v) {
            $slug = \substr($v->uri, 25);
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug] = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }
        }

        $totalCells = $pathCount * $dateCount;

        if (\function_exists('pcntl_fork') && $fileSize > self::DISCOVER_SIZE) {
            $this->parseParallel($inputPath, $outputPath, $fileSize, $pathIds, $paths, $pathCount, $dateChars, $dates, $dateCount, $totalCells);
        } else {
            $buckets = $this->parseRange($inputPath, 0, $fileSize, $pathIds, $dateChars, $pathCount);
            $counts = $this->bucketsToFlat($buckets, $pathCount, $dateCount, $totalCells);
            $this->writeJson($outputPath, $counts, $paths, $dates, $pathCount, $dateCount);
        }
    }

    private function parseParallel(
        string $inputPath, string $outputPath, int $fileSize,
        array $pathIds, array $paths, int $pathCount,
        array $dateChars, array $dates, int $dateCount, int $totalCells,
    ): void {
        $boundaries = [0];
        $bh = \fopen($inputPath, 'rb');
        for ($i = 1; $i < self::WORKERS; $i++) {
            \fseek($bh, (int)($fileSize * $i / self::WORKERS));
            \fgets($bh);
            $boundaries[] = \ftell($bh);
        }
        \fclose($bh);
        $boundaries[] = $fileSize;

        $tmpDir = \is_dir('/dev/shm') ? '/dev/shm' : \sys_get_temp_dir();
        $myPid = \getmypid();

        $children = [];
        for ($w = 0; $w < self::WORKERS - 1; $w++) {
            $tmpFile = $tmpDir . '/p100m_' . $myPid . '_' . $w;
            $pid = \pcntl_fork();

            if ($pid === 0) {
                $buckets = $this->parseRange(
                    $inputPath, $boundaries[$w], $boundaries[$w + 1],
                    $pathIds, $dateChars, $pathCount,
                );
                $flat = $this->bucketsToFlat($buckets, $pathCount, $dateCount, $totalCells);
                \file_put_contents($tmpFile, \pack('v*', ...$flat));
                exit(0);
            }

            if ($pid > 0) {
                $children[] = [$pid, $tmpFile];
            }
        }

        // Parent handles last chunk
        $buckets = $this->parseRange(
            $inputPath, $boundaries[self::WORKERS - 1], $fileSize,
            $pathIds, $dateChars, $pathCount,
        );
        $counts = $this->bucketsToFlat($buckets, $pathCount, $dateCount, $totalCells);
        unset($buckets);

        foreach ($children as [$cpid, $tmpFile]) {
            \pcntl_waitpid($cpid, $status);
            $wData = \unpack('v*', \file_get_contents($tmpFile));
            \unlink($tmpFile);
            $j = 0;
            foreach ($wData as $v) {
                $counts[$j++] += $v;
            }
        }

        $this->writeJson($outputPath, $counts, $paths, $dates, $pathCount, $dateCount);
    }

    private function parseRange(
        string $inputPath, int $start, int $end,
        array $pathIds, array $dateChars, int $pathCount,
    ): array {
        $buckets = \array_fill(0, $pathCount, '');

        $h = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($h, 0);
        \fseek($h, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $toRead = $remaining > self::CHUNK_SIZE ? self::CHUNK_SIZE : $remaining;
            $chunk = \fread($h, $toRead);
            $len = \strlen($chunk);
            if ($len === 0) break;
            $remaining -= $len;

            $lastNl = \strrpos($chunk, "\n");
            if ($lastNl === false) break;

            $tail = $len - $lastNl - 1;
            if ($tail > 0) {
                \fseek($h, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $p = 25;
            $fence = $lastNl - 700;

            while ($p < $fence) {
                $c = \strpos($chunk, ',', $p);
                $buckets[$pathIds[\substr($chunk, $p, $c - $p)]] .= $dateChars[\substr($chunk, $c + 3, 8)];
                $p = $c + 52;

                $c = \strpos($chunk, ',', $p);
                $buckets[$pathIds[\substr($chunk, $p, $c - $p)]] .= $dateChars[\substr($chunk, $c + 3, 8)];
                $p = $c + 52;

                $c = \strpos($chunk, ',', $p);
                $buckets[$pathIds[\substr($chunk, $p, $c - $p)]] .= $dateChars[\substr($chunk, $c + 3, 8)];
                $p = $c + 52;

                $c = \strpos($chunk, ',', $p);
                $buckets[$pathIds[\substr($chunk, $p, $c - $p)]] .= $dateChars[\substr($chunk, $c + 3, 8)];
                $p = $c + 52;

                $c = \strpos($chunk, ',', $p);
                $buckets[$pathIds[\substr($chunk, $p, $c - $p)]] .= $dateChars[\substr($chunk, $c + 3, 8)];
                $p = $c + 52;

                $c = \strpos($chunk, ',', $p);
                $buckets[$pathIds[\substr($chunk, $p, $c - $p)]] .= $dateChars[\substr($chunk, $c + 3, 8)];
                $p = $c + 52;
            }

            while ($p < $lastNl) {
                $c = \strpos($chunk, ',', $p);
                if ($c === false || $c >= $lastNl) break;
                $buckets[$pathIds[\substr($chunk, $p, $c - $p)]] .= $dateChars[\substr($chunk, $c + 3, 8)];
                $p = $c + 52;
            }
        }

        \fclose($h);
        return $buckets;
    }

    private function bucketsToFlat(array $buckets, int $pathCount, int $dateCount, int $totalCells): array
    {
        $flat = \array_fill(0, $totalCells, 0);
        for ($p = 0; $p < $pathCount; $p++) {
            if ($buckets[$p] !== '') {
                $base = $p * $dateCount;
                foreach (\array_count_values(\unpack('v*', $buckets[$p])) as $did => $cnt) {
                    $flat[$base + $did] = $cnt;
                }
            }
        }
        return $flat;
    }

    private function writeJson(
        string $outputPath, array $counts, array $paths,
        array $dates, int $pathCount, int $dateCount,
    ): void {
        $out = \fopen($outputPath, 'wb');
        \stream_set_write_buffer($out, 1_048_576);
        \fwrite($out, '{');

        $datePrefixes = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "20' . $dates[$d] . '": ';
        }

        $first = true;
        for ($p = 0; $p < $pathCount; $p++) {
            $base = $p * $dateCount;
            $entries = [];
            for ($d = 0; $d < $dateCount; $d++) {
                $v = $counts[$base + $d];
                if ($v === 0) continue;
                $entries[] = $datePrefixes[$d] . $v;
            }
            if ($entries === []) continue;

            \fwrite($out, ($first ? "\n    " : ",\n    ")
                . "\"\\/blog\\/" . \str_replace('/', '\\/', $paths[$p])
                . "\": {\n" . \implode(",\n", $entries) . "\n    }");
            $first = false;
        }

        \fwrite($out, "\n}");
        \fclose($out);
    }
}