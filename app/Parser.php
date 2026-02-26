<?php

declare(strict_types=1);

namespace App;

use App\Commands\Visit;

use const SEEK_CUR;

final class Parser
{
    private const int READ_CHUNK = 16_777_216;
    private const int DISCOVER_SIZE = 2_097_152;
    private const int WRITE_BUFFER = 1_048_576;
    private const int URL_PREFIX_LEN = 25;
    private const int WORKERS = 10;

    public function parse(string $inputPath, string $outputPath): void
    {
        \gc_disable();

        $fileSize = \filesize($inputPath);

        // Pre-generate all possible dates (2020-2026) with short keys (YY-MM-DD)
        $dateIds = [];
        $dates = [];
        $dateCount = 0;
        for ($y = 20; $y <= 26; $y++) {
            $yStr = ($y < 10 ? '0' : '') . $y;
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => (($y + 2000) % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $mStr = ($m < 10 ? '0' : '') . $m;
                $ymStr = $yStr . '-' . $mStr . '-';
                for ($d = 1; $d <= $maxD; $d++) {
                    $key = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $dateIds[$key] = $dateCount;
                    $dates[$dateCount] = $key;
                    $dateCount++;
                }
            }
        }

        // Discover paths from first chunk + Visit::all()
        [$pathIds, $paths, $pathCount] = $this->discoverPaths($inputPath, $fileSize, $dateCount);

        if (\function_exists('pcntl_fork') && $fileSize > self::DISCOVER_SIZE) {
            $this->parseParallel($inputPath, $outputPath, $fileSize, $pathIds, $paths, $pathCount, $dateIds, $dates, $dateCount);
        } else {
            $counts = $this->parseRange($inputPath, 0, $fileSize, $pathIds, $dateIds, $pathCount, $dateCount);
            $this->writeJson($outputPath, $paths, $pathCount, $dates, $dateCount, $counts);
        }
    }

    private function discoverPaths(string $inputPath, int $fileSize, int $dateCount): array
    {
        $pathIds = [];
        $paths = [];
        $pathCount = 0;

        $handle = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($handle, 0);
        $discoverSize = $fileSize > self::DISCOVER_SIZE ? self::DISCOVER_SIZE : $fileSize;
        $chunk = \fread($handle, $discoverSize);
        \fclose($handle);

        $lastNl = \strrpos($chunk, "\n");
        $slugStart = 25;
        while ($slugStart < $lastNl) {
            $commaPos = \strpos($chunk, ',', $slugStart);

            $slug = \substr($chunk, $slugStart, $commaPos - $slugStart);
            if (!isset($pathIds[$slug])) {
                // Store pathId * dateCount to avoid multiply in hot loop
                $pathIds[$slug] = $pathCount * $dateCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }
            $slugStart = $commaPos + 52;
        }
        unset($chunk);

        foreach (Visit::all() as $visit) {
            $slug = \substr($visit->uri, self::URL_PREFIX_LEN);
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug] = $pathCount * $dateCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }
        }

        return [$pathIds, $paths, $pathCount];
    }

    private function parseParallel(
        string $inputPath, string $outputPath, int $fileSize,
        array $pathIds, array $paths, int $pathCount,
        array $dateIds, array $dates, int $dateCount,
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
                $wCounts = $this->parseRange(
                    $inputPath, $boundaries[$w], $boundaries[$w + 1],
                    $pathIds, $dateIds, $pathCount, $dateCount,
                );
                \file_put_contents($tmpFile, \pack('V*', ...$wCounts));
                exit(0);
            }

            if ($pid < 0) {
                throw new \RuntimeException('Fork failed');
            }

            $children[] = [$pid, $tmpFile];
        }

        $counts = $this->parseRange(
            $inputPath, $boundaries[self::WORKERS - 1], $boundaries[self::WORKERS],
            $pathIds, $dateIds, $pathCount, $dateCount,
        );

        foreach ($children as [$cpid, $tmpFile]) {
            \pcntl_waitpid($cpid, $status);
            $wCounts = \unpack('V*', \file_get_contents($tmpFile));
            \unlink($tmpFile);
            $j = 0;
            foreach ($wCounts as $v) {
                $counts[$j++] += $v;
            }
        }

        $this->writeJson($outputPath, $paths, $pathCount, $dates, $dateCount, $counts);
    }

    private function parseRange(
        string $inputPath, int $start, int $end,
        array $pathIds, array $dateIds,
        int $pathCount, int $dateCount,
    ): array {
        $counts = \array_fill(0, $pathCount * $dateCount, 0);

        $handle = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($handle, 0);
        \fseek($handle, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $toRead = $remaining > self::READ_CHUNK ? self::READ_CHUNK : $remaining;
            $chunk = \fread($handle, $toRead);
            $chunkLen = \strlen($chunk);
            if ($chunkLen === 0) break;
            $remaining -= $chunkLen;

            $lastNl = \strrpos($chunk, "\n");
            if ($lastNl === false) {
                \fseek($handle, -$chunkLen, SEEK_CUR);
                $remaining += $chunkLen;
                break;
            }

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                \fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            // Hot loop — comma-based scanning with 4x unrolling
            $slugStart = 25;
            $fence = $lastNl - 400;

            while ($slugStart < $fence) {
                $commaPos = \strpos($chunk, ',', $slugStart);
                $counts[$pathIds[\substr($chunk, $slugStart, $commaPos - $slugStart)] + $dateIds[\substr($chunk, $commaPos + 3, 8)]]++;
                $slugStart = $commaPos + 52;

                $commaPos = \strpos($chunk, ',', $slugStart);
                $counts[$pathIds[\substr($chunk, $slugStart, $commaPos - $slugStart)] + $dateIds[\substr($chunk, $commaPos + 3, 8)]]++;
                $slugStart = $commaPos + 52;

                $commaPos = \strpos($chunk, ',', $slugStart);
                $counts[$pathIds[\substr($chunk, $slugStart, $commaPos - $slugStart)] + $dateIds[\substr($chunk, $commaPos + 3, 8)]]++;
                $slugStart = $commaPos + 52;

                $commaPos = \strpos($chunk, ',', $slugStart);
                $counts[$pathIds[\substr($chunk, $slugStart, $commaPos - $slugStart)] + $dateIds[\substr($chunk, $commaPos + 3, 8)]]++;
                $slugStart = $commaPos + 52;
            }

            while ($slugStart < $lastNl) {
                $commaPos = \strpos($chunk, ',', $slugStart);
                $counts[$pathIds[\substr($chunk, $slugStart, $commaPos - $slugStart)] + $dateIds[\substr($chunk, $commaPos + 3, 8)]]++;
                $slugStart = $commaPos + 52;
            }
        }

        \fclose($handle);
        return $counts;
    }

    private function writeJson(
        string $outputPath,
        array $paths,
        int $pathCount,
        array $dates,
        int $dateCount,
        array $counts,
    ): void {
        // Pre-compute date prefixes and escaped path headers
        $datePrefixes = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "20' . $dates[$d] . '": ';
        }

        $pathHeaders = [];
        for ($p = 0; $p < $pathCount; $p++) {
            $pathHeaders[$p] = "\n    \"\\/blog\\/" . \str_replace('/', '\\/', $paths[$p]) . "\": {\n";
        }

        $out = \fopen($outputPath, 'wb');
        \stream_set_write_buffer($out, self::WRITE_BUFFER);
        \fwrite($out, '{');

        $firstPath = true;

        for ($p = 0; $p < $pathCount; $p++) {
            $base = $p * $dateCount;
            $dateEntries = [];

            for ($d = 0; $d < $dateCount; $d++) {
                $count = $counts[$base + $d];
                if ($count === 0) continue;
                $dateEntries[] = $datePrefixes[$d] . $count;
            }

            if ($dateEntries === []) continue;

            \fwrite($out, ($firstPath ? '' : ',')
                . $pathHeaders[$p] . \implode(",\n", $dateEntries) . "\n    }");
            $firstPath = false;
        }

        \fwrite($out, "\n}");
        \fclose($out);
    }
}