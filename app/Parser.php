<?php

namespace App;

final class Parser
{
    private const int DISCOVER_SIZE = 16_777_216;
    private const int WORKERS = 4;

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $fileSize = filesize($inputPath);

        $dates = self::generateDateCatalog();
        $dateCount = count($dates);

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $warmUpSize = $fileSize > self::DISCOVER_SIZE ? self::DISCOVER_SIZE : $fileSize;
        $raw = fread($handle, $warmUpSize);
        fclose($handle);

        $pathIndex = [];
        $paths = [];
        $pathCount = 0;
        $pos = 0;
        $lastNl = strrpos($raw, "\n");

        while ($pos < $lastNl) {
            $nlPos = strpos($raw, "\n", $pos + 52);
            if ($nlPos === false || $nlPos > $lastNl) break;

            $slug = substr($raw, $pos + 25, $nlPos - $pos - 51);
            if (!isset($pathIndex[$slug])) {
                $pathIndex[$slug] = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }

            $pos = $nlPos + 1;
        }
        unset($raw);

        foreach (\App\Commands\Visit::all() as $visit) {
            $slug = substr($visit->uri, 25);
            if (!isset($pathIndex[$slug])) {
                $pathIndex[$slug] = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }
        }

        $boundaries = [0];
        $bh = fopen($inputPath, 'rb');
        for ($i = 1; $i < self::WORKERS; $i++) {
            fseek($bh, (int)($fileSize * $i / self::WORKERS));
            fgets($bh);
            $boundaries[] = ftell($bh);
        }
        fclose($bh);
        $boundaries[] = $fileSize;

        $shmDir = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
        $myPid = getmypid();

        $children = [];
        for ($w = 0; $w < self::WORKERS - 1; $w++) {
            $tmpFile = $shmDir . '/p100m_' . $myPid . '_' . $w;
            $pid = pcntl_fork();
            if ($pid === 0) {
                $wCounts = self::parseRange(
                    $inputPath, $boundaries[$w], $boundaries[$w + 1],
                    $pathIndex, $dates, $pathCount, $dateCount
                );
                file_put_contents($tmpFile, pack('V*', ...$wCounts));
                exit(0);
            }
            $children[] = [$pid, $tmpFile];
        }

        $counts = self::parseRange(
            $inputPath, $boundaries[self::WORKERS - 1], $boundaries[self::WORKERS],
            $pathIndex, $dates, $pathCount, $dateCount
        );

        foreach ($children as [$cpid, $tmpFile]) {
            pcntl_waitpid($cpid, $status);
            $wCounts = unpack('V*', file_get_contents($tmpFile));
            unlink($tmpFile);
            $j = 0;
            foreach ($wCounts as $v) {
                $counts[$j++] += $v;
            }
        }

        self::writeJson($outputPath, $counts, $paths, $dateCount);
    }

    private static function generateDateCatalog(): array
    {
        $dates = [];
        $id = 0;
        for ($y = 2020; $y <= 2026; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => ($y % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $ym = $y . '-' . ($m < 10 ? '0' . $m : $m) . '-';
                for ($d = 1; $d <= $maxD; $d++) {
                    $dates[$ym . ($d < 10 ? '0' . $d : $d)] = $id++;
                }
            }
        }
        return $dates;
    }

    private static function parseRange(
        string $inputPath, int $start, int $end,
        array $pathIndex, array $dates,
        int $pathCount, int $dateCount
    ): array {
        $stride = $dateCount;
        $counts = array_fill(0, $pathCount * $stride, 0);
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $chunk = fread($handle, $remaining > 1_048_576 ? 1_048_576 : $remaining);
            $chunkLen = strlen($chunk);
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) {
                continue;
            }

            if ($lastNl < $chunkLen - 1) {
                $excess = $chunkLen - $lastNl - 1;
                fseek($handle, -$excess, SEEK_CUR);
                $remaining += $excess;
            }

            $pos = 0;
            while ($pos < $lastNl) {
                $nlPos = strpos($chunk, "\n", $pos + 52);
                if ($nlPos === false || $nlPos > $lastNl) break;

                $pathId = $pathIndex[substr($chunk, $pos + 25, $nlPos - $pos - 51)] ?? -1;
                if ($pathId >= 0) {
                    $counts[$pathId * $stride + $dates[substr($chunk, $nlPos - 25, 10)]]++;
                }

                $pos = $nlPos + 1;
            }
        }

        fclose($handle);
        return $counts;
    }

    private static function writeJson(
        string $outputPath, array $counts, array $paths,
        int $dateCount
    ): void {
        $daysInMonth = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        $sortedDates = [];
        for ($y = 2020; $y <= 2026; $y++) {
            $isLeap = ($y % 4 === 0 && ($y % 100 !== 0 || $y % 400 === 0));
            for ($m = 1; $m <= 12; $m++) {
                $maxD = $daysInMonth[$m];
                if ($m === 2 && $isLeap) $maxD = 29;
                $ym = $y . '-' . ($m < 10 ? '0' . $m : $m) . '-';
                for ($d = 1; $d <= $maxD; $d++) {
                    $sortedDates[] = $ym . ($d < 10 ? '0' . $d : $d);
                }
            }
        }

        $pathCount = count($paths);
        $fh = fopen($outputPath, 'wb');
        stream_set_write_buffer($fh, 1_048_576);
        $buf = "{\n";
        $firstPath = true;

        for ($p = 0; $p < $pathCount; $p++) {
            $base = $p * $dateCount;
            $hasAny = false;
            for ($d = 0; $d < $dateCount; $d++) {
                if ($counts[$base + $d] > 0) {
                    $hasAny = true;
                    break;
                }
            }
            if (!$hasAny) continue;

            if (!$firstPath) {
                $buf .= ",\n";
            }
            $firstPath = false;

            $buf .= '    "\\/blog\\/' . str_replace('/', '\\/', $paths[$p]) . '": {' . "\n";

            $firstDate = true;
            for ($d = 0; $d < $dateCount; $d++) {
                $val = $counts[$base + $d];
                if ($val > 0) {
                    if (!$firstDate) {
                        $buf .= ",\n";
                    }
                    $firstDate = false;
                    $buf .= '        "' . $sortedDates[$d] . '": ' . $val;
                }
            }

            $buf .= "\n    }";

            if (strlen($buf) > 1048576) {
                fwrite($fh, $buf);
                $buf = '';
            }
        }

        $buf .= "\n}";
        fwrite($fh, $buf);
        fclose($fh);
    }
}
