<?php

namespace App;

use App\Commands\Visit;
use const SEEK_CUR;

final class Parser
{
    private const int DISCOVER_SIZE = 16_777_216;
    private const int WORKERS = 5;
    private const int READ_CHUNK = 1_048_576;

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $fileSize = filesize($inputPath);

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

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $warmUpSize = $fileSize > self::DISCOVER_SIZE ? self::DISCOVER_SIZE : $fileSize;
        $raw = fread($handle, $warmUpSize);
        fclose($handle);

        $pathIds = [];
        $paths = [];
        $pathCount = 0;
        $pos = 0;
        $lastNl = strrpos($raw, "\n");

        while ($pos < $lastNl) {
            $nlPos = strpos($raw, "\n", $pos + 52);
            if ($nlPos === false) break;

            $slug = substr($raw, $pos + 25, $nlPos - $pos - 51);
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug] = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }

            $pos = $nlPos + 1;
        }
        unset($raw);

        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, 25);
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug] = $pathCount;
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

        $tmpDir = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
        $myPid = getmypid();

        $children = [];
        for ($w = 0; $w < self::WORKERS - 1; $w++) {
            $tmpFile = $tmpDir . '/p100m_' . $myPid . '_' . $w;
            $pid = pcntl_fork();
            if ($pid === 0) {
                $counts = $this->parseRange(
                    $inputPath, $boundaries[$w], $boundaries[$w + 1],
                    $pathIds, $dateIds, $pathCount, $dateCount
                );
                file_put_contents($tmpFile, pack('V*', ...array_merge(...$counts)));
                exit(0);
            }
            $children[] = [$pid, $tmpFile];
        }

        $counts = $this->parseRange(
            $inputPath, $boundaries[self::WORKERS - 1], $boundaries[self::WORKERS],
            $pathIds, $dateIds, $pathCount, $dateCount
        );

        $merged = array_merge(...$counts);
        unset($counts);

        foreach ($children as [$cpid, $tmpFile]) {
            pcntl_waitpid($cpid, $status);
            $wCounts = unpack('V*', file_get_contents($tmpFile));
            unlink($tmpFile);
            $j = 0;
            foreach ($wCounts as $v) {
                $merged[$j++] += $v;
            }
        }

        $this->writeJson($outputPath, $merged, $paths, $dates, $dateCount);
    }

    private function parseRange(
        string $inputPath, int $start, int $end,
        array $pathIds, array $dateIds,
        int $pathCount, int $dateCount
    ): array {
        $emptyRow = array_fill(0, $dateCount, 0);
        $counts = array_fill(0, $pathCount, $emptyRow);
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $chunk = fread($handle, $remaining > self::READ_CHUNK ? self::READ_CHUNK : $remaining);
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

                $counts[$pathIds[substr($chunk, $pos + 25, $nlPos - $pos - 51)]][$dateIds[substr($chunk, $nlPos - 23, 8)]]++;

                $pos = $nlPos + 1;
            }
        }

        fclose($handle);
        return $counts;
    }

    private function writeJson(
        string $outputPath, array $counts, array $paths,
        array $dates, int $dateCount
    ): void {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);
        fwrite($out, '{');

        $firstPath = true;
        $pathCount = count($paths);

        for ($p = 0; $p < $pathCount; $p++) {
            $base = $p * $dateCount;
            $firstDate = true;
            $dateBuf = '';

            for ($d = 0; $d < $dateCount; $d++) {
                $count = $counts[$base + $d];
                if ($count === 0) {
                    continue;
                }

                if (!$firstDate) {
                    $dateBuf .= ",\n";
                }
                $firstDate = false;
                $dateBuf .= '        "20' . $dates[$d] . '": ' . $count;
            }

            if ($firstDate) {
                continue;
            }

            $buf = $firstPath ? '' : ',';
            $firstPath = false;
            $buf .= "\n    \"\\/blog\\/" . str_replace('/', '\\/', $paths[$p]) . "\": {\n" . $dateBuf . "\n    }";
            fwrite($out, $buf);
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
