<?php

namespace App;

use App\Commands\Visit;
use const SEEK_CUR;

final class Parser
{
    private const int DISCOVER_SIZE = 1_048_576;
    private const int WORKERS = 10;
    private const int READ_CHUNK = 4_194_304;

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
        $lastNl = strrpos($raw, "\n");

        $p = 25;
        while ($p < $lastNl) {
            $sep = strpos($raw, ',', $p);
            if ($sep === false || $sep >= $lastNl) break;

            $slug = substr($raw, $p, $sep - $p);
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug] = $pathCount * $dateCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }

            $p = $sep + 52;
        }
        unset($raw);

        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, 25);
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug] = $pathCount * $dateCount;
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
                $wCounts = $this->parseRange(
                    $inputPath, $boundaries[$w], $boundaries[$w + 1],
                    $pathIds, $dateIds, $pathCount, $dateCount
                );
                file_put_contents($tmpFile, pack('V*', ...$wCounts));
                exit(0);
            }
            $children[] = [$pid, $tmpFile];
        }

        $counts = $this->parseRange(
            $inputPath, $boundaries[self::WORKERS - 1], $boundaries[self::WORKERS],
            $pathIds, $dateIds, $pathCount, $dateCount
        );

        $pending = $children;
        while (!empty($pending)) {
            $anyDone = false;
            foreach ($pending as $key => [$cpid, $tmpFile]) {
                $ret = pcntl_waitpid($cpid, $status, WNOHANG);
                if ($ret > 0) {
                    $wCounts = unpack('V*', file_get_contents($tmpFile));
                    unlink($tmpFile);
                    $j = 0;
                    foreach ($wCounts as $v) {
                        $counts[$j++] += $v;
                    }
                    unset($pending[$key]);
                    $anyDone = true;
                    break;
                }
            }
            if (!$anyDone && !empty($pending)) {
                reset($pending);
                $key = key($pending);
                [$cpid, $tmpFile] = $pending[$key];
                pcntl_waitpid($cpid, $status);
                $wCounts = unpack('V*', file_get_contents($tmpFile));
                unlink($tmpFile);
                $j = 0;
                foreach ($wCounts as $v) {
                    $counts[$j++] += $v;
                }
                unset($pending[$key]);
            }
        }

        $this->writeJson($outputPath, $counts, $paths, $dates, $dateCount);
    }

    private function parseRange(
        string $inputPath, int $start, int $end,
        array $pathIds, array $dateIds,
        int $pathCount, int $dateCount
    ): array {
        $counts = array_fill(0, $pathCount * $dateCount, 0);
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $toRead = $remaining > self::READ_CHUNK ? self::READ_CHUNK : $remaining;
            $chunk = fread($handle, $toRead);
            $chunkLen = strlen($chunk);
            if ($chunkLen === 0) break;
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) {
                fseek($handle, -$chunkLen, SEEK_CUR);
                $remaining += $chunkLen;
                break;
            }

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $p = 25;
            $fence = $lastNl - 400;

            while ($p < $fence) {
                $sep = strpos($chunk, ',', $p);
                $counts[$pathIds[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $counts[$pathIds[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $counts[$pathIds[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $counts[$pathIds[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;
            }

            while ($p < $lastNl) {
                $sep = strpos($chunk, ',', $p);
                if ($sep === false || $sep >= $lastNl) break;
                $counts[$pathIds[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;
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
