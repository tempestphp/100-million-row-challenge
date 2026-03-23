<?php

namespace App;

final class Parser
{
    private const int WORKERS = 8;
    private const int CHUNK_SIZE = 8 * 1024 * 1024;
    private const int PREFIX_LEN = 25;

    public function parse(string $inputPath, string $outputPath): void
    {
        \gc_disable();

        $fileSize = \filesize($inputPath);
        $handle = \fopen($inputPath, 'rb');

        $sample = \fread($handle, 2 * 1024 * 1024);
        $pathIds = [];
        $paths = [];
        $pathCount = 0;

        $pos = 0;
        $lastNl = \strrpos($sample, "\n");
        if ($lastNl !== false) {
            while ($pos < $lastNl) {
                $nlPos = \strpos($sample, "\n", $pos + 52);
                if (!$nlPos || $nlPos > $lastNl) {
                    break;
                }
                $slug = \substr($sample, $pos + self::PREFIX_LEN, $nlPos - $pos - 51);
                if (!isset($pathIds[$slug])) {
                    $pathIds[$slug] = $pathCount++;
                    $paths[] = $slug;
                }

                $pos = $nlPos + 1;
            }
        }

        $dateIds = [];
        $dateStrings = [];
        $dIdx = 0;
        for ($y = 2020; $y <= 2026; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $mS = \sprintf('%02d', $m);
                for ($d = 1; $d <= 31; $d++) {
                    $dS = \sprintf('%02d', $d);
                    $dateStr = "$y-$mS-$dS";
                    $dateIds[$dateStr] = $dIdx;
                    $dateStrings[$dIdx] = $dateStr;
                    $dIdx++;
                }
            }
        }
        $totalDates = $dIdx;

        $boundaries = [0];
        for ($i = 1; $i < self::WORKERS; $i++) {
            \fseek($handle, (int)($fileSize * $i / self::WORKERS));
            \fgets($handle);
            $boundaries[] = \ftell($handle);
        }
        $boundaries[] = $fileSize;
        \fclose($handle);

        $tmpDir = '/tmp';
        $pids = [];
        $files = [];

        for ($w = 0; $w < self::WORKERS; $w++) {
            $tmpFile = $tmpDir . '/' . 'chl_' . $w . '.bin';
            $pid = \pcntl_fork();

            if ($pid === 0) {
                $this->workerProcess($inputPath, $boundaries[$w], $boundaries[$w + 1], $pathIds, $dateIds, $pathCount, $totalDates, $tmpFile);
                exit(0);
            }
            $pids[] = $pid;
            $files[] = $tmpFile;
        }

        $totalCounts = array_fill(0, $pathCount * $totalDates, 0);
        foreach ($pids as $idx => $pid) {
            \pcntl_waitpid($pid, $status);
            $wData = unpack('V*', \file_get_contents($files[$idx]));
            \unlink($files[$idx]);
            foreach ($wData as $k => $v) {
                if ($v > 0) $totalCounts[$k - 1] += $v;
            }
        }

        $this->writeFinalJson($outputPath, $paths, $dateStrings, $totalCounts, $totalDates);
    }

    private function workerProcess($path, $start, $end, $pathIds, $dateIds, $pathCount, $totalDates, $tmpFile): void
    {
        $counts = \array_fill(0, $pathCount * $totalDates, 0);
        $h = \fopen($path, 'rb');
        \fseek($h, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $chunk = \fread($h, min($remaining, self::CHUNK_SIZE));
            $len = \strlen($chunk);
            if ($len === 0) break;
            $remaining -= $len;

            $lastNl = \strrpos($chunk, "\n");
            if ($lastNl === false) continue;

            $diff = $len - ($lastNl + 1);
            if ($diff > 0) {
                \fseek($h, -$diff, SEEK_CUR);
                $remaining += $diff;
            }

            $offset = 0;
            while (($nl = \strpos($chunk, "\n", $offset)) !== false && $nl <= $lastNl) {
                $line = \substr($chunk, $offset, $nl - $offset);
                $offset = $nl + 1;
                if ($line === '') continue;

                $comma = \strpos($line, ',');
                $slug = \substr($line, self::PREFIX_LEN, $comma - self::PREFIX_LEN);
                $date = \substr($line, $comma + 1, 10);

                if (isset($pathIds[$slug])) {
                    $counts[($pathIds[$slug] * $totalDates) + $dateIds[$date]]++;
                }
            }
        }
        file_put_contents($tmpFile, \pack('V*', ...$counts));
    }

    private function writeFinalJson($outPath, $paths, $dateStrings, $counts, $totalDates): void
    {
        $fp = \fopen($outPath, 'wb');
        \fwrite($fp, "{\n");
        $firstSlug = true;

        foreach ($paths as $pIdx => $slug) {
            $base = $pIdx * $totalDates;
            $slugData = "";
            $hasData = false;
            $firstDate = true;

            for ($d = 0; $d < $totalDates; $d++) {
                $val = $counts[$base + $d];
                if ($val > 0) {
                    if (!$firstDate) $slugData .= ",\n";
                    $slugData .= "        \"{$dateStrings[$d]}\": $val";
                    $hasData = true;
                    $firstDate = false;
                }
            }

            if ($hasData) {
                if (!$firstSlug) {
                    \fwrite($fp, ",\n");
                }
                $escapedSlug = \str_replace('/', '\/', $slug);
                \fwrite($fp, "    \"\/blog\/{$escapedSlug}\": {\n{$slugData}\n    }");
                $firstSlug = false;
            }
        }

        \fwrite($fp, "\n}");
        \fclose($fp);
    }
}