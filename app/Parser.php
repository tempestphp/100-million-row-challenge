<?php

namespace App;

use App\Commands\Visit;
use parallel\Runtime;

use const SEEK_CUR;


final class Parser
{
    private const int WORKERS = 10;
    private const int READ_CHUNK = 655_360;
    private const int DISCOVER_SIZE = 2_097_152;

    public function parse($inputPath, $outputPath)
    {
        \gc_disable();

        $fileSize = \filesize($inputPath);

        $dIdx = [];
        $dKeys = [];
        $dCnt = 0;
        for ($y = 20; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => (($y + 2000) % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $mStr = ($m < 10 ? '0' : '') . $m;
                $ymStr = $y . '-' . $mStr . '-';
                for ($d = 1; $d <= $maxD; $d++) {
                    $key = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $dIdx[$key] = $dCnt;
                    $dKeys[$dCnt] = $key;
                    $dCnt++;
                }
            }
        }

        $dBin = [];
        foreach ($dIdx as $date => $id) {
            $dBin[$date] = \chr($id & 0xFF) . \chr($id >> 8);
        }

        $handle = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($handle, 0);
        $warmUpSize = $fileSize > self::DISCOVER_SIZE ? self::DISCOVER_SIZE : $fileSize;
        $raw = \fread($handle, $warmUpSize);
        \fclose($handle);

        $pIdx = [];
        $pKeys = [];
        $pCnt = 0;
        $pos = 0;
        $lastNl = \strrpos($raw, "\n");

        while ($pos < $lastNl) {
            $nlPos = \strpos($raw, "\n", $pos + 52);
            if ($nlPos === false) break;

            $slug = \substr($raw, $pos + 25, $nlPos - $pos - 51);
            if (!isset($pIdx[$slug])) {
                $pIdx[$slug] = $pCnt;
                $pKeys[$pCnt] = $slug;
                $pCnt++;
            }

            $pos = $nlPos + 1;
        }
        unset($raw);

        foreach (Visit::all() as $visit) {
            $slug = \substr($visit->uri, 25);
            if (!isset($pIdx[$slug])) {
                $pIdx[$slug] = $pCnt;
                $pKeys[$pCnt] = $slug;
                $pCnt++;
            }
        }

        $cuts = [0];
        $bh = \fopen($inputPath, 'rb');
        for ($i = 1; $i < self::WORKERS; $i++) {
            \fseek($bh, (int) ($fileSize * $i / self::WORKERS));
            \fgets($bh);
            $cuts[] = \ftell($bh);
        }
        \fclose($bh);
        $cuts[] = $fileSize;

        $jobs = [];
        $chunkSize = self::READ_CHUNK;

        for ($w = 0; $w < self::WORKERS - 1; $w++) {
            $runtime = new Runtime();
            $wStart = $cuts[$w];
            $wEnd = $cuts[$w + 1];
            $jobs[] = $runtime->run(static function (
                string $inputPath, int $start, int $end,
                array $pIdx, array $dBin,
                int $pCnt, int $dCnt, int $chunkSize,
            ): array {
                \gc_disable();
                $buckets = \array_fill(0, $pCnt, '');
                $handle = \fopen($inputPath, 'rb');
                \stream_set_read_buffer($handle, 0);
                \fseek($handle, $start);
                $remaining = $end - $start;

                while ($remaining > 0) {
                    $toRead = $remaining > $chunkSize ? $chunkSize : $remaining;
                    $chunk = \fread($handle, $toRead);
                    $chunkLen = \strlen($chunk);
                    if ($chunkLen === 0) break;
                    $remaining -= $chunkLen;

                    $lastNl = \strrpos($chunk, "\n");
                    if ($lastNl === false) break;

                    $tail = $chunkLen - $lastNl - 1;
                    if ($tail > 0) {
                        \fseek($handle, -$tail, SEEK_CUR);
                        $remaining += $tail;
                    }

                    $p = 25;
                    $fence = $lastNl - 720;

                    while ($p < $fence) {
                        $sep = \strpos($chunk, ',', $p);
                        $buckets[$pIdx[\substr($chunk, $p, $sep - $p)]] .= $dBin[\substr($chunk, $sep + 3, 8)];
                        $p = $sep + 52;

                        $sep = \strpos($chunk, ',', $p);
                        $buckets[$pIdx[\substr($chunk, $p, $sep - $p)]] .= $dBin[\substr($chunk, $sep + 3, 8)];
                        $p = $sep + 52;

                        $sep = \strpos($chunk, ',', $p);
                        $buckets[$pIdx[\substr($chunk, $p, $sep - $p)]] .= $dBin[\substr($chunk, $sep + 3, 8)];
                        $p = $sep + 52;

                        $sep = \strpos($chunk, ',', $p);
                        $buckets[$pIdx[\substr($chunk, $p, $sep - $p)]] .= $dBin[\substr($chunk, $sep + 3, 8)];
                        $p = $sep + 52;

                        $sep = \strpos($chunk, ',', $p);
                        $buckets[$pIdx[\substr($chunk, $p, $sep - $p)]] .= $dBin[\substr($chunk, $sep + 3, 8)];
                        $p = $sep + 52;

                        $sep = \strpos($chunk, ',', $p);
                        $buckets[$pIdx[\substr($chunk, $p, $sep - $p)]] .= $dBin[\substr($chunk, $sep + 3, 8)];
                        $p = $sep + 52;
                    }

                    while ($p < $lastNl) {
                        $sep = \strpos($chunk, ',', $p);
                        if ($sep === false || $sep >= $lastNl) break;
                        $buckets[$pIdx[\substr($chunk, $p, $sep - $p)]] .= $dBin[\substr($chunk, $sep + 3, 8)];
                        $p = $sep + 52;
                    }
                }

                \fclose($handle);

                $totals = \array_fill(0, $pCnt * $dCnt, 0);
                for ($p = 0; $p < $pCnt; $p++) {
                    if ($buckets[$p] === '') continue;
                    $offset = $p * $dCnt;
                    foreach (\array_count_values(\unpack('v*', $buckets[$p])) as $did => $count) {
                        $totals[$offset + $did] += $count;
                    }
                }

                return $totals;
            }, [$inputPath, $wStart, $wEnd, $pIdx, $dBin, $pCnt, $dCnt, $chunkSize]);
        }

        $totals = $this->parseRange(
            $inputPath, $cuts[self::WORKERS - 1], $cuts[self::WORKERS],
            $pIdx, $dBin, $pCnt, $dCnt,
        );

        foreach ($jobs as $future) {
            $wCounts = $future->value();
            $j = 0;
            foreach ($wCounts as $v) {
                $totals[$j++] += $v;
            }
        }

        $this->writeJson($outputPath, $totals, $pKeys, $dKeys, $dCnt);
    }

    private function parseRange(
        $inputPath, $start, $end,
        $pIdx, $dBin,
        $pCnt, $dCnt,
    ) {
        $buckets = \array_fill(0, $pCnt, '');
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
            if ($lastNl === false) break;

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                \fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $p = 25;
            $fence = $lastNl - 720;

            while ($p < $fence) {
                $sep = \strpos($chunk, ',', $p);
                $buckets[$pIdx[\substr($chunk, $p, $sep - $p)]] .= $dBin[\substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = \strpos($chunk, ',', $p);
                $buckets[$pIdx[\substr($chunk, $p, $sep - $p)]] .= $dBin[\substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = \strpos($chunk, ',', $p);
                $buckets[$pIdx[\substr($chunk, $p, $sep - $p)]] .= $dBin[\substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = \strpos($chunk, ',', $p);
                $buckets[$pIdx[\substr($chunk, $p, $sep - $p)]] .= $dBin[\substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = \strpos($chunk, ',', $p);
                $buckets[$pIdx[\substr($chunk, $p, $sep - $p)]] .= $dBin[\substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = \strpos($chunk, ',', $p);
                $buckets[$pIdx[\substr($chunk, $p, $sep - $p)]] .= $dBin[\substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;
            }

            while ($p < $lastNl) {
                $sep = \strpos($chunk, ',', $p);
                if ($sep === false || $sep >= $lastNl) break;
                $buckets[$pIdx[\substr($chunk, $p, $sep - $p)]] .= $dBin[\substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;
            }
        }

        \fclose($handle);

        $totals = \array_fill(0, $pCnt * $dCnt, 0);
        for ($p = 0; $p < $pCnt; $p++) {
            if ($buckets[$p] === '') continue;
            $offset = $p * $dCnt;
            foreach (\array_count_values(\unpack('v*', $buckets[$p])) as $did => $count) {
                $totals[$offset + $did] += $count;
            }
        }

        return $totals;
    }

    private function writeJson(
        $outputPath, $totals, $pKeys,
        $dKeys, $dCnt,
    ) {
        $out = \fopen($outputPath, 'wb');
        \stream_set_write_buffer($out, 1_048_576);
        \fwrite($out, '{');

        $datePrefixes = [];
        for ($d = 0; $d < $dCnt; $d++) {
            $datePrefixes[$d] = '        "20' . $dKeys[$d] . '": ';
        }

        $pCnt = \count($pKeys);
        $escapedPaths = [];
        for ($p = 0; $p < $pCnt; $p++) {
            $escapedPaths[$p] = "\"\\/blog\\/" . \str_replace('/', '\\/', $pKeys[$p]) . "\"";
        }

        $firstPath = true;

        for ($p = 0; $p < $pCnt; $p++) {
            $base = $p * $dCnt;
            $dateEntries = [];

            for ($d = 0; $d < $dCnt; $d++) {
                $count = $totals[$base + $d];
                if ($count === 0) continue;
                $dateEntries[] = $datePrefixes[$d] . $count;
            }

            if ($dateEntries === []) continue;

            $buf = $firstPath ? "\n    " : ",\n    ";
            $firstPath = false;
            $buf .= $escapedPaths[$p] . ": {\n" . \implode(",\n", $dateEntries) . "\n    }";
            \fwrite($out, $buf);
        }

        \fwrite($out, "\n}");
        \fclose($out);
    }
}