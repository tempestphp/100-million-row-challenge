<?php

namespace App;

ini_set('memory_limit', '8192M');

final class Parser
{
    private const READ_CHUNK = 262_144;
    private const PATH_SCAN_SIZE = 2_097_152;
    private const PREFIX_LEN = 25; // "https://stitcher.io/blog/"
    private const WRITE_BUF = 1_048_576;

    public function parse(string $inputPath, string $outputPath): void
    {
        \gc_disable();

        $fileSize = \filesize($inputPath);

        $dateIds = [];
        $dates = [];
        $dateCount = 0;
        for ($y = 21; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => $y === 24 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $mStr = ($m < 10 ? '0' : '') . $m;
                $ymStr = "$y-$mStr-";
                for ($d = 1; $d <= $maxD; $d++) {
                    $key = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $dateIds[$key] = $dateCount;
                    $dates[$dateCount] = $key;
                    $dateCount++;
                }
            }
        }

        $next = [];
        for ($i = 0; $i < 255; $i++) {
            $next[\chr($i)] = \chr($i + 1);
        }

        $handle = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($handle, 0);
        $raw = \fread($handle, \min(self::PATH_SCAN_SIZE, $fileSize));
        \fclose($handle);

        $pathIds = [];
        $paths = [];
        $pathCount = 0;
        $pos = 0;
        $lastNl = \strrpos($raw, "\n") ?: 0;

        while ($pos < $lastNl) {
            $nlPos = \strpos($raw, "\n", $pos + 52);
            if ($nlPos === false) break;

            $slug = \substr($raw, $pos + self::PREFIX_LEN, $nlPos - $pos - 51);
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug] = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }

            $pos = $nlPos + 1;
        }
        unset($raw);

        $pathBaseMap = [];
        foreach ($pathIds as $slug => $id) {
            $pathBaseMap[$slug] = $id * $dateCount;
        }
        $outputSize = $pathCount * $dateCount;

        $numProcs = \max(1, (int) \shell_exec('nproc') ?: 4);
        $ranges = $this->splitRanges($inputPath, $fileSize, $numProcs);

        $pidToTmp = [];
        for ($i = 0; $i < $numProcs; $i++) {
            $tmp = \tempnam(\sys_get_temp_dir(), 'mrc_');
            $pid = \pcntl_fork();
            if ($pid === -1) throw new \Exception('pcntl_fork failed');
            if ($pid === 0) {
                $output = $this->processRange(
                    $inputPath, $ranges[$i][0], $ranges[$i][1],
                    $pathBaseMap, $dateIds, $next, $outputSize
                );
                \file_put_contents($tmp, $output);
                exit(0);
            }
            $pidToTmp[$pid] = $tmp;
        }

        $counts  = \array_fill(0, $outputSize, 0);
        $pending = $numProcs;
        while ($pending > 0) {
            $finished = \pcntl_waitpid(-1, $status);
            if ($finished <= 0 || !isset($pidToTmp[$finished])) continue;
            $tmp  = $pidToTmp[$finished];
            $proc = \array_values(\unpack('C*', \file_get_contents($tmp)));
            for ($j = 0; $j < $outputSize; $j++) {
                $counts[$j] += $proc[$j];
            }
            \unlink($tmp);
            $pending--;
        }

        $this->jsonize($outputPath, $counts, $paths, $dates, $pathCount, $dateCount);
    }

    private function splitRanges(string $inputPath, int $fileSize, int $n): array
    {
        $ranges = [];
        $handle = \fopen($inputPath, 'rb');
        $prev = 0;
        for ($i = 1; $i < $n; $i++) {
            \fseek($handle, (int) ($fileSize * $i / $n));
            \fgets($handle);
            $cur = \ftell($handle);
            $ranges[] = [$prev, $cur];
            $prev     = $cur;
        }
        $ranges[] = [$prev, $fileSize];
        \fclose($handle);
        return $ranges;
    }

    private function processRange(string $inputPath, int $start, int $end, array $pathBaseMap, array $dateIds, array $next, int $outputSize): string {
        $output = \str_repeat("\0", $outputSize);
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

            $p = self::PREFIX_LEN;
            $fence = $lastNl - 800;

            while ($p < $fence) {
                $sep = \strpos($chunk, ',', $p);
                $idx = $pathBaseMap[\substr($chunk, $p, $sep - $p)] + $dateIds[\substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = \strpos($chunk, ',', $p);
                $idx = $pathBaseMap[\substr($chunk, $p, $sep - $p)] + $dateIds[\substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = \strpos($chunk, ',', $p);
                $idx = $pathBaseMap[\substr($chunk, $p, $sep - $p)] + $dateIds[\substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = \strpos($chunk, ',', $p);
                $idx = $pathBaseMap[\substr($chunk, $p, $sep - $p)] + $dateIds[\substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = \strpos($chunk, ',', $p);
                $idx = $pathBaseMap[\substr($chunk, $p, $sep - $p)] + $dateIds[\substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = \strpos($chunk, ',', $p);
                $idx = $pathBaseMap[\substr($chunk, $p, $sep - $p)] + $dateIds[\substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = \strpos($chunk, ',', $p);
                $idx = $pathBaseMap[\substr($chunk, $p, $sep - $p)] + $dateIds[\substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = \strpos($chunk, ',', $p);
                $idx = $pathBaseMap[\substr($chunk, $p, $sep - $p)] + $dateIds[\substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
            }

            while ($p < $lastNl) {
                $sep = \strpos($chunk, ',', $p);
                if ($sep === false || $sep >= $lastNl) break;
                $idx = $pathBaseMap[\substr($chunk, $p, $sep - $p)] + $dateIds[\substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
            }
        }

        \fclose($handle);
        return $output;
    }

    private function jsonize(string $filename, array $counts, array $paths, array $dates, int $pathCount, int $dateCount): void {
        $datePrefixes = \array_map(fn($d) => '        "20' . $d . '": ', $dates);
        $escapedPaths = \array_map(fn($p) => "\"\\/blog\\/" . \str_replace('/', '\\/', $p) . '"', $paths);

        $file = \fopen($filename, 'wb');
        \stream_set_write_buffer($file, self::WRITE_BUF);
        \fwrite($file, '{');

        $isFirst = true;
        for ($p = 0; $p < $pathCount; $p++) {
            $base = $p * $dateCount;
            $dateEntries = [];
            for ($d = 0; $d < $dateCount; $d++) {
                if ($c = $counts[$base + $d])
                    $dateEntries[] = $datePrefixes[$d] . $c;
            }
            if (!$dateEntries) continue;

            $buf = $isFirst ? "\n    " : ",\n    ";
            $isFirst = false;
            $buf .= $escapedPaths[$p] . ": {\n" . \implode(",\n", $dateEntries) . "\n    }";
            \fwrite($file, $buf);
        }

        \fwrite($file, "\n}");
        \fclose($file);
    }
}
