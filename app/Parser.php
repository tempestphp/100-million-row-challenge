<?php

namespace App;

ini_set('memory_limit', '8192M');

final class Parser
{
    private const READ_CHUNK = 163_840;
    private const PATH_SCAN_SIZE = 2_097_152;
    private const PREFIX_LEN = 25; // "https://stitcher.io/blog/"
    private const WRITE_BUF  = 1_048_576;

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
                $ymStr = "{$y}-{$mStr}-";
                for ($d = 1; $d <= $maxD; $d++) {
                    $key = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $dateIds[$key] = $dateCount;
                    $dates[$dateCount] = $key;
                    $dateCount++;
                }
            }
        }

        $dateIdBytes = [];
        foreach ($dateIds as $date => $id) {
            $dateIdBytes[$date] = \chr($id & 0xFF) . \chr($id >> 8);
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

        $buckets = \array_fill(0, $pathCount, '');

        $handle = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($handle, 0);
        $remaining = $fileSize;

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
                $buckets[$pathIds[\substr($chunk, $p, $sep - $p)]] .= $dateIdBytes[\substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = \strpos($chunk, ',', $p);
                $buckets[$pathIds[\substr($chunk, $p, $sep - $p)]] .= $dateIdBytes[\substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = \strpos($chunk, ',', $p);
                $buckets[$pathIds[\substr($chunk, $p, $sep - $p)]] .= $dateIdBytes[\substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = \strpos($chunk, ',', $p);
                $buckets[$pathIds[\substr($chunk, $p, $sep - $p)]] .= $dateIdBytes[\substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = \strpos($chunk, ',', $p);
                $buckets[$pathIds[\substr($chunk, $p, $sep - $p)]] .= $dateIdBytes[\substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = \strpos($chunk, ',', $p);
                $buckets[$pathIds[\substr($chunk, $p, $sep - $p)]] .= $dateIdBytes[\substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = \strpos($chunk, ',', $p);
                $buckets[$pathIds[\substr($chunk, $p, $sep - $p)]] .= $dateIdBytes[\substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = \strpos($chunk, ',', $p);
                $buckets[$pathIds[\substr($chunk, $p, $sep - $p)]] .= $dateIdBytes[\substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;
            }

            while ($p < $lastNl) {
                $sep = \strpos($chunk, ',', $p);
                if ($sep === false || $sep >= $lastNl) break;
                $slug = \substr($chunk, $p, $sep - $p);
                if (!isset($pathIds[$slug])) {
                    $pathIds[$slug] = $pathCount;
                    $paths[$pathCount] = $slug;
                    $buckets[$pathCount] = '';
                    $pathCount++;
                }
                $buckets[$pathIds[$slug]] .= $dateIdBytes[\substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;
            }
        }

        \fclose($handle);

        $this->jsonize($outputPath, $buckets, $paths, $dates, $dateCount);
    }

    private function jsonize(string $filename, array $buckets, array $paths, array $dates, int $dateCount): void {
        $datePrefixes = \array_map(fn($d) => '        "20' . $d . '": ', $dates);
        $escapedPaths = \array_map(fn($p) => "\"\\/blog\\/" . \str_replace('/', '\\/', $p) . '"', $paths);

        $file = \fopen($filename, 'wb');
        \stream_set_write_buffer($file, self::WRITE_BUF);
        \fwrite($file, '{');

        $isFirst = true;
        foreach ($paths as $pid => $path) {
            if ($buckets[$pid] === '') continue;

            $hits = \array_count_values(\unpack('v*', $buckets[$pid]));
            $dateEntries = [];
            for ($d = 0; $d < $dateCount; $d++) {
                if (isset($hits[$d]))
                    $dateEntries[] = $datePrefixes[$d] . $hits[$d];
            }
            if (!$dateEntries) continue;

            $buf = $isFirst ? "\n    " : ",\n    ";
            $isFirst = false;
            $buf .= $escapedPaths[$pid] . ": {\n" . \implode(",\n", $dateEntries) . "\n    }";
            \fwrite($file, $buf);
        }

        \fwrite($file, "\n}");
        \fclose($file);
    }
}
