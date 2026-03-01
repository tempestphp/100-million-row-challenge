<?php

namespace App;

use App\Commands\Visit;

use function array_fill;
use function fclose;
use function file_put_contents;
use function filesize;
use function fopen;
use function fread;
use function fseek;
use function gc_disable;
use function implode;
use function min;
use function str_replace;
use function stream_set_read_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;

use const SEEK_CUR;

final class Parser
{
    private const int READ_BUF = 262_144;
    private const int DISCOVER_SIZE = 2_097_152;

    public function parse(string $inputPath, string $outputPath): void
    {
        self::run($inputPath, $outputPath);
    }

    public static function run(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $fileSize = filesize($inputPath);

        // ── Phase 1: Build slug + date mappings ──

        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);
        $sample = fread($fh, min(self::DISCOVER_SIZE, $fileSize));

        $slugBase = [];
        $slugOrder = [];
        $slugCount = 0;

        $pos = 0;
        $sampleLastNl = strrpos($sample, "\n");
        if ($sampleLastNl === false) $sampleLastNl = 0;

        while ($pos < $sampleLastNl) {
            $nl = strpos($sample, "\n", $pos + 52);
            if ($nl === false) break;
            $slug = substr($sample, $pos + 25, $nl - $pos - 51);
            if (!isset($slugBase[$slug])) {
                $slugBase[$slug] = $slugCount; // temporary: store id, multiply later
                $slugOrder[$slugCount] = $slug;
                $slugCount++;
            }
            $pos = $nl + 1;
        }
        unset($sample);

        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, 25);
            if (!isset($slugBase[$slug])) {
                $slugBase[$slug] = $slugCount;
                $slugOrder[$slugCount] = $slug;
                $slugCount++;
            }
        }

        // Build date mapping: "YY-MM-DD" (8 chars) → sequential int
        $dateToId = [];
        $dateStr = [];
        $dateCount = 0;
        for ($y = 21; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => $y === 24 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $ms = $m < 10 ? "0{$m}" : (string)$m;
                for ($d = 1; $d <= $maxD; $d++) {
                    $ds = $d < 10 ? "0{$d}" : (string)$d;
                    $key = "{$y}-{$ms}-{$ds}";
                    $dateToId[$key] = $dateCount;
                    $dateStr[$dateCount] = "20{$y}-{$ms}-{$ds}";
                    $dateCount++;
                }
            }
        }

        // Pre-multiply slug bases: slugBase[slug] = slugId * dateCount
        foreach ($slugBase as $slug => $id) {
            $slugBase[$slug] = $id * $dateCount;
        }

        $totalEntries = $slugCount * $dateCount;

        // Pre-build JSON fragments
        $jsonDatePrefix = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $jsonDatePrefix[$d] = '        "' . $dateStr[$d] . '": ';
        }
        $jsonSlugHeader = [];
        for ($s = 0; $s < $slugCount; $s++) {
            $jsonSlugHeader[$s] = '"\\/blog\\/' . str_replace('/', '\\/', $slugOrder[$s]) . '"';
        }

        // ── Phase 2: Parse entire file single-threaded ──

        $counts = array_fill(0, $totalEntries, 0);
        fseek($fh, 0);
        $remaining = $fileSize;
        $bufSize = self::READ_BUF;

        while ($remaining > 0) {
            $toRead = $remaining > $bufSize ? $bufSize : $remaining;
            $chunk = fread($fh, $toRead);
            $cLen = strlen($chunk);
            if ($cLen === 0) break;
            $remaining -= $cLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) break;

            $tail = $cLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($fh, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $p = 25;
            $fence = $lastNl - 600;

            // ── 5x unrolled fast path ──
            if ($p < $fence) {
                do {
                    $sep = strpos($chunk, ',', $p);
                    $counts[$slugBase[substr($chunk, $p, $sep - $p)] + $dateToId[substr($chunk, $sep + 3, 8)]]++;
                    $p = $sep + 52;

                    $sep = strpos($chunk, ',', $p);
                    $counts[$slugBase[substr($chunk, $p, $sep - $p)] + $dateToId[substr($chunk, $sep + 3, 8)]]++;
                    $p = $sep + 52;

                    $sep = strpos($chunk, ',', $p);
                    $counts[$slugBase[substr($chunk, $p, $sep - $p)] + $dateToId[substr($chunk, $sep + 3, 8)]]++;
                    $p = $sep + 52;

                    $sep = strpos($chunk, ',', $p);
                    $counts[$slugBase[substr($chunk, $p, $sep - $p)] + $dateToId[substr($chunk, $sep + 3, 8)]]++;
                    $p = $sep + 52;

                    $sep = strpos($chunk, ',', $p);
                    $counts[$slugBase[substr($chunk, $p, $sep - $p)] + $dateToId[substr($chunk, $sep + 3, 8)]]++;
                    $p = $sep + 52;
                } while ($p < $fence);
            }

            // ── Safe tail loop ──
            while ($p < $lastNl) {
                $sep = strpos($chunk, ',', $p);
                if ($sep === false || $sep >= $lastNl) break;
                $counts[$slugBase[substr($chunk, $p, $sep - $p)] + $dateToId[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;
            }
        }

        fclose($fh);

        // ── Phase 3: Write JSON output ──

        $out = '{';
        $first = true;

        for ($s = 0; $s < $slugCount; $s++) {
            $base = $s * $dateCount;
            $dateEntries = [];

            for ($d = 0; $d < $dateCount; $d++) {
                $n = $counts[$base + $d];
                if ($n === 0) continue;
                $dateEntries[] = $jsonDatePrefix[$d] . $n;
            }

            if ($dateEntries === []) continue;

            $out .= ($first ? '' : ',') . "\n    " . $jsonSlugHeader[$s] . ": {\n"
                . implode(",\n", $dateEntries)
                . "\n    }";
            $first = false;
        }

        $out .= "\n}";
        file_put_contents($outputPath, $out);
    }
}
