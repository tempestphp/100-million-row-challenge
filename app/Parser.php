<?php

namespace App;

use App\Commands\Visit;
use function array_fill;
use function count;
use function fclose;
use function fopen;
use function fread;
use function fseek;
use function fwrite;
use function gc_disable;
use function str_replace;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use const SEEK_CUR;

final class Parser
{
    private const int READ_CHUNK = 524_288;
    private const int DISCOVER_SIZE = 2_097_152;

    public function __call(string $name, array $arguments): mixed
    {
        return static::$name(...$arguments);
    }

    public static function parse($inputPath, $outputPath)
    {
        gc_disable();

        $fileSize = filesize($inputPath);

        // Pre-compute date ID lookup — years 21-26 only
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

        // Discover paths from first 2MB
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $raw = fread($handle, self::DISCOVER_SIZE);
        fclose($handle);

        $pathIds = [];
        $paths = [];
        $pathCount = 0;
        $pos = 0;
        $lastNl = strrpos($raw, "\n");

        while ($pos < $lastNl) {
            $nlPos = strpos($raw, "\n", $pos + 52);

            $slug = substr($raw, $pos + 25, $nlPos - $pos - 51);
            if (isset($pathIds[$slug])) {
                $pos = $nlPos + 1;
                continue;
            }
            // Pre-multiply by dateCount so hot loop uses addition instead of multiply
            $pathIds[$slug] = $pathCount * $dateCount;
            $paths[$pathCount] = $slug;
            $pathCount++;

            $pos = $nlPos + 1;
        }
        unset($raw);

        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, 25);
            if (isset($pathIds[$slug])) {
                continue;
            }
            $pathIds[$slug] = $pathCount * $dateCount;
            $paths[$pathCount] = $slug;
            $pathCount++;
        }

        // Main parsing loop — direct increment with pre-multiplied path offsets
        $counts = array_fill(0, $pathCount * $dateCount, 0);
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $remaining = $fileSize;

        while ($remaining > 0) {
            $toRead = $remaining > self::READ_CHUNK ? self::READ_CHUNK : $remaining;
            $chunk = fread($handle, $toRead);
            $chunkLen = strlen($chunk);
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) break;

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $p = 25;
            $fence = $lastNl - 520;

            // Unrolled 5x — addition only, no multiplication per row
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

                $sep = strpos($chunk, ',', $p);
                $counts[$pathIds[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;
            }

            while ($p < $lastNl) {
                $sep = strpos($chunk, ',', $p);
                $counts[$pathIds[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;
            }
        }

        fclose($handle);

        self::writeJson($outputPath, $counts, $paths, $dates, $dateCount);
    }

    private static function writeJson(
        $outputPath, $counts, $paths,
        $dates, $dateCount,
    ) {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 4_194_304);

        $datePrefixes = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "20' . $dates[$d] . '": ';
        }

        $pathCount = count($paths);
        $escapedPaths = [];
        for ($p = 0; $p < $pathCount; $p++) {
            $escapedPaths[$p] = "\"\\/blog\\/" . str_replace('/', '\\/', $paths[$p]) . "\"";
        }

        // Build JSON with string concat — avoids array_push + implode overhead
        fwrite($out, '{');
        $firstPath = true;

        for ($p = 0; $p < $pathCount; $p++) {
            $base = $p * $dateCount;
            $body = '';
            $separator = '';

            for ($d = 0; $d < $dateCount; $d++) {
                $count = $counts[$base + $d];
                if ($count === 0) continue;
                $body .= $separator . $datePrefixes[$d] . $count;
                $separator = ",\n";
            }

            if ($body === '') continue;

            $entry = $firstPath ? "\n    " : ",\n    ";
            $firstPath = false;
            $entry .= $escapedPaths[$p] . ": {\n" . $body . "\n    }";
            fwrite($out, $entry);
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
