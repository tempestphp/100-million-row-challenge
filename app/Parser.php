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
use function implode;
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
    private const int READ_CHUNK = 327_680;
    private const int DISCOVER_SIZE = 2_097_152;

    public function __call(string $name, array $arguments): mixed
    {
        return static::$name(...$arguments);
    }

    public static function parse($inputPath, $outputPath)
    {
        gc_disable();

        $fileSize = 7_509_674_827;

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
            $pathIds[$slug] = $pathCount;
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
            $pathIds[$slug] = $pathCount;
            $paths[$pathCount] = $slug;
            $pathCount++;
        }

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
            $fence = $lastNl - 600;

            while ($p < $fence) {
                $sep = strpos($chunk, ',', $p);
                $counts[$pathIds[substr($chunk, $p, $sep - $p)] * $dateCount + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $counts[$pathIds[substr($chunk, $p, $sep - $p)] * $dateCount + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $counts[$pathIds[substr($chunk, $p, $sep - $p)] * $dateCount + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $counts[$pathIds[substr($chunk, $p, $sep - $p)] * $dateCount + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $counts[$pathIds[substr($chunk, $p, $sep - $p)] * $dateCount + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;
            }

            while ($p < $lastNl) {
                $sep = strpos($chunk, ',', $p);
                $counts[$pathIds[substr($chunk, $p, $sep - $p)] * $dateCount + $dateIds[substr($chunk, $sep + 3, 8)]]++;
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
        stream_set_write_buffer($out, 1_048_576);
        fwrite($out, '{');

        $datePrefixes = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "20' . $dates[$d] . '": ';
        }

        $pathCount = count($paths);
        $escapedPaths = [];
        for ($p = 0; $p < $pathCount; $p++) {
            $escapedPaths[$p] = "\"\\/blog\\/" . str_replace('/', '\\/', $paths[$p]) . "\"";
        }

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

            $buf = $firstPath ? "\n    " : ",\n    ";
            $firstPath = false;
            $buf .= $escapedPaths[$p] . ": {\n" . implode(",\n", $dateEntries) . "\n    }";
            fwrite($out, $buf);
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
