<?php

namespace App;

use App\Commands\Visit;

use function array_count_values;
use function array_fill;
use function chr;
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
use function unpack;

use const SEEK_CUR;

final class Parser
{
    private const int CHUNK_SIZE = 536_870_912;
    private const int DISCOVER_SIZE = 2_097_152;
    private const int WRITE_BUF = 1_048_576;

    public function __call(string $name, array $arguments): mixed
    {
        return static::$name(...$arguments);
    }

    public static function parse($inputPath, $outputPath)
    {
        gc_disable();

        $fileSize = filesize($inputPath);

        // Generate date mappings — years 20-26
        $dateIdChars = [];
        $dates = [];
        $dateCount = 0;
        for ($y = 20; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => ($y % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $mStr = ($m < 10 ? '0' : '') . $m;
                $ymStr = "{$y}-{$mStr}-";
                for ($d = 1; $d <= $maxD; $d++) {
                    $key = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $dateIdChars[$key] = chr($dateCount & 0xFF) . chr($dateCount >> 8);
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

        // Also include paths from Visit::all()
        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, 25);
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug] = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }
        }

        // Parse entire file in one pass with large chunks
        $buckets = array_fill(0, $pathCount, '');
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $remaining = $fileSize;

        while ($remaining > 0) {
            $toRead = $remaining > self::CHUNK_SIZE ? self::CHUNK_SIZE : $remaining;
            $chunk = fread($handle, $toRead);
            $chunkLen = strlen($chunk);
            if ($chunkLen === 0) break;
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) break;

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $p = 25;
            $fence = $lastNl - 480; // 8 lines * ~60 chars

            // Unrolled loop: 8 lines per iteration
            while ($p < $fence) {
                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;
            }

            // Remaining lines
            while ($p < $lastNl) {
                $sep = strpos($chunk, ',', $p);
                if ($sep === false || $sep >= $lastNl) break;
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;
            }
        }

        fclose($handle);

        // Batch count: unpack all 2-byte date IDs per path, then count_values
        $counts = array_fill(0, $pathCount * $dateCount, 0);
        for ($p = 0; $p < $pathCount; $p++) {
            if ($buckets[$p] === '') continue;
            $offset = $p * $dateCount;
            foreach (array_count_values(unpack('v*', $buckets[$p])) as $did => $count) {
                $counts[$offset + $did] = $count;
            }
        }
        unset($buckets);

        // Write JSON output
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, self::WRITE_BUF);

        $datePrefixes = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "20' . $dates[$d] . '": ';
        }

        $escapedPaths = [];
        for ($p = 0; $p < $pathCount; $p++) {
            $escapedPaths[$p] = "\"\\/blog\\/" . str_replace('/', '\\/', $paths[$p]) . "\"";
        }

        fwrite($out, '{');
        $first = true;

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

            fwrite($out, ($first ? '' : ',') . "\n    " . $escapedPaths[$p] . ": {\n" . $body . "\n    }");
            $first = false;
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
