<?php

namespace App;

use function array_fill;
use function chr;
use function count;
use function fclose;
use function fgets;
use function file_get_contents;
use function file_put_contents;
use function filesize;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function getmypid;
use function implode;
use function pcntl_fork;
use function pcntl_waitpid;
use function str_repeat;
use function str_replace;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function unlink;
use function unpack;

use const SEEK_CUR;

final class Parser
{
    private const int BUFFER_SIZE = 1_048_576; // 1MB buffer
    private const int PREFIX_LEN = 25;
    private const int DATE_COUNT = 2191;

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $fileSize = filesize($inputPath);
        $pid = getmypid();

        // Pre-compute date lookup - use direct offset calculation
        $dateIds = [];
        $dates = [];
        $idx = 0;
        for ($y = 21; $y <= 26; $y++) {
            $yStr = (string)$y;
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => ($y === 24) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $mStr = $m < 10 ? "0{$m}" : (string)$m;
                $prefix = "{$yStr}-{$mStr}-";
                for ($d = 1; $d <= $maxD; $d++) {
                    $dStr = $d < 10 ? "0{$d}" : (string)$d;
                    $dateIds[$prefix . $dStr] = $idx;
                    $dates[$idx++] = $prefix . $dStr;
                }
            }
        }

        // Increment lookup
        $inc = [];
        for ($i = 0; $i < 255; $i++) {
            $inc[chr($i)] = chr($i + 1);
        }

        // Build slug map
        $slugIds = [];
        $slugs = [];
        $slugCount = 0;
        $knownSlugCount = count(Commands\Visit::all());

        $bh = fopen($inputPath, 'rb');
        $leftover = '';
        $done = false;
        while (!$done) {
            $data = fread($bh, 131072); // Larger initial scan buffer
            if ($data === false || $data === '') break;
            $chunk = $leftover . $data;
            $leftover = '';
            $pos = 0;
            $chunkLen = strlen($chunk);
            while ($pos < $chunkLen) {
                $nl = strpos($chunk, "\n", $pos);
                if ($nl === false) {
                    $leftover = substr($chunk, $pos);
                    break;
                }
                // Extract slug: from pos+25 to newline-26
                $slugEnd = $nl - 26;
                if ($slugEnd <= $pos + self::PREFIX_LEN) {
                    $pos = $nl + 1;
                    continue;
                }
                $slug = substr($chunk, $pos + self::PREFIX_LEN, $slugEnd - $pos - self::PREFIX_LEN);
                if (!isset($slugIds[$slug])) {
                    $slugIds[$slug] = $slugCount * self::DATE_COUNT;
                    $slugs[$slugCount++] = $slug;
                    if ($slugCount >= $knownSlugCount) {
                        $done = true;
                        break;
                    }
                }
                $pos = $nl + 1;
            }
        }
        fclose($bh);

        $cellCount = $slugCount * self::DATE_COUNT;

        // Adaptive workers
        $numWorkers = match (true) {
            $fileSize > 500_000_000 => 12,
            $fileSize > 100_000_000 => 10,
            $fileSize > 10_000_000 => 6,
            $fileSize > 1_000_000 => 4,
            default => 1,
        };

        // Compute boundaries
        $boundaries = [0];
        if ($numWorkers > 1) {
            $chunkSize = (int)($fileSize / $numWorkers);
            $bh = fopen($inputPath, 'rb');
            for ($w = 1; $w < $numWorkers; $w++) {
                $targetOffset = $w * $chunkSize;
                if ($targetOffset >= $fileSize) break;
                fseek($bh, $targetOffset);
                fgets($bh);
                $pos = ftell($bh);
                if ($pos < $fileSize) {
                    $boundaries[] = $pos;
                }
            }
            fclose($bh);
        }
        $boundaries[] = $fileSize;
        $numWorkers = count($boundaries) - 1;

        // Single worker
        if ($numWorkers === 1) {
            $output = self::processChunk($inputPath, 0, $fileSize, $slugIds, $dateIds, $inc, $cellCount);
            $counts = array_fill(0, $cellCount, 0);
            $j = 0;
            foreach (unpack('C*', $output) as $v) {
                $counts[$j++] = $v;
            }
            self::writeJson($outputPath, $counts, $slugs, $dates, $slugCount);
            return;
        }

        // Fork children for all but last chunk
        $tempFiles = [];
        $children = [];

        for ($w = 0; $w < $numWorkers - 1; $w++) {
            $tempFile = "/tmp/parser_{$pid}_{$w}.bin";
            $tempFiles[$w] = $tempFile;

            $cpid = pcntl_fork();
            if ($cpid === 0) {
                $output = self::processChunk(
                    $inputPath, $boundaries[$w], $boundaries[$w + 1],
                    $slugIds, $dateIds, $inc, $cellCount
                );
                file_put_contents($tempFile, $output);
                exit(0);
            }
            $children[] = $cpid;
        }

        // Parent processes LAST chunk in parallel with children!
        $parentOutput = self::processChunk(
            $inputPath, $boundaries[$numWorkers - 1], $boundaries[$numWorkers],
            $slugIds, $dateIds, $inc, $cellCount
        );

        // Wait for children
        foreach ($children as $cpid) {
            pcntl_waitpid($cpid, $status);
        }

        // Merge: start with parent's result
        $counts = array_fill(0, $cellCount, 0);
        $j = 0;
        foreach (unpack('C*', $parentOutput) as $v) {
            $counts[$j++] = $v;
        }
        unset($parentOutput);

        // Add children's results
        foreach ($tempFiles as $tempFile) {
            $data = file_get_contents($tempFile);
            if ($data !== false && $data !== '') {
                $j = 0;
                foreach (unpack('C*', $data) as $v) {
                    $counts[$j++] += $v;
                }
            }
            @unlink($tempFile);
        }

        self::writeJson($outputPath, $counts, $slugs, $dates, $slugCount);
    }

    private static function processChunk(
        string $inputPath, int $start, int $end,
        array $slugIds, array $dateIds, array $inc, int $cellCount
    ): string {
        $output = str_repeat("\0", $cellCount);
        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);
        fseek($fh, $start);

        $remaining = $end - $start;
        $bufSize = self::BUFFER_SIZE;

        while ($remaining > 0) {
            $toRead = $remaining > $bufSize ? $bufSize : $remaining;
            $chunk = fread($fh, $toRead);
            if (!$chunk) break;

            $len = strlen($chunk);
            $remaining -= $len;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) continue;

            $tail = $len - $lastNl - 1;
            if ($tail > 0) {
                fseek($fh, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            // Newline-based parsing: find \n, extract slug and date by offset
            // Line format: https://stitcher.io/blog/{slug},20YY-MM-DDTHH:MM:SS+00:00\n
            // Date (YY-MM-DD) is at position: newline - 23 (8 chars)
            // Comma is at: newline - 26
            // Slug starts at: line_start + 25
            
            $p = 0;
            $fence = $lastNl - 2600; // ~25 lines safety margin

            // 20x unrolled with newline-based parsing
            while ($p < $fence) {
                $nl = strpos($chunk, "\n", $p); $i = $slugIds[substr($chunk, $p + 25, $nl - $p - 51)] + $dateIds[substr($chunk, $nl - 23, 8)]; $output[$i] = $inc[$output[$i]]; $p = $nl + 1;
                $nl = strpos($chunk, "\n", $p); $i = $slugIds[substr($chunk, $p + 25, $nl - $p - 51)] + $dateIds[substr($chunk, $nl - 23, 8)]; $output[$i] = $inc[$output[$i]]; $p = $nl + 1;
                $nl = strpos($chunk, "\n", $p); $i = $slugIds[substr($chunk, $p + 25, $nl - $p - 51)] + $dateIds[substr($chunk, $nl - 23, 8)]; $output[$i] = $inc[$output[$i]]; $p = $nl + 1;
                $nl = strpos($chunk, "\n", $p); $i = $slugIds[substr($chunk, $p + 25, $nl - $p - 51)] + $dateIds[substr($chunk, $nl - 23, 8)]; $output[$i] = $inc[$output[$i]]; $p = $nl + 1;
                $nl = strpos($chunk, "\n", $p); $i = $slugIds[substr($chunk, $p + 25, $nl - $p - 51)] + $dateIds[substr($chunk, $nl - 23, 8)]; $output[$i] = $inc[$output[$i]]; $p = $nl + 1;
                $nl = strpos($chunk, "\n", $p); $i = $slugIds[substr($chunk, $p + 25, $nl - $p - 51)] + $dateIds[substr($chunk, $nl - 23, 8)]; $output[$i] = $inc[$output[$i]]; $p = $nl + 1;
                $nl = strpos($chunk, "\n", $p); $i = $slugIds[substr($chunk, $p + 25, $nl - $p - 51)] + $dateIds[substr($chunk, $nl - 23, 8)]; $output[$i] = $inc[$output[$i]]; $p = $nl + 1;
                $nl = strpos($chunk, "\n", $p); $i = $slugIds[substr($chunk, $p + 25, $nl - $p - 51)] + $dateIds[substr($chunk, $nl - 23, 8)]; $output[$i] = $inc[$output[$i]]; $p = $nl + 1;
                $nl = strpos($chunk, "\n", $p); $i = $slugIds[substr($chunk, $p + 25, $nl - $p - 51)] + $dateIds[substr($chunk, $nl - 23, 8)]; $output[$i] = $inc[$output[$i]]; $p = $nl + 1;
                $nl = strpos($chunk, "\n", $p); $i = $slugIds[substr($chunk, $p + 25, $nl - $p - 51)] + $dateIds[substr($chunk, $nl - 23, 8)]; $output[$i] = $inc[$output[$i]]; $p = $nl + 1;
                $nl = strpos($chunk, "\n", $p); $i = $slugIds[substr($chunk, $p + 25, $nl - $p - 51)] + $dateIds[substr($chunk, $nl - 23, 8)]; $output[$i] = $inc[$output[$i]]; $p = $nl + 1;
                $nl = strpos($chunk, "\n", $p); $i = $slugIds[substr($chunk, $p + 25, $nl - $p - 51)] + $dateIds[substr($chunk, $nl - 23, 8)]; $output[$i] = $inc[$output[$i]]; $p = $nl + 1;
                $nl = strpos($chunk, "\n", $p); $i = $slugIds[substr($chunk, $p + 25, $nl - $p - 51)] + $dateIds[substr($chunk, $nl - 23, 8)]; $output[$i] = $inc[$output[$i]]; $p = $nl + 1;
                $nl = strpos($chunk, "\n", $p); $i = $slugIds[substr($chunk, $p + 25, $nl - $p - 51)] + $dateIds[substr($chunk, $nl - 23, 8)]; $output[$i] = $inc[$output[$i]]; $p = $nl + 1;
                $nl = strpos($chunk, "\n", $p); $i = $slugIds[substr($chunk, $p + 25, $nl - $p - 51)] + $dateIds[substr($chunk, $nl - 23, 8)]; $output[$i] = $inc[$output[$i]]; $p = $nl + 1;
                $nl = strpos($chunk, "\n", $p); $i = $slugIds[substr($chunk, $p + 25, $nl - $p - 51)] + $dateIds[substr($chunk, $nl - 23, 8)]; $output[$i] = $inc[$output[$i]]; $p = $nl + 1;
                $nl = strpos($chunk, "\n", $p); $i = $slugIds[substr($chunk, $p + 25, $nl - $p - 51)] + $dateIds[substr($chunk, $nl - 23, 8)]; $output[$i] = $inc[$output[$i]]; $p = $nl + 1;
                $nl = strpos($chunk, "\n", $p); $i = $slugIds[substr($chunk, $p + 25, $nl - $p - 51)] + $dateIds[substr($chunk, $nl - 23, 8)]; $output[$i] = $inc[$output[$i]]; $p = $nl + 1;
                $nl = strpos($chunk, "\n", $p); $i = $slugIds[substr($chunk, $p + 25, $nl - $p - 51)] + $dateIds[substr($chunk, $nl - 23, 8)]; $output[$i] = $inc[$output[$i]]; $p = $nl + 1;
                $nl = strpos($chunk, "\n", $p); $i = $slugIds[substr($chunk, $p + 25, $nl - $p - 51)] + $dateIds[substr($chunk, $nl - 23, 8)]; $output[$i] = $inc[$output[$i]]; $p = $nl + 1;
            }

            // Remaining lines
            while ($p < $lastNl) {
                $nl = strpos($chunk, "\n", $p);
                if ($nl === false || $nl > $lastNl) break;
                $slugLen = $nl - $p - 51;
                if ($slugLen > 0) {
                    $i = $slugIds[substr($chunk, $p + 25, $slugLen)] + $dateIds[substr($chunk, $nl - 23, 8)];
                    $output[$i] = $inc[$output[$i]];
                }
                $p = $nl + 1;
            }
        }

        fclose($fh);
        return $output;
    }

    private static function writeJson(string $outputPath, array $counts, array $slugs, array $dates, int $slugCount): void
    {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);

        $dateCount = self::DATE_COUNT;

        $datePfx = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePfx[$d] = '        "20' . $dates[$d] . '": ';
        }

        $escaped = [];
        for ($p = 0; $p < $slugCount; $p++) {
            $escaped[$p] = '"\\/blog\\/' . str_replace('/', '\\/', $slugs[$p]) . '"';
        }

        fwrite($out, '{');
        $first = true;
        $base = 0;
        $buffer = '';

        for ($p = 0; $p < $slugCount; $p++) {
            $entries = [];
            for ($d = 0; $d < $dateCount; $d++) {
                if (($c = $counts[$base + $d]) !== 0) {
                    $entries[] = $datePfx[$d] . $c;
                }
            }

            if ($entries !== []) {
                $buffer .= ($first ? '' : ',') . "\n    " . $escaped[$p] . ": {\n" . implode(",\n", $entries) . "\n    }";
                $first = false;

                if (strlen($buffer) > 131072) {
                    fwrite($out, $buffer);
                    $buffer = '';
                }
            }
            $base += $dateCount;
        }

        if ($buffer !== '') {
            fwrite($out, $buffer);
        }
        fwrite($out, "\n}");
        fclose($out);
    }
}