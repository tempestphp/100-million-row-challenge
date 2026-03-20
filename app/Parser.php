<?php

namespace App;

use function array_fill;
use function fclose;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use const SEEK_CUR;
use const SEEK_END;

final class Parser
{
    private const int SLUG_TOTAL    = 268;
    private const int INITIAL_READ  = 181_000;
    private const int DISC_READ     = 1_048_576;
    private const int CHUNK_READ    = 1_048_576;
    private const int UNROLL        = 10;
    private const string URL_PREFIX = 'https://stitcher.io/blog/';

    public static function parse($inputPath, $outputPath)
    {
        gc_disable();

        $dateIds = [];
        $dates = [];
        $dc = 0;
        for ($y = 1; $y <= 6; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) { 2 => $y === 4 ? 29 : 28, 4, 6, 9, 11 => 30, default => 31 };
                $mStr = ($m < 10 ? '0' : '') . $m;
                $ymStr = "{$y}-{$mStr}-";
                for ($d = 1; $d <= $maxD; $d++) {
                    $dStr = ($d < 10 ? '0' : '') . $d;
                    $dateIds[$ymStr . $dStr] = $dc;
                    $dates[$dc] = '202' . $y . '-' . $mStr . '-' . $dStr;
                    $dc++;
                }
            }
        }

        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);

        $paths = [];
        $seen = [];
        $pathCount = 0;
        $raw = fread($fh, self::INITIAL_READ);
        $lastNl = strrpos($raw, "\n") ?: 0;
        $pos = 0;

        while ($pos < $lastNl && $pathCount < self::SLUG_TOTAL) {
            $nl = strpos($raw, "\n", $pos + 52);
            if ($nl === false) { break; }
            $slug = substr($raw, $pos + 25, $nl - $pos - 51);
            if (! isset($seen[$slug])) {
                $paths[$pathCount] = $slug;
                $seen[$slug] = $pathCount * $dc;
                $pathCount++;
            }
            $pos = $nl + 1;
        }

        if ($pathCount < self::SLUG_TOTAL) {
            $leftover = substr($raw, $lastNl + 1);
            while ($pathCount < self::SLUG_TOTAL && ! feof($fh)) {
                $chunk = $leftover . fread($fh, self::DISC_READ);
                if ($chunk === '') { break; }
                $lastNl = strrpos($chunk, "\n");
                if ($lastNl === false) { $leftover = $chunk; continue; }
                $pos = 25;
                while ($pos < $lastNl) {
                    $sep = strpos($chunk, ',', $pos);
                    if ($sep === false || $sep >= $lastNl) { break; }
                    $slug = substr($chunk, $pos, $sep - $pos);
                    if (! isset($seen[$slug])) {
                        $paths[$pathCount] = $slug;
                        $seen[$slug] = $pathCount * $dc;
                        $pathCount++;
                        if ($pathCount === self::SLUG_TOTAL) { break 2; }
                    }
                    $pos = $sep + 52;
                }
                $leftover = substr($chunk, $lastNl + 1);
            }
        }
        unset($raw, $seen);

        $keyBytes = 1;
        while (true) {
            $keys = [];
            foreach ($paths as $slug) {
                $key = substr(self::URL_PREFIX . $slug, -$keyBytes);
                if (isset($keys[$key])) { $keyBytes++; continue 2; }
                $keys[$key] = true;
            }
            break;
        }

        $maxLineBytes = 0;
        $slugLookup = [];
        foreach ($paths as $id => $slug) {
            $lineBytes = strlen($slug) + 52;
            if ($lineBytes > $maxLineBytes) { $maxLineBytes = $lineBytes; }
            $slugLookup[substr(self::URL_PREFIX . $slug, -$keyBytes)] = ($lineBytes << 20) | ($id * $dc);
        }

        $bucketSize = $pathCount * $dc;
        $keyOffset = 26 + $keyBytes;
        $slotMask = (1 << 20) - 1;
        $unrollFloor = ($maxLineBytes * self::UNROLL) + $keyOffset;
        $dateOff = 22;
        $dateLen = 7;
        $shift = 20;
        $counts = array_fill(0, $bucketSize, 0);

        fseek($fh, 0, SEEK_END);
        $fileSize = ftell($fh);
        fseek($fh, 0);
        $remaining = $fileSize;

        while ($remaining > 0) {
            $toRead = $remaining > self::CHUNK_READ ? self::CHUNK_READ : $remaining;
            $chunk = fread($fh, $toRead);
            $chunkLen = strlen($chunk);
            $remaining -= $chunkLen;
            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) { break; }
            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) { fseek($fh, -$tail, SEEK_CUR); $remaining += $tail; }

            $pos = $lastNl;
            while ($pos > $unrollFloor) {
                $token = $slugLookup[substr($chunk, $pos - $keyOffset, $keyBytes)];
                $counts[($token & $slotMask) + $dateIds[substr($chunk, $pos - $dateOff, $dateLen)]]++;
                $pos -= $token >> $shift;

                $token = $slugLookup[substr($chunk, $pos - $keyOffset, $keyBytes)];
                $counts[($token & $slotMask) + $dateIds[substr($chunk, $pos - $dateOff, $dateLen)]]++;
                $pos -= $token >> $shift;

                $token = $slugLookup[substr($chunk, $pos - $keyOffset, $keyBytes)];
                $counts[($token & $slotMask) + $dateIds[substr($chunk, $pos - $dateOff, $dateLen)]]++;
                $pos -= $token >> $shift;

                $token = $slugLookup[substr($chunk, $pos - $keyOffset, $keyBytes)];
                $counts[($token & $slotMask) + $dateIds[substr($chunk, $pos - $dateOff, $dateLen)]]++;
                $pos -= $token >> $shift;

                $token = $slugLookup[substr($chunk, $pos - $keyOffset, $keyBytes)];
                $counts[($token & $slotMask) + $dateIds[substr($chunk, $pos - $dateOff, $dateLen)]]++;
                $pos -= $token >> $shift;

                $token = $slugLookup[substr($chunk, $pos - $keyOffset, $keyBytes)];
                $counts[($token & $slotMask) + $dateIds[substr($chunk, $pos - $dateOff, $dateLen)]]++;
                $pos -= $token >> $shift;

                $token = $slugLookup[substr($chunk, $pos - $keyOffset, $keyBytes)];
                $counts[($token & $slotMask) + $dateIds[substr($chunk, $pos - $dateOff, $dateLen)]]++;
                $pos -= $token >> $shift;

                $token = $slugLookup[substr($chunk, $pos - $keyOffset, $keyBytes)];
                $counts[($token & $slotMask) + $dateIds[substr($chunk, $pos - $dateOff, $dateLen)]]++;
                $pos -= $token >> $shift;
            }

            while ($pos >= $keyOffset) {
                $token = $slugLookup[substr($chunk, $pos - $keyOffset, $keyBytes)];
                $counts[($token & $slotMask) + $dateIds[substr($chunk, $pos - $dateOff, $dateLen)]]++;
                $pos -= $token >> $shift;
            }
        }

        fclose($fh);

        self::writeJson($outputPath, $counts, $paths, $dates, $dc, $pathCount);
    }

    private static function writeJson($outputPath, $counts, $paths, $dates, $dateCount, $pathCount)
    {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);
        fwrite($out, '{');

        $datePrefixes = [];
        for ($d = $dateCount - 1; $d >= 0; $d--) {
            $datePrefixes[$d] = '        "' . $dates[$d] . '": ';
        }

        $pathHeaders = [];
        for ($p = $pathCount - 1; $p >= 0; $p--) {
            $pathHeaders[$p] = '"\/blog\/' . $paths[$p] . '": {';
        }

        $sep = "\n    ";
        $base = 0;

        for ($p = 0; $p < $pathCount; $p++) {
            $firstDay = -1;
            $c = $base;
            for ($d = 0; $d < $dateCount; $d++) {
                if ($counts[$c] !== 0) { $firstDay = $d; break; }
                $c++;
            }

            if ($firstDay === -1) { $base += $dateCount; continue; }

            $json = $sep . $pathHeaders[$p] . "\n" . $datePrefixes[$firstDay] . $counts[$c];
            $sep = ",\n    ";

            for ($d = $firstDay + 1; $d < $dateCount; $d++) {
                $c++;
                if ($counts[$c] === 0) { continue; }
                $json .= ",\n" . $datePrefixes[$d] . $counts[$c];
            }

            $json .= "\n    }";
            fwrite($out, $json);
            $base += $dateCount;
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}