<?php

declare(strict_types=1);

namespace App;

use App\Commands\Visit;

final class Parser
{
    private const int CHUNK_SIZE = 262_144;
    private const int PROBE_SIZE = 2_097_152;
    private const int WRITE_BUF  = 1_048_576;

    public static function parse(string $inputPath, string $outputPath): void
    {
        \gc_disable();

        $fileSize   = \filesize($inputPath);
        $dateIds    = [];
        $dateLabels = [];
        $numDates   = 0;

        for ($year = 21; $year <= 26; $year++) {
            for ($month = 1; $month <= 12; $month++) {
                $daysInMonth = match ($month) {
                    2           => ($year % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default     => 31,
                };

                $monthStr   = $month < 10 ? "0{$month}" : (string) $month;
                $datePrefix = "{$year}-{$monthStr}-";

                for ($day = 1; $day <= $daysInMonth; $day++) {
                    $key                   = $datePrefix . ($day < 10 ? "0{$day}" : (string) $day);
                    $dateIds[$key]         = $numDates;
                    $dateLabels[$numDates] = $key;
                    $numDates++;
                }
            }
        }

        $fileHandle = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($fileHandle, 0);
        $sample = \fread($fileHandle, \min($fileSize, self::PROBE_SIZE));
        \fclose($fileHandle);

        $slugBase   = [];
        $slugLabels = [];
        $numSlugs   = 0;
        $bound      = \strrpos($sample, "\n");

        for ($pos = 0; $pos < $bound;) {
            $newlinePos = \strpos($sample, "\n", $pos + 52);

            if ($newlinePos === false) {
                break;
            }

            $slug = \substr($sample, $pos + 25, $newlinePos - $pos - 51);

            if (!isset($slugBase[$slug])) {
                $slugBase[$slug]       = $numSlugs * $numDates;
                $slugLabels[$numSlugs] = $slug;
                $numSlugs++;
            }

            $pos = $newlinePos + 1;
        }

        unset($sample);

        foreach (Visit::all() as $visit) {
            $slug = \substr($visit->uri, 25);

            if (!isset($slugBase[$slug])) {
                $slugBase[$slug]       = $numSlugs * $numDates;
                $slugLabels[$numSlugs] = $slug;
                $numSlugs++;
            }
        }

        $counts     = \array_fill(0, $numSlugs * $numDates, 0);
        $fileHandle = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($fileHandle, 0);
        $remaining  = $fileSize;

        while ($remaining > 0) {
            $chunk       = \fread($fileHandle, $remaining > self::CHUNK_SIZE ? self::CHUNK_SIZE : $remaining);
            $chunkLength = \strlen($chunk);

            if ($chunkLength === 0) {
                break;
            }

            $remaining -= $chunkLength;

            $lastNl = \strrpos($chunk, "\n");

            if ($lastNl === false) {
                break;
            }

            $over = $chunkLength - $lastNl - 1;
            if ($over > 0) {
                \fseek($fileHandle, -$over, \SEEK_CUR);
                $remaining += $over;
            }

            $pos  = 25;
            $safe = $lastNl - 600;

            while ($pos < $safe) {
                $sep = \strpos($chunk, ',', $pos);
                $counts[$slugBase[\substr($chunk, $pos, $sep - $pos)] + $dateIds[\substr($chunk, $sep + 3, 8)]]++;
                $pos = $sep + 52;

                $sep = \strpos($chunk, ',', $pos);
                $counts[$slugBase[\substr($chunk, $pos, $sep - $pos)] + $dateIds[\substr($chunk, $sep + 3, 8)]]++;
                $pos = $sep + 52;

                $sep = \strpos($chunk, ',', $pos);
                $counts[$slugBase[\substr($chunk, $pos, $sep - $pos)] + $dateIds[\substr($chunk, $sep + 3, 8)]]++;
                $pos = $sep + 52;

                $sep = \strpos($chunk, ',', $pos);
                $counts[$slugBase[\substr($chunk, $pos, $sep - $pos)] + $dateIds[\substr($chunk, $sep + 3, 8)]]++;
                $pos = $sep + 52;

                $sep = \strpos($chunk, ',', $pos);
                $counts[$slugBase[\substr($chunk, $pos, $sep - $pos)] + $dateIds[\substr($chunk, $sep + 3, 8)]]++;
                $pos = $sep + 52;

                $sep = \strpos($chunk, ',', $pos);
                $counts[$slugBase[\substr($chunk, $pos, $sep - $pos)] + $dateIds[\substr($chunk, $sep + 3, 8)]]++;
                $pos = $sep + 52;

                $sep = \strpos($chunk, ',', $pos);
                $counts[$slugBase[\substr($chunk, $pos, $sep - $pos)] + $dateIds[\substr($chunk, $sep + 3, 8)]]++;
                $pos = $sep + 52;

                $sep = \strpos($chunk, ',', $pos);
                $counts[$slugBase[\substr($chunk, $pos, $sep - $pos)] + $dateIds[\substr($chunk, $sep + 3, 8)]]++;
                $pos = $sep + 52;
            }

            while ($pos < $lastNl) {
                $sep = \strpos($chunk, ',', $pos);

                if ($sep === false || $sep >= $lastNl) {
                    break;
                }

                $counts[$slugBase[\substr($chunk, $pos, $sep - $pos)] + $dateIds[\substr($chunk, $sep + 3, 8)]]++;
                $pos = $sep + 52;
            }
        }

        \fclose($fileHandle);

        $outputHandle = \fopen($outputPath, 'wb');

        \stream_set_write_buffer($outputHandle, self::WRITE_BUF);

        $datePfx = [];
        for ($dateId = 0; $dateId < $numDates; $dateId++) {
            $datePfx[$dateId] = '        "20' . $dateLabels[$dateId] . '": ';
        }

        $slugHdr = [];
        for ($slugId = 0; $slugId < $numSlugs; $slugId++) {
            $slugHdr[$slugId] = '"\/blog\/' . \str_replace('/', '\/', $slugLabels[$slugId]) . '"';
        }

        \fwrite($outputHandle, '{');
        $first = true;

        for ($slugId = 0; $slugId < $numSlugs; $slugId++) {
            $base      = $slugId * $numDates;
            $body      = '';
            $separator = '';

            for ($dateId = 0; $dateId < $numDates; $dateId++) {
                $count = $counts[$base + $dateId];

                if ($count === 0) {
                    continue;
                }

                $body     .= $separator . $datePfx[$dateId] . $count;
                $separator = ",\n";
            }

            if ($body === '') {
                continue;
            }

            \fwrite($outputHandle, ($first ? '' : ',') . "\n    " . $slugHdr[$slugId] . ": {\n" . $body . "\n    }");
            $first = false;
        }

        \fwrite($outputHandle, "\n}");
        \fclose($outputHandle);
    }
}
