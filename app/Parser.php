<?php

namespace App;

use App\Commands\Visit;

final class Parser
{
    private const int CHUNK_SIZE = 262_144;
    private const int PROBE_SIZE = 2_097_152;
    private const int WRITE_BUF = 1_048_576;

    public function parse(string $inputPath, string $outputPath): void
    {
        \gc_disable();

        $fileSize = \filesize($inputPath);
        $dateIds = [];
        $dateLabels = [];
        $numDates = 0;

        for ($y = 19; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $days = match ($m) {
                    2 => ($y % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $mS = $m < 10 ? "0{$m}" : (string)$m;
                for ($d = 1; $d <= $days; $d++) {
                    $key = "{$y}-{$mS}-" . ($d < 10 ? "0{$d}" : (string)$d);
                    $dateIds[$key] = $numDates;
                    $dateLabels[$numDates] = $key;
                    $numDates++;
                }
            }
        }

        $h = \fopen($inputPath, 'rb');
        $sample = \fread($h, \min($fileSize, self::PROBE_SIZE));
        \fclose($h);

        $slugBase = [];
        $slugLabels = [];
        $numSlugs = 0;
        $bound = \strrpos($sample, "\n");
        for ($pos = 0; $pos < $bound;) {
            $nl = \strpos($sample, "\n", $pos + 52);
            if ($nl === false) break;
            $slug = \substr($sample, $pos + 25, $nl - $pos - 51);
            if (!isset($slugBase[$slug])) {
                $slugBase[$slug] = $numSlugs * $numDates;
                $slugLabels[$numSlugs] = $slug;
                $numSlugs++;
            }
            $pos = $nl + 1;
        }

        foreach (Visit::all() as $visit) {
            $slug = \substr($visit->uri, 25);
            if (!isset($slugBase[$slug])) {
                $slugBase[$slug] = $numSlugs * $numDates;
                $slugLabels[$numSlugs] = $slug;
                $numSlugs++;
            }
        }

        $counts = \array_fill(0, $numSlugs * $numDates, 0);
        $h = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($h, 0);
        $remaining = $fileSize;

        while ($remaining > 0) {
            $chunk = \fread($h, \min($remaining, self::CHUNK_SIZE));
            $len = \strlen($chunk);
            if ($len === 0) break;
            $remaining -= $len;

            $lastNl = \strrpos($chunk, "\n");
            if ($lastNl === false) break;
            $over = $len - $lastNl - 1;
            if ($over > 0) {
                \fseek($h, -$over, \SEEK_CUR);
                $remaining += $over;
            }

            $pos = 25;
            $safe = $lastNl - 600;

            while ($pos < $safe) {
                $s = \strpos($chunk, ',', $pos);
                $counts[$slugBase[\substr($chunk, $pos, $s - $pos)] + $dateIds[\substr($chunk, $s + 3, 8)]]++;
                $pos = $s + 52;
                $s = \strpos($chunk, ',', $pos);
                $counts[$slugBase[\substr($chunk, $pos, $s - $pos)] + $dateIds[\substr($chunk, $s + 3, 8)]]++;
                $pos = $s + 52;
                $s = \strpos($chunk, ',', $pos);
                $counts[$slugBase[\substr($chunk, $pos, $s - $pos)] + $dateIds[\substr($chunk, $s + 3, 8)]]++;
                $pos = $s + 52;
            }

            while ($pos < $lastNl) {
                $s = \strpos($chunk, ',', $pos);
                if ($s === false || $s >= $lastNl) break;
                $counts[$slugBase[\substr($chunk, $pos, $s - $pos)] + $dateIds[\substr($chunk, $s + 3, 8)]]++;
                $pos = $s + 52;
            }
        }
        \fclose($h);

        $this->writeFinalJson($outputPath, $slugLabels, $dateLabels, $counts, $numSlugs, $numDates);
    }

    private function writeFinalJson(string $path, array $slugs, array $dates, array &$counts, int $numSlugs, int $numDates): void
    {
        $out = \fopen($path, 'wb');
        \stream_set_write_buffer($out, self::WRITE_BUF);

        $datePfx = [];
        for ($i = 0; $i < $numDates; $i++) {
            $datePfx[$i] = '        "20' . $dates[$i] . '": ';
        }

        \fwrite($out, '{');
        $firstSlug = true;

        for ($sId = 0; $sId < $numSlugs; $sId++) {
            $base = $sId * $numDates;
            $body = '';
            $sep = '';
            for ($dId = 0; $dId < $numDates; $dId++) {
                if (($val = $counts[$base + $dId]) > 0) {
                    $body .= $sep . $datePfx[$dId] . $val;
                    $sep = ",\n";
                }
            }

            $hdr = '"\/blog\/' . \str_replace('/', '\/', $slugs[$sId]) . '"';
            \fwrite($out, ($firstSlug ? '' : ',') . "\n    " . $hdr . ": {\n" . $body . "\n    }");
            $firstSlug = false;
        }
        \fwrite($out, "\n}");
        \fclose($out);
    }
}