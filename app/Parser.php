<?php

namespace App;

use App\Commands\Visit;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        \gc_disable();

        $fileSize = \filesize($inputPath);
        $dateIds = [];
        $dateLabels = [];
        $numDates = 0;

        $paddings = [];
        for ($i = 1; $i <= 31; $i++) {
            $paddings[$i] = $i < 10 ? "0$i" : (string)$i;
        }

        for ($y = 21; $y <= 26; $y++) {
            $yearPrefix = $y . '-';
            $monthDays = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

            if ($y === 24) {
                $monthDays[2] = 29;
            }

            for ($m = 1; $m <= 12; $m++) {
                $prefix = $yearPrefix . $paddings[$m] . '-';
                $days = $monthDays[$m];
                for ($d = 1; $d <= $days; $d++) {
                    $key = $prefix . $paddings[$d];
                    $dateIds[$key] = $numDates;
                    $dateLabels[$numDates] = $key;
                    $numDates++;
                }
            }
        }

        $h = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($h, 0);
        $sample = \fread($h, $fileSize > 181000 ? 181000 : $fileSize);
        \fclose($h);

        $slugBase = [];
        $slugLabels = [];
        $numSlugs = 0;
        $bound = \strrpos($sample, "\n");
        if ($bound === false) {
            $bound = 0;
        }

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

        unset($sample);

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
            $toRead = $remaining > 1_048_576 ? 1_048_576 : $remaining;
            $chunk = \fread($h, $toRead);
            $len = \strlen($chunk);

            if ($len === 0) {
                break;
            }

            $remaining -= $len;

            $lastNl = \strrpos($chunk, "\n");
            if ($lastNl === false) break;
            $over = $len - $lastNl - 1;
            if ($over > 0) {
                \fseek($h, -$over, \SEEK_CUR);
                $remaining += $over;
            }

            $pos = 25;
            $safe = $lastNl - 1010;

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

                $s = \strpos($chunk, ',', $pos);
                $counts[$slugBase[\substr($chunk, $pos, $s - $pos)] + $dateIds[\substr($chunk, $s + 3, 8)]]++;
                $pos = $s + 52;

                $s = \strpos($chunk, ',', $pos);
                $counts[$slugBase[\substr($chunk, $pos, $s - $pos)] + $dateIds[\substr($chunk, $s + 3, 8)]]++;
                $pos = $s + 52;

                $s = \strpos($chunk, ',', $pos);
                $counts[$slugBase[\substr($chunk, $pos, $s - $pos)] + $dateIds[\substr($chunk, $s + 3, 8)]]++;
                $pos = $s + 52;

                $s = \strpos($chunk, ',', $pos);
                $counts[$slugBase[\substr($chunk, $pos, $s - $pos)] + $dateIds[\substr($chunk, $s + 3, 8)]]++;
                $pos = $s + 52;

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

    private function writeFinalJson($path, $slugs, $dates, &$counts, $numSlugs, $numDates)
    {
        $out = \fopen($path, 'wb');
        \stream_set_write_buffer($out, 1_048_576);

        $datePfx = [];
        for ($i = 0; $i < $numDates; $i++) {
            $datePfx[$i] = '        "20' . $dates[$i] . '": ';
        }

        \fwrite($out, '{');
        $firstSlug = true;

        for ($sId = 0; $sId < $numSlugs; $sId++) {
            $base = $sId * $numDates;
            $firstDate = -1;

            for ($dId = 0; $dId < $numDates; $dId++) {
                if ($counts[$base + $dId] !== 0) {
                    $firstDate = $dId;
                    break;
                }
            }

            if ($firstDate === -1) {
                continue;
            }

            $buffer = $firstSlug ? "\n    " : ",\n    ";
            $firstSlug = false;

            $buffer .= '"\/blog\/' . \str_replace('/', '\/', $slugs[$sId]) . '": {' . "\n";
            $buffer .= $datePfx[$firstDate] . $counts[$base + $firstDate];

            for ($dId = $firstDate + 1; $dId < $numDates; $dId++) {
                $count = $counts[$base + $dId];

                if ($count === 0) {
                    continue;
                }

                $buffer .= ",\n" . $datePfx[$dId] . $count;
            }

            $buffer .= "\n    }";

            \fwrite($out, $buffer);
        }

        \fwrite($out, "\n}");
        \fclose($out);
    }
}