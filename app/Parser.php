<?php

declare(strict_types=1);

namespace App;

use App\Commands\Visit;

final class Parser
{
    private const int CHUNK_SIZE = 536_870_912; // 512MB
    private const int PROBE_SIZE = 2_097_152;   // 2MB
    private const int WRITE_BUF  = 1_048_576;   // 1MB

    private const int PREFIX_LEN = 25;

    private const array DIG = [
        '0'=>0,'1'=>1,'2'=>2,'3'=>3,'4'=>4,'5'=>5,'6'=>6,'7'=>7,'8'=>8,'9'=>9,
    ];

    public function parse(string $inputPath, string $outputPath): void
    {
        \gc_disable();

        $fileSize = \filesize($inputPath);
        if ($fileSize === false) {
            throw new \RuntimeException("Failed to stat input file: $inputPath");
        }

        // 1) Dates 2020..2026 -> dateId
        $yearBaseId = [];
        $daysBefore = [];
        $dateLabels = [];
        $numDates   = 0;

        for ($yy = 20; $yy <= 26; $yy++) {
            $yearBaseId[$yy] = $numDates;

            $daysBefore[$yy] = [0,0,0,0,0,0,0,0,0,0,0,0,0];
            $run = 0;

            for ($m = 1; $m <= 12; $m++) {
                $daysBefore[$yy][$m] = $run;

                $dim = match ($m) {
                    2           => ($yy % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default     => 31,
                };

                $mm = ($m < 10) ? "0{$m}" : (string)$m;

                for ($d = 1; $d <= $dim; $d++) {
                    $dd = ($d < 10) ? "0{$d}" : (string)$d;
                    $dateLabels[$numDates] = "{$yy}-{$mm}-{$dd}";
                    $numDates++;
                }

                $run += $dim;
            }
        }

        // 2) Slugs from probe + Visit::all()
        $fh = \fopen($inputPath, 'rb');
        if ($fh === false) {
            throw new \RuntimeException("Failed to read input file: $inputPath");
        }
        \stream_set_read_buffer($fh, 0);
        $sample = \fread($fh, \min($fileSize, self::PROBE_SIZE));
        \fclose($fh);

        if ($sample === false) {
            throw new \RuntimeException("Failed to read sample");
        }

        $slugIndex  = [];
        $slugLabels = [];
        $numSlugs   = 0;

        $bound = \strrpos($sample, "\n");
        if ($bound === false) $bound = \strlen($sample);

        for ($pos = 0; $pos < $bound;) {
            $nl = \strpos($sample, "\n", $pos + 52);
            if ($nl === false) break;

            $slug = \substr($sample, $pos + self::PREFIX_LEN, $nl - $pos - 51);
            if (!isset($slugIndex[$slug])) {
                $slugIndex[$slug]      = $numSlugs;
                $slugLabels[$numSlugs] = $slug;
                $numSlugs++;
            }
            $pos = $nl + 1;
        }
        unset($sample);

        foreach (Visit::all() as $visit) {
            $slug = \substr($visit->uri, self::PREFIX_LEN);
            if (!isset($slugIndex[$slug])) {
                $slugIndex[$slug]      = $numSlugs;
                $slugLabels[$numSlugs] = $slug;
                $numSlugs++;
            }
        }

        // 3) Parse -> bins (2 bytes per row)
        $bins = \array_fill(0, $numSlugs, '');

        $D = self::DIG;

        $fh = \fopen($inputPath, 'rb');
        if ($fh === false) {
            throw new \RuntimeException("Failed to read input file: $inputPath");
        }
        \stream_set_read_buffer($fh, 0);

        $remaining = $fileSize;

        while ($remaining > 0) {
            $readSize = $remaining > self::CHUNK_SIZE ? self::CHUNK_SIZE : $remaining;
            $chunk = \fread($fh, $readSize);
            if ($chunk === false) {
                throw new \RuntimeException("Failed to read chunk");
            }

            $chunkLength = \strlen($chunk);
            if ($chunkLength === 0) break;

            $remaining -= $chunkLength;

            $lastNl = \strrpos($chunk, "\n");
            if ($lastNl === false) break;

            $over = $chunkLength - $lastNl - 1;
            if ($over > 0) {
                \fseek($fh, -$over, \SEEK_CUR);
                $remaining += $over;
            }

            $pos = 0;

            // Make this margin big enough for “weirdly long” slugs near the end of the chunk.
            // If we’re too close to $lastNl, strpos() can return false and your offsets explode.
            $safe = $lastNl - 4096;
            if ($safe < 0) $safe = 0;

            // --- Unrolled 4x with guards (still fast; prevents bogus $nl)
            while ($pos < $safe) {
                // 1
                $nl = \strpos($chunk, "\n", $pos + 52);
                if ($nl === false || $nl > $lastNl) break;
                $slugId = $slugIndex[\substr($chunk, $pos + self::PREFIX_LEN, $nl - $pos - 51)];

                $yy = $D[$chunk[$nl - 23]] * 10 + $D[$chunk[$nl - 22]];
                $mm = $D[$chunk[$nl - 20]] * 10 + $D[$chunk[$nl - 19]];
                $dd = $D[$chunk[$nl - 17]] * 10 + $D[$chunk[$nl - 16]];
                if ($yy < 20 || $yy > 26 || $mm < 1 || $mm > 12) { $pos = $nl + 1; continue; }

                $dateId = $yearBaseId[$yy] + $daysBefore[$yy][$mm] + ($dd - 1);
                $bins[$slugId] .= \chr($dateId & 0xFF) . \chr($dateId >> 8);
                $pos = $nl + 1;

                // 2
                $nl = \strpos($chunk, "\n", $pos + 52);
                if ($nl === false || $nl > $lastNl) break;
                $slugId = $slugIndex[\substr($chunk, $pos + self::PREFIX_LEN, $nl - $pos - 51)];

                $yy = $D[$chunk[$nl - 23]] * 10 + $D[$chunk[$nl - 22]];
                $mm = $D[$chunk[$nl - 20]] * 10 + $D[$chunk[$nl - 19]];
                $dd = $D[$chunk[$nl - 17]] * 10 + $D[$chunk[$nl - 16]];
                if ($yy < 20 || $yy > 26 || $mm < 1 || $mm > 12) { $pos = $nl + 1; continue; }

                $dateId = $yearBaseId[$yy] + $daysBefore[$yy][$mm] + ($dd - 1);
                $bins[$slugId] .= \chr($dateId & 0xFF) . \chr($dateId >> 8);
                $pos = $nl + 1;

                // 3
                $nl = \strpos($chunk, "\n", $pos + 52);
                if ($nl === false || $nl > $lastNl) break;
                $slugId = $slugIndex[\substr($chunk, $pos + self::PREFIX_LEN, $nl - $pos - 51)];

                $yy = $D[$chunk[$nl - 23]] * 10 + $D[$chunk[$nl - 22]];
                $mm = $D[$chunk[$nl - 20]] * 10 + $D[$chunk[$nl - 19]];
                $dd = $D[$chunk[$nl - 17]] * 10 + $D[$chunk[$nl - 16]];
                if ($yy < 20 || $yy > 26 || $mm < 1 || $mm > 12) { $pos = $nl + 1; continue; }

                $dateId = $yearBaseId[$yy] + $daysBefore[$yy][$mm] + ($dd - 1);
                $bins[$slugId] .= \chr($dateId & 0xFF) . \chr($dateId >> 8);
                $pos = $nl + 1;

                // 4
                $nl = \strpos($chunk, "\n", $pos + 52);
                if ($nl === false || $nl > $lastNl) break;
                $slugId = $slugIndex[\substr($chunk, $pos + self::PREFIX_LEN, $nl - $pos - 51)];

                $yy = $D[$chunk[$nl - 23]] * 10 + $D[$chunk[$nl - 22]];
                $mm = $D[$chunk[$nl - 20]] * 10 + $D[$chunk[$nl - 19]];
                $dd = $D[$chunk[$nl - 17]] * 10 + $D[$chunk[$nl - 16]];
                if ($yy < 20 || $yy > 26 || $mm < 1 || $mm > 12) { $pos = $nl + 1; continue; }

                $dateId = $yearBaseId[$yy] + $daysBefore[$yy][$mm] + ($dd - 1);
                $bins[$slugId] .= \chr($dateId & 0xFF) . \chr($dateId >> 8);
                $pos = $nl + 1;
            }

            // Tail loop (safe + correct)
            while ($pos < $lastNl) {
                $nl = \strpos($chunk, "\n", $pos + 52);
                if ($nl === false || $nl > $lastNl) break;

                $slugId = $slugIndex[\substr($chunk, $pos + self::PREFIX_LEN, $nl - $pos - 51)];

                $yy = $D[$chunk[$nl - 23]] * 10 + $D[$chunk[$nl - 22]];
                $mm = $D[$chunk[$nl - 20]] * 10 + $D[$chunk[$nl - 19]];
                $dd = $D[$chunk[$nl - 17]] * 10 + $D[$chunk[$nl - 16]];

                if ($yy >= 20 && $yy <= 26 && $mm >= 1 && $mm <= 12) {
                    $dateId = $yearBaseId[$yy] + $daysBefore[$yy][$mm] + ($dd - 1);
                    $bins[$slugId] .= \chr($dateId & 0xFF) . \chr($dateId >> 8);
                }

                $pos = $nl + 1;
            }
        }

        \fclose($fh);

        // 4) Bulk count to dense grid
        $grid = \array_fill(0, $numSlugs * $numDates, 0);

        for ($slugId = 0; $slugId < $numSlugs; $slugId++) {
            $bin = $bins[$slugId];
            if ($bin === '') continue;

            $base = $slugId * $numDates;
            foreach (\array_count_values(\unpack('v*', $bin)) as $dateId => $count) {
                $grid[$base + $dateId] = $count;
            }
        }
        unset($bins);

        // 5) Output
        $out = \fopen($outputPath, 'wb');
        if ($out === false) {
            throw new \RuntimeException("Failed to write output file: $outputPath");
        }
        \stream_set_write_buffer($out, self::WRITE_BUF);

        $datePfx = [];
        for ($dateId = 0; $dateId < $numDates; $dateId++) {
            $datePfx[$dateId] = '        "20' . $dateLabels[$dateId] . '": ';
        }

        $slugHdr = [];
        for ($slugId = 0; $slugId < $numSlugs; $slugId++) {
            $slugHdr[$slugId] = '"\\/blog\\/' . \str_replace('/', '\\/', $slugLabels[$slugId]) . '"';
        }

        \fwrite($out, '{');
        $first = true;

        for ($slugId = 0; $slugId < $numSlugs; $slugId++) {
            $base      = $slugId * $numDates;
            $body      = '';
            $sep       = '';

            for ($dateId = 0; $dateId < $numDates; $dateId++) {
                $count = $grid[$base + $dateId];
                if ($count === 0) continue;

                $body .= $sep . $datePfx[$dateId] . $count;
                $sep = ",\n";
            }

            if ($body === '') continue;

            \fwrite($out, ($first ? '' : ',') . "\n    " . $slugHdr[$slugId] . ": {\n" . $body . "\n    }");
            $first = false;
        }

        \fwrite($out, "\n}");
        \fclose($out);
    }
}