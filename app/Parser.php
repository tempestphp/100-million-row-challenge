<?php

namespace App;

use App\Commands\Visit;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        // Pre-compute date IDs using 8-char key "YY-MM-DD" for faster hashing
        $dateIds = [];
        $dateStrings = [];
        $di = 0;
        $monthDays = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        for ($y = 2019; $y <= 2027; $y++) {
            $leap = ($y % 4 === 0 && ($y % 100 !== 0 || $y % 400 === 0));
            $yy = sprintf('%02d', $y % 100);
            for ($m = 1; $m <= 12; $m++) {
                $days = $monthDays[$m - 1];
                if ($m === 2 && $leap) $days = 29;
                $mm = sprintf('%02d', $m);
                for ($d = 1; $d <= $days; $d++) {
                    $dateIds[$yy . '-' . $mm . '-' . sprintf('%02d', $d)] = $di;
                    $dateStrings[$di] = sprintf('%04d-%02d-%02d', $y, $m, $d);
                    $di++;
                }
            }
        }
        $numDates = $di;

        // Pre-compute slug IDs from Visit::all()
        $slugBase = [];
        $si = 0;
        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, 25);
            if (!isset($slugBase[$slug])) {
                $slugBase[$slug] = $si * $numDates;
                $si++;
            }
        }

        // Flat counts array: counts[slugId * numDates + dateId]
        $counts = array_fill(0, $si * $numDates, 0);

        // Track slug insertion order for output
        $slugOrder = [];
        $slugSeen = [];

        // Parse file
        $fileSize = filesize($inputPath);
        $handle = fopen($inputPath, 'r');
        $remaining = $fileSize;
        $leftover = '';

        while ($remaining > 0) {
            $chunk = fread($handle, min(4_194_304, $remaining));
            if ($chunk === false || $chunk === '') break;
            $remaining -= strlen($chunk);

            $startPos = 0;

            if ($leftover !== '') {
                $firstNl = strpos($chunk, "\n");
                if ($firstNl === false) {
                    $leftover .= $chunk;
                    continue;
                }
                $line = $leftover . substr($chunk, 0, $firstNl);
                $len = strlen($line);
                if ($len > 51) {
                    $sep = strpos($line, ',', 25);
                    if ($sep !== false) {
                        $slug = substr($line, 25, $sep - 25);
                        if (isset($slugBase[$slug])) {
                            $counts[$slugBase[$slug] + $dateIds[substr($line, $sep + 3, 8)]]++;
                            if (!isset($slugSeen[$slug])) { $slugSeen[$slug] = true; $slugOrder[] = $slug; }
                        }
                    }
                }
                $startPos = $firstNl + 1;
                $leftover = '';
            }

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false || $lastNl < $startPos) {
                $leftover = ($startPos > 0) ? substr($chunk, $startPos) : $chunk;
                continue;
            }
            if ($lastNl < strlen($chunk) - 1) {
                $leftover = substr($chunk, $lastNl + 1);
            } else {
                $leftover = '';
            }

            // Hot loop — comma search, fixed 52-char jump, flat array, 8-char date key
            $pos = $startPos + 25;
            while ($pos < $lastNl) {
                $sep = strpos($chunk, ',', $pos);
                if ($sep === false || $sep >= $lastNl) break;
                $slug = substr($chunk, $pos, $sep - $pos);
                $counts[$slugBase[$slug] + $dateIds[substr($chunk, $sep + 3, 8)]]++;
                if (!isset($slugSeen[$slug])) { $slugSeen[$slug] = true; $slugOrder[] = $slug; }
                $pos = $sep + 52;
            }
        }

        if ($leftover !== '') {
            $len = strlen($leftover);
            if ($len > 51) {
                $sep = strpos($leftover, ',', 25);
                if ($sep !== false) {
                    $slug = substr($leftover, 25, $sep - 25);
                    if (isset($slugBase[$slug])) {
                        $counts[$slugBase[$slug] + $dateIds[substr($leftover, $sep + 3, 8)]]++;
                        if (!isset($slugSeen[$slug])) { $slugSeen[$slug] = true; $slugOrder[] = $slug; }
                    }
                }
            }
        }

        fclose($handle);

        // Build output in insertion order, dates chronological from pre-computation
        $data = [];
        foreach ($slugOrder as $slug) {
            $base = $slugBase[$slug];
            $dates = [];
            for ($di = 0; $di < $numDates; $di++) {
                $c = $counts[$base + $di];
                if ($c > 0) {
                    $dates[$dateStrings[$di]] = $c;
                }
            }
            if (!empty($dates)) {
                $data['/blog/' . $slug] = $dates;
            }
        }

        file_put_contents($outputPath, json_encode($data, JSON_PRETTY_PRINT));
    }
}
