<?php

namespace App;

use App\Commands\Visit;

final class Parser
{
    private array $dateIds;
    private array $dateStrings;
    private int $numDates;
    private array $slugBase;
    private int $numSlugs;

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();
        $this->initMaps();

        $fileSize = filesize($inputPath);

        if ($fileSize < 1_000_000) {
            $slugOrder = [];
            $slugSeen = [];
            $counts = $this->processChunk($inputPath, 0, $fileSize, $slugOrder, $slugSeen);
            $this->writeOutput($counts, $slugOrder, $outputPath);
            return;
        }

        // Multi-process parallel parsing
        $workerCount = 8;
        $boundaries = $this->chunkFile($inputPath, $fileSize, $workerCount);
        $childCount = count($boundaries);
        $tmpDir = sys_get_temp_dir();
        $useIgbinary = function_exists('igbinary_serialize');
        $pids = [];

        $total = $this->numSlugs * $this->numDates;

        for ($i = 1; $i < $childCount; $i++) {
            $pid = pcntl_fork();
            if ($pid === 0) {
                $so = [];
                $ss = [];
                $counts = $this->processChunk($inputPath, $boundaries[$i][0], $boundaries[$i][1], $so, $ss);
                // Extract non-zero entries for compact serialization
                $sparse = [];
                for ($j = 0; $j < $total; $j++) {
                    if ($counts[$j] > 0) {
                        $sparse[$j] = $counts[$j];
                    }
                }
                $serialized = $useIgbinary ? igbinary_serialize($sparse) : serialize($sparse);
                file_put_contents("$tmpDir/p100m_$i.tmp", $serialized);
                exit(0);
            }
            $pids[] = $pid;
        }

        // Parent processes chunk 0 and tracks slug insertion order
        $slugOrder = [];
        $slugSeen = [];
        $merged = $this->processChunk($inputPath, $boundaries[0][0], $boundaries[0][1], $slugOrder, $slugSeen);

        while (pcntl_wait($status) > 0);

        // Merge: sparse addition (only non-zero entries from children)
        for ($i = 1; $i < $childCount; $i++) {
            $tmpFile = "$tmpDir/p100m_$i.tmp";
            $raw = file_get_contents($tmpFile);
            unlink($tmpFile);
            $sparse = $useIgbinary ? igbinary_unserialize($raw) : unserialize($raw);

            foreach ($sparse as $idx => $cnt) {
                $merged[$idx] += $cnt;
            }
        }

        // Ensure all slugs with data are in the output order
        foreach ($this->slugBase as $slug => $base) {
            if (!isset($slugSeen[$slug])) {
                for ($di = 0; $di < $this->numDates; $di++) {
                    if ($merged[$base + $di] > 0) {
                        $slugOrder[] = $slug;
                        break;
                    }
                }
            }
        }

        $this->writeOutput($merged, $slugOrder, $outputPath);
    }

    private function initMaps(): void
    {
        $this->dateIds = [];
        $this->dateStrings = [];
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
                    $this->dateIds[$yy . '-' . $mm . '-' . sprintf('%02d', $d)] = $di;
                    $this->dateStrings[$di] = sprintf('%04d-%02d-%02d', $y, $m, $d);
                    $di++;
                }
            }
        }
        $this->numDates = $di;

        $this->slugBase = [];
        $si = 0;
        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, 25);
            if (!isset($this->slugBase[$slug])) {
                $this->slugBase[$slug] = $si * $this->numDates;
                $si++;
            }
        }
        $this->numSlugs = $si;
    }

    private function processChunk(string $filePath, int $start, int $end, array &$slugOrder, array &$slugSeen): array
    {
        $counts = array_fill(0, $this->numSlugs * $this->numDates, 0);
        $dateIds = $this->dateIds;
        $slugBase = $this->slugBase;

        $handle = fopen($filePath, 'r');
        fseek($handle, $start);
        $remaining = $end - $start;
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
        return $counts;
    }

    private function writeOutput(array &$counts, array &$slugOrder, string $outputPath): void
    {
        $numDates = $this->numDates;
        $dateStrings = $this->dateStrings;
        $data = [];
        foreach ($slugOrder as $slug) {
            $base = $this->slugBase[$slug];
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

    private function chunkFile(string $filePath, int $fileSize, int $workerCount): array
    {
        $chunkSize = intdiv($fileSize, $workerCount);
        $boundaries = [];
        $handle = fopen($filePath, 'r');
        $start = 0;
        for ($i = 0; $i < $workerCount - 1; $i++) {
            $end = $start + $chunkSize;
            fseek($handle, $end);
            $buf = fread($handle, 4096);
            if ($buf !== false && ($nl = strpos($buf, "\n")) !== false) {
                $end += $nl + 1;
            }
            $boundaries[] = [$start, $end];
            $start = $end;
        }
        $boundaries[] = [$start, $fileSize];
        fclose($handle);
        return $boundaries;
    }
}
