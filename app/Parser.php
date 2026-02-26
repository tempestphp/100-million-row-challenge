<?php

namespace App;

use App\Commands\Visit;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $fileSize = filesize($inputPath);
        $workers = (int) (getenv('WORKER_COUNT') ?: 8);

        // ─── Build date lookup ───
        // Map 7-char truncated dates ("Y-MM-DD") → sequential IDs for flat array indexing.

        $dateLookup = [];
        $dateLabels = [];
        $numDates = 0;

        $day = mktime(0, 0, 0, 1, 1, 2020);
        $stop = mktime(0, 0, 0, 12, 31, 2026);

        while ($day <= $stop) {
            $full = date('Y-m-d', $day);
            $dateLookup[substr($full, 3)] = $numDates;
            $dateLabels[$numDates] = $full;
            $numDates++;
            $day += 86400;
        }

        // ─── Pre-seed path map from Visit::all() ───
        // Guarantees all valid blog slugs are registered before parsing.
        // Paths are stored as slugs (after "/blog/") for compact hash keys.

        $slugIndex = [];
        $slugLabels = [];
        $numSlugs = 0;
        $blogPrefix = 25; // strlen("https://stitcher.io/blog/")

        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, $blogPrefix);
            $slugIndex[$slug] = $numSlugs * $numDates;
            $slugLabels[$numSlugs] = $slug;
            $numSlugs++;
        }

        // Scan file to establish first-seen slug ordering (may differ from Visit::all())
        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);
        $sample = fread($fh, min(32_000_000, $fileSize));
        fclose($fh);

        // Rebuild index using actual file ordering
        $slugIndex = [];
        $slugLabels = [];
        $numSlugs = 0;

        $sampleEnd = strrpos($sample, "\n");
        $sp = 0;

        while ($sp < $sampleEnd) {
            $nl = strpos($sample, "\n", $sp);
            if ($nl === false) {
                break;
            }

            $slug = substr($sample, $sp + $blogPrefix, $nl - $sp - $blogPrefix - 26);

            if (!isset($slugIndex[$slug])) {
                $slugIndex[$slug] = $numSlugs * $numDates;
                $slugLabels[$numSlugs] = $slug;
                $numSlugs++;
            }

            $sp = $nl + 1;
        }

        unset($sample);

        $totalCells = $numSlugs * $numDates;

        // ─── Split file into newline-aligned chunks ───

        $bounds = [0];
        $fh = fopen($inputPath, 'r');

        for ($i = 1; $i < $workers; $i++) {
            $target = intdiv($fileSize * $i, $workers);
            fseek($fh, $target);
            fgets($fh); // consume partial line
            $bounds[] = ftell($fh);
        }

        $bounds[] = $fileSize;
        fclose($fh);

        $numChunks = count($bounds) - 1;

        // ─── Fork child processes ───

        $tmpDir = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
        $myPid = getmypid();
        $childFiles = [];
        $childPids = [];

        for ($w = 1; $w < $numChunks; $w++) {
            $childFiles[$w] = $tmpDir . '/parse_' . $myPid . '_' . $w;
            $pid = pcntl_fork();

            if ($pid === 0) {
                $result = $this->crunch($inputPath, $bounds[$w], $bounds[$w + 1], $slugIndex, $dateLookup, $totalCells);
                file_put_contents($childFiles[$w], pack('V*', ...$result));
                exit(0);
            }

            $childPids[] = $pid;
        }

        // Parent crunches first chunk
        $tally = $this->crunch($inputPath, $bounds[0], $bounds[1], $slugIndex, $dateLookup, $totalCells);

        // ─── Collect and merge results ───

        foreach ($childPids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        for ($w = 1; $w < $numChunks; $w++) {
            $raw = file_get_contents($childFiles[$w]);
            unlink($childFiles[$w]);

            $j = 0;
            foreach (unpack('V*', $raw) as $v) {
                $tally[$j++] += $v;
            }
        }

        // ─── Emit JSON ───

        $out = fopen($outputPath, 'wb');
        $json = '{';
        $needComma = false;

        for ($s = 0; $s < $numSlugs; $s++) {
            $base = $s * $numDates;

            if ($needComma) {
                $json .= ',';
            }
            $needComma = true;

            $escaped = str_replace('/', '\\/', $slugLabels[$s]);
            $json .= "\n    \"\\/blog\\/" . $escaped . '": {';

            $firstEntry = true;

            for ($d = 0; $d < $numDates; $d++) {
                $n = $tally[$base + $d];
                if ($n === 0) {
                    continue;
                }

                $json .= $firstEntry ? "\n" : ",\n";
                $json .= '        "' . $dateLabels[$d] . '": ' . $n;
                $firstEntry = false;
            }

            $json .= "\n    }";

            if (strlen($json) > 65536) {
                fwrite($out, $json);
                $json = '';
            }
        }

        $json .= "\n}";
        fwrite($out, $json);
        fclose($out);
    }

    /**
     * Parse a byte range of the input file and return per-slug-per-date counts.
     */
    private function crunch(
        string $path,
        int $from,
        int $until,
        array $slugIndex,
        array $dateLookup,
        int $cells,
    ): array {
        $fh = fopen($path, 'rb');
        stream_set_read_buffer($fh, 0);
        fseek($fh, $from);

        $counts = array_fill(0, $cells, 0);
        $consumed = 0;
        $total = $until - $from;
        $bufSize = 8_388_608; // 8MB
        $prefix = 25; // strlen("https://stitcher.io/blog/")
        $stride = 52; // comma(1) + timestamp(25) + newline(1) + prefix(25)

        while ($consumed < $total) {
            $want = $total - $consumed;
            $raw = fread($fh, $want > $bufSize ? $bufSize : $want);
            if ($raw === false || $raw === '') {
                break;
            }

            $end = strrpos($raw, "\n");
            if ($end === false) {
                continue;
            }

            // Rewind past any partial trailing line
            $tail = strlen($raw) - $end - 1;
            if ($tail > 0) {
                fseek($fh, -$tail, SEEK_CUR);
            }
            $consumed += $end + 1;

            // Parse rows: find comma separator, extract slug + date
            $p = $prefix;
            $fence = $end - 104;

            while ($p < $fence) {
                $sep = strpos($raw, ',', $p);
                $counts[$slugIndex[substr($raw, $p, $sep - $p)] + $dateLookup[substr($raw, $sep + 4, 7)]]++;
                $p = $sep + $stride;

                $sep = strpos($raw, ',', $p);
                $counts[$slugIndex[substr($raw, $p, $sep - $p)] + $dateLookup[substr($raw, $sep + 4, 7)]]++;
                $p = $sep + $stride;
            }

            while ($p < $end) {
                $sep = strpos($raw, ',', $p);
                if ($sep === false) {
                    break;
                }
                $counts[$slugIndex[substr($raw, $p, $sep - $p)] + $dateLookup[substr($raw, $sep + 4, 7)]]++;
                $p = $sep + $stride;
            }
        }

        fclose($fh);

        return $counts;
    }
}
