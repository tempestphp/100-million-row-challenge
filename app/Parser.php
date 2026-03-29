<?php

namespace App;

use App\Commands\Visit;

// potential buffer sizes:
// 1 << 14 = 16384
// 1 << 15 = 32768
// 1 << 16 = 65536
// 1 << 17 = 131072
// 1 << 18 = 262144
// 1 << 19 = 524288
// 1 << 20 = 1048576
// 1 << 21 = 2097152
// 1 << 22 = 4194304
// 1 << 23 = 8388608

final class Parser
{
    private const WORKER_COUNT = 10;
    private const WRITE_BUF = 1048576;
    private const PROBE_SIZE = 1048576;

    public function parse($inputPath, $outputPath)
    {
        \gc_disable();

        $fileSize = \filesize($inputPath);

        // ── Enumerate every possible calendar date as a compact 2-byte id ──
        $dateChars  = [];   // "YY-MM-DD" → 2-byte string (little-endian id)
        $dateLabels = [];   // id → "YY-MM-DD"
        $totalDates = 0;

        for ($yr = 20; $yr <= 26; $yr++) {
            for ($mo = 1; $mo <= 12; $mo++) {
                $dim = match ($mo) {
                    2       => (($yr + 2000) % 4 === 0) ? 29 : 28,
                    4,6,9,11 => 30,
                    default  => 31,
                };
                $moStr = $mo < 10 ? "0{$mo}" : (string) $mo;
                $pfx   = "{$yr}-{$moStr}-";

                for ($dy = 1; $dy <= $dim; $dy++) {
                    $key = $pfx . ($dy < 10 ? "0{$dy}" : (string) $dy);
                    $dateLabels[$totalDates] = $key;
                    $dateChars[$key] = \chr($totalDates & 0xFF) . \chr($totalDates >> 8);
                    $totalDates++;
                }
            }
        }

        // ── Discover slug→id map by scanning the first X MB ──
        $slugToId  = [];
        $slugList  = [];

        $probe = \fopen($inputPath, 'r');
        \stream_set_read_buffer($probe, 0);
        $probeLen = (int) \min($fileSize, self::PROBE_SIZE); //$fileSize > self::PROBE_SIZE ? self::PROBE_SIZE : $fileSize;
        $sample   = \fread($probe, $probeLen);
        \fclose($probe);
        $cur   = 0;
        $bound = \strrpos($sample, "\n");

        while ($cur < $bound) {
            $eol = \strpos($sample, "\n", $cur + 52);
            if ($eol === false) break;
            // 25 = strlen("https://stitcher.io/blog/"), 51 = 25 + 1(comma) + 25(datetime)
            $slug = \substr($sample, $cur + 25, $eol - $cur - 51);
            if (!isset($slugToId[$slug])) {
                $slugToId[$slug] = \count($slugList);
                $slugList[] = $slug;
            }
            $cur = $eol + 1;
        }
        unset($sample);

        // Ensure every known Visit slug is registered
        // We can't just take this list as the URL's are picked at random
        foreach (Visit::all() as $v) {
            $slug = \substr($v->uri, 25);
            if (!isset($slugToId[$slug])) {
                $slugToId[$slug] = \count($slugList);
                $slugList[] = $slug;
            }
        }

        // ── Compute line-aligned chunk boundaries ──
        $edges = [0];
        $bh = \fopen($inputPath, 'r');
        for ($i = 1; $i < self::WORKER_COUNT; $i++) {
            \fseek($bh, (int) ($fileSize * $i / self::WORKER_COUNT));
            \fgets($bh);
            $edges[] = \ftell($bh);
        }
        \fclose($bh);
        $edges[] = $fileSize;

        // ── Fork workers ──
        $time = \microtime(true);
        $dir   = \sys_get_temp_dir();
        $myPid = \getmypid();
        $kids  = [];

        for ($w = 1; $w < self::WORKER_COUNT; $w++) {
            $path = "{$dir}/rc_{$myPid}_{$w}";
            $pid  = \pcntl_fork();
            if ($pid === 0) {
                $result = $this->crunch(
                    $inputPath, $edges[$w], $edges[$w + 1],
                    $slugToId, $dateChars,  \count($slugList), $totalDates
                );
                \file_put_contents($path, \pack('C*', ...$result));
                exit(0);
            }
            $kids[] = [$pid, $path];
        }

        // Parent handles the last chunk directly
        $grid = $this->crunch(
            $inputPath,
            $edges[0],
            $edges[1],
            $slugToId, $dateChars, \count($slugList), $totalDates,
        );
        \fprintf(STDERR, "all chunks: %.2f seconds\n", \microtime(true) - $time);

        // ── Merge child results ──
        $time = \microtime(true);
        foreach ($kids as [$pid, $tmpPath]) {
            \pcntl_waitpid($pid, $status);
            $i = 0;
            foreach (\unpack('C*',\file_get_contents($tmpPath)) as $v) {
                $grid[$i++] += $v;
            }
            \unlink($tmpPath);
        }
        \fprintf(STDERR, "merge: %.2f seconds\n", microtime(true) - $time);


        // ── Stream JSON output ──
        $this->writeOutput($outputPath, $grid, $slugList, $dateLabels);
    }

    /**
     * Parse a byte-range of the CSV into a flat counts array.
     *
     * Instead of incrementing nested arrays on every line, we append a
     * compact 2-byte date-id to a per-slug string bucket.  At the end we
     * unpack each bucket in one shot — far fewer hash-table operations.
     */
    private function crunch(
        $file,
        $from,
        $to,
        $slugToId,
        $dateChars,
        $totalSlugs,
        $totalDates,
    ) {
        $bins = \array_fill(0, $totalSlugs, '');

        $fh = \fopen($file, 'r');
        \stream_set_read_buffer($fh, 0);
        \fseek($fh, $from);
        $left = $to - $from;

        while ($left > 0) {
            $chunk = \fread($fh, $left > 196608 ? 196608 : $left);
            $cLen  = \strlen($chunk);
            if ($cLen === 0) break;
            $left -= $cLen;

            $lastNl = \strrpos($chunk, "\n");
            if ($lastNl === false) break;

            // Rewind past the incomplete trailing line so the next
            // fread starts exactly at a line boundary — no copying.
            $overshoot = $cLen - $lastNl - 1;
            if ($overshoot > 0) {
                \fseek($fh, -$overshoot, \SEEK_CUR);
                $left += $overshoot;
            }

            for ($p = 0; $p < $lastNl; ) {
                $nl = \strpos($chunk, "\n", $p + 52);
                if ($nl === false) break;
                $bins[$slugToId[\substr($chunk, $p + 25, $nl - $p - 51)]]
                    .= $dateChars[\substr($chunk, $nl - 23, 8)];
                $p = $nl + 1;
            }
        }

        \fclose($fh);

        // Tally: unpack each slug's bucket of 2-byte date ids into counts
        $grid = \array_fill(0, $totalSlugs * $totalDates, 0);
        foreach ($bins as $s => $data) {
            if ($data === '') continue;
            $offset = $s * $totalDates;
            foreach (\unpack('v*', $data) as $did) {
                $grid[$offset + $did]++;
            }
        }

        return $grid;
    }

    /**
     * Stream well-formatted JSON without json_encode overhead.
     * Dates come out sorted automatically because the id space is
     * chronological (year 2020 → 2026).
     */
    private function writeOutput(
        $outputPath,
        $grid,
        $slugList,
        $dateLabels
    ) {
        $time = microtime(true);
        $fh = \fopen($outputPath, 'wb');
        \stream_set_write_buffer($fh, 1 << 16);

        // Pre-format the repeating pieces once
        $dates = [];
        foreach ($dateLabels as $d) {
            $dates[] = "        \"20{$d}\": ";
        }
        $totalDates = \count($dateLabels);

        $slugs = [];
        foreach ($slugList as $s) {
            $slugs[] = '"\\/blog\\/' . \str_replace('/', '\\/', $s) . '"';
        }

        \fwrite($fh, '{');
        $prefix = '';
        $body = '';
        $comma = '';
        $n = 0;
        $i = 0;
        foreach ($slugs as $s => $slug) {
            $body = '';
            $comma = '';

            foreach ($dates as $d => $date) {
                $n = $grid[$i++];
                if ($n === 0) continue;
                $body .= $comma . $date . $n;
                $comma = ",\n";
            }

            if ('' === $body) {
                continue;
            }

            \fwrite($fh, "{$prefix}\n    {$slug}: {\n{$body}\n    }");
            $prefix = ',';
        }
        \fwrite($fh, "\n}");
        \fclose($fh);
        \fprintf(STDERR, "writeOutput: %.2f seconds\n", microtime(true) - $time);
    }
}
