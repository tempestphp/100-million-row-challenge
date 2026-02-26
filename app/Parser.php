<?php

declare(strict_types=1);

namespace App;

use App\Commands\Visit;

use function array_chunk;
use function array_count_values;
use function array_fill;
use function chr;
use function count;
use function fclose;
use function fgets;
use function file_get_contents;
use function filesize;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function getmypid;
use function pack;
use function pcntl_fork;
use function pcntl_wait;
use function str_replace;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function sys_get_temp_dir;
use function unlink;
use function unpack;

use const SEEK_CUR;

final class Parser
{
    public function parse($inputPath, $outputPath)
    {
        gc_disable();

        $fileSize = filesize($inputPath);
        $workers = 12;

        // ─── Build date lookup (arithmetic, no mktime/date overhead) ───

        $dateLookup = [];
        $dateLabels = [];
        $numDates = 0;

        for ($y = 20; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => ($y % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $ms = ($m < 10 ? '0' : '') . $m;
                $ym = $y . '-' . $ms . '-';
                for ($d = 1; $d <= $maxD; $d++) {
                    $key = $ym . (($d < 10 ? '0' : '') . $d);
                    $dateLookup[$key] = $numDates;
                    $dateLabels[$numDates] = '20' . $key;
                    $numDates++;
                }
            }
        }

        // Encode date IDs as 2-byte packed chars for bucket accumulation
        $dateChars = [];
        foreach ($dateLookup as $key => $id) {
            $dateChars[$key] = chr($id & 0xFF) . chr($id >> 8);
        }

        // ─── Discover slugs from file sample + Visit::all() fallback ───

        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);
        $sample = fread($fh, $fileSize > 2_097_152 ? 2_097_152 : $fileSize);
        fclose($fh);

        $slugIndex = [];
        $slugLabels = [];
        $numSlugs = 0;

        $sampleEnd = strrpos($sample, "\n");
        $sp = 0;

        while ($sp < $sampleEnd) {
            $nl = strpos($sample, "\n", $sp + 52);
            if ($nl === false) {
                break;
            }

            $slug = substr($sample, $sp + 25, $nl - $sp - 51);

            if (!isset($slugIndex[$slug])) {
                $slugIndex[$slug] = $numSlugs;
                $slugLabels[$numSlugs] = $slug;
                $numSlugs++;
            }

            $sp = $nl + 1;
        }

        unset($sample);

        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, 25);
            if (!isset($slugIndex[$slug])) {
                $slugIndex[$slug] = $numSlugs;
                $slugLabels[$numSlugs] = $slug;
                $numSlugs++;
            }
        }

        // ─── Split file into newline-aligned chunks ───

        $bounds = [0];
        $fh = fopen($inputPath, 'rb');

        for ($i = 1; $i < $workers; $i++) {
            fseek($fh, (int) ($fileSize * $i / $workers));
            fgets($fh);
            $bounds[] = ftell($fh);
        }

        $bounds[] = $fileSize;
        fclose($fh);

        $numChunks = count($bounds) - 1;

        // ─── Fork children (0..N-2), parent takes last chunk ───

        $tmpDir = sys_get_temp_dir();
        $myPid = getmypid();
        $children = [];

        for ($w = 0; $w < $numChunks - 1; $w++) {
            $tmpFile = $tmpDir . '/parse_' . $myPid . '_' . $w;
            $pid = pcntl_fork();

            if ($pid === 0) {
                $result = $this->crunch(
                    $inputPath, $bounds[$w], $bounds[$w + 1],
                    $slugIndex, $dateChars, $numSlugs, $numDates,
                );
                $wfh = fopen($tmpFile, 'wb');
                foreach (array_chunk($result, 8192) as $batch) {
                    fwrite($wfh, pack('V*', ...$batch));
                }
                fclose($wfh);
                exit(0);
            }

            $children[] = [$pid, $tmpFile];
        }

        // Parent crunches last chunk (children get a head start)
        $tally = $this->crunch(
            $inputPath, $bounds[$numChunks - 1], $bounds[$numChunks],
            $slugIndex, $dateChars, $numSlugs, $numDates,
        );

        // ─── Merge child results (as each finishes) ───

        $childMap = [];
        foreach ($children as [$pid, $tmpFile]) {
            $childMap[$pid] = $tmpFile;
        }

        $pending = count($childMap);
        while ($pending > 0) {
            $pid = pcntl_wait($status);
            if (isset($childMap[$pid])) {
                $j = 0;
                foreach (unpack('V*', file_get_contents($childMap[$pid])) as $v) {
                    $tally[$j++] += $v;
                }
                unlink($childMap[$pid]);
                $pending--;
            }
        }
        // ─── Emit JSON ───

        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 524_288);

        // Pre-compute formatted date prefixes and escaped paths
        $datePrefixes = [];
        for ($d = 0; $d < $numDates; $d++) {
            $datePrefixes[$d] = '        "' . $dateLabels[$d] . '": ';
        }

        $escapedPaths = [];
        for ($s = 0; $s < $numSlugs; $s++) {
            $escapedPaths[$s] = '"\\/blog\\/' . str_replace('/', '\\/', $slugLabels[$s]) . '"';
        }

        fwrite($out, '{');
        $firstSlug = true;

        for ($s = 0; $s < $numSlugs; $s++) {
            $base = $s * $numDates;
            $buf = '';
            $sep = '';

            for ($d = 0; $d < $numDates; $d++) {
                $n = $tally[$base + $d];
                if ($n === 0) {
                    continue;
                }
                $buf .= $sep . $datePrefixes[$d] . $n;
                $sep = ",\n";
            }

            if ($buf === '') {
                continue;
            }

            fwrite($out, ($firstSlug ? '' : ',') . "\n    " . $escapedPaths[$s] . ": {\n" . $buf . "\n    }");
            $firstSlug = false;
        }

        fwrite($out, "\n}");
        fclose($out);
    }

    /**
     * Parse a byte range using bucket accumulation for cache-friendly counting.
     */
    private function crunch(
        $path, $from, $until,
        $slugIndex, $dateChars, $numSlugs, $numDates,
    ) {
        $buckets = array_fill(0, $numSlugs, '');
        $fh = fopen($path, 'rb');
        stream_set_read_buffer($fh, 0);
        fseek($fh, $from);

        $remaining = $until - $from;
        $bufSize = 4_194_304;

        while ($remaining > 0) {
            $raw = fread($fh, $remaining > $bufSize ? $bufSize : $remaining);
            $len = strlen($raw);
            if ($len === 0) {
                break;
            }
            $remaining -= $len;

            $end = strrpos($raw, "\n");
            if ($end === false) {
                break;
            }

            $tail = $len - $end - 1;
            if ($tail > 0) {
                fseek($fh, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $p = 0;
            $fence = $end - 480;

            while ($p < $fence) {
                $nl = strpos($raw, "\n", $p + 52);
                $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateChars[substr($raw, $nl - 23, 8)];
                $p = $nl + 1;

                $nl = strpos($raw, "\n", $p + 52);
                $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateChars[substr($raw, $nl - 23, 8)];
                $p = $nl + 1;

                $nl = strpos($raw, "\n", $p + 52);
                $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateChars[substr($raw, $nl - 23, 8)];
                $p = $nl + 1;

                $nl = strpos($raw, "\n", $p + 52);
                $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateChars[substr($raw, $nl - 23, 8)];
                $p = $nl + 1;
            }

            while ($p < $end) {
                $nl = strpos($raw, "\n", $p + 52);
                if ($nl === false) {
                    break;
                }
                $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]] .= $dateChars[substr($raw, $nl - 23, 8)];
                $p = $nl + 1;
            }
        }

        fclose($fh);

        // Convert buckets → flat counts array
        $counts = array_fill(0, $numSlugs * $numDates, 0);

        for ($s = 0; $s < $numSlugs; $s++) {
            if ($buckets[$s] === '') {
                continue;
            }
            $base = $s * $numDates;
            foreach (array_count_values(unpack('v*', $buckets[$s])) as $dateId => $count) {
                $counts[$base + $dateId] += $count;
            }
        }

        return $counts;
    }
}
