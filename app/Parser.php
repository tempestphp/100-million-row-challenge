<?php

namespace App;

use App\Commands\Visit;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();
        //ini_set('memory_limit', '-1');

        $fileSize = filesize($inputPath);
        $workers = $this->getCpuCount();

        $fhScan = fopen($inputPath, 'rb');
        $scanBuf = fread($fhScan, 8192);
        fclose($fhScan);

        $minYear = 99;
        $maxYear = 0;
        $century = '20';
        $scanPos = 0;
        $scanEnd = strrpos($scanBuf, "\n") ?: strlen($scanBuf);
        while ($scanPos < $scanEnd) {
            $scanNl = strpos($scanBuf, "\n", $scanPos + 52);
            if ($scanNl === false) break;
            $century = substr($scanBuf, $scanNl - 25, 2);
            $yy = (int)substr($scanBuf, $scanNl - 23, 2);
            if ($yy < $minYear) $minYear = $yy;
            if ($yy > $maxYear) $maxYear = $yy;
            $scanPos = $scanNl + 1;
        }
        unset($scanBuf);

        $minYear = max(0, $minYear - 1);
        $maxYear = min(99, $maxYear + 1);

        $dateLookup = [];
        $dateLabels = [];
        $numDates = 0;

        for ($y = $minYear; $y <= $maxYear; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => ($y % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $ys = $y < 10 ? "0{$y}" : (string)$y;
                $ms = $m < 10 ? "0{$m}" : (string)$m;
                $ym = "{$ys}-{$ms}-";
                for ($d = 1; $d <= $maxD; $d++) {
                    $ds = $d < 10 ? "0{$d}" : (string)$d;
                    $key = $ym . $ds;
                    $dateLookup[$key] = $numDates;
                    $dateLabels[$numDates] = $century . $key;
                    $numDates++;
                }
            }
        }

        $dateChars = [];
        foreach ($dateLookup as $key => $id) {
            $dateChars[$key] = chr($id & 0xFF) . chr($id >> 8);
        }

        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);
        $sampleSize = min($fileSize, 4_194_304);
        $sample = fread($fh, $sampleSize);
        fclose($fh);

        $slugIndex = [];
        $slugLabels = [];
        $numSlugs = 0;

        $sampleEnd = strrpos($sample, "\n");
        $sp = 0;

        while ($sp < $sampleEnd) {
            $nl = strpos($sample, "\n", $sp + 52);
            if ($nl === false) break;
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

        $bounds = [0];
        $fh = fopen($inputPath, 'rb');
        for ($i = 1; $i < $workers; $i++) {
            fseek($fh, (int)($fileSize * $i / $workers));
            fgets($fh);
            $bounds[] = ftell($fh);
        }
        $bounds[] = $fileSize;
        fclose($fh);

        $numChunks = count($bounds) - 1;

        $tmpDir = sys_get_temp_dir();
        $myPid = getmypid();
        $childMap = [];

        for ($w = 0; $w < $numChunks - 1; $w++) {
            $tmpFile = $tmpDir . '/p_' . $myPid . '_' . $w;
            $pid = pcntl_fork();

            if ($pid === 0) {
                $result = $this->crunch(
                    $inputPath, $bounds[$w], $bounds[$w + 1],
                    $slugIndex, $dateChars, $numSlugs, $numDates,
                );
                $wfh = fopen($tmpFile, 'wb');
                fwrite($wfh, pack('V*', ...$result));
                fclose($wfh);
                exit(0);
            }

            $childMap[$pid] = $tmpFile;
        }

        $tally = $this->crunch(
            $inputPath, $bounds[$numChunks - 1], $bounds[$numChunks],
            $slugIndex, $dateChars, $numSlugs, $numDates,
        );

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

        $this->writeJson($tally, $numSlugs, $numDates, $slugLabels, $dateLabels, $outputPath);
    }

    private function crunch(
        string $path, int $from, int $until,
        array $slugIndex, array $dateChars, int $numSlugs, int $numDates,
    ): array {
        $buckets = array_fill(0, $numSlugs, '');

        $fh = fopen($path, 'rb');
        stream_set_read_buffer($fh, 0);
        fseek($fh, $from);

        $remaining = $until - $from;
        $bufSize = 8_388_608;

        while ($remaining > 0) {
            $raw = fread($fh, $remaining > $bufSize ? $bufSize : $remaining);
            $len = strlen($raw);
            if ($len === 0) break;
            $remaining -= $len;

            $end = strrpos($raw, "\n");
            if ($end === false) break;

            $tail = $len - $end - 1;
            if ($tail > 0) {
                fseek($fh, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $p = 0;
            $fence = $end - 480;

            while ($p < $fence) {
                $nl = strpos($raw, "\n", $p + 52);
                $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]]
                    .= $dateChars[substr($raw, $nl - 23, 8)];
                $p = $nl + 1;

                $nl = strpos($raw, "\n", $p + 52);
                $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]]
                    .= $dateChars[substr($raw, $nl - 23, 8)];
                $p = $nl + 1;

                $nl = strpos($raw, "\n", $p + 52);
                $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]]
                    .= $dateChars[substr($raw, $nl - 23, 8)];
                $p = $nl + 1;

                $nl = strpos($raw, "\n", $p + 52);
                $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]]
                    .= $dateChars[substr($raw, $nl - 23, 8)];
                $p = $nl + 1;
            }

            while ($p < $end) {
                $nl = strpos($raw, "\n", $p + 52);
                if ($nl === false) break;
                $buckets[$slugIndex[substr($raw, $p + 25, $nl - $p - 51)]]
                    .= $dateChars[substr($raw, $nl - 23, 8)];
                $p = $nl + 1;
            }
        }

        fclose($fh);

        $counts = array_fill(0, $numSlugs * $numDates, 0);

        for ($s = 0; $s < $numSlugs; $s++) {
            if ($buckets[$s] === '') continue;
            $base = $s * $numDates;
            foreach (array_count_values(unpack('v*', $buckets[$s])) as $dateId => $count) {
                $counts[$base + $dateId] = $count;
            }
        }

        return $counts;
    }

    private function writeJson(
        array &$tally, int $numSlugs, int $numDates,
        array $slugLabels, array $dateLabels, string $outputPath,
    ): void {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);

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
                if ($n === 0) continue;
                $buf .= $sep . $datePrefixes[$d] . $n;
                $sep = ",\n";
            }

            if ($buf === '') continue;

            fwrite($out,
                ($firstSlug ? '' : ',')
                . "\n    " . $escapedPaths[$s]
                . ": {\n" . $buf . "\n    }"
            );
            $firstSlug = false;
        }

        fwrite($out, "\n}");
        fclose($out);
    }

    private function getCpuCount(): int
    {
        if (is_readable('/proc/cpuinfo')) {
            $cpuinfo = file_get_contents('/proc/cpuinfo');
            preg_match_all('/^processor/m', $cpuinfo, $matches);
            if (count($matches[0]) > 0) return count($matches[0]);
        }
        $result = @shell_exec('sysctl -n hw.ncpu 2>/dev/null');
        if ($result !== null && $result !== false) {
            $count = (int)trim($result);
            if ($count > 0) return $count;
        }
        return 8;
    }
}
