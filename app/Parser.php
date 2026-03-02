<?php

declare(strict_types=1);

namespace App;

use App\Commands\Visit;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();
        ini_set('memory_limit', '-1');

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
            $c = strpos($scanBuf, ",", $scanPos + 29);
            if ($c === false) break;
            $century = substr($scanBuf, $c + 1, 2);
            $yy = (int)substr($scanBuf, $c + 3, 2);
            if ($yy < $minYear) $minYear = $yy;
            if ($yy > $maxYear) $maxYear = $yy;
            $scanPos = $c + 27;
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

        $sampleEnd = strrpos($sample, "\n") ?: strlen($sample);
        $sp = 0;

        while ($sp < $sampleEnd) {
            $c = strpos($sample, ",", $sp + 29);
            if ($c === false) break;
            $slug = substr($sample, $sp + 25, $c - $sp - 25);
            if (!isset($slugIndex[$slug])) {
                $slugIndex[$slug] = $numSlugs;
                $slugLabels[$numSlugs] = $slug;
                $numSlugs++;
            }
            $sp = $c + 27;
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

        $pipes = [];
        $childPids = [];

        for ($w = 0; $w < $numChunks - 1; $w++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            $pid = pcntl_fork();

            if ($pid === 0) {
                fclose($pair[0]);
                $buckets = $this->crunch(
                    $inputPath, $bounds[$w], $bounds[$w + 1],
                    $slugIndex, $dateChars, $numSlugs,
                );

                // Convert buckets to (slugId, dateId, count) tuples
                $entries = [];
                for ($s = 0; $s < $numSlugs; $s++) {
                    if ($buckets[$s] === '') continue;
                    $dateCounts = array_count_values(unpack('v*', $buckets[$s]));
                    foreach ($dateCounts as $dateId => $count) {
                        $entries[] = $s;
                        $entries[] = $dateId;
                        $entries[] = $count;
                    }
                }
                $buckets = null;

                $blob = pack('V*', ...$entries);
                $entries = null;

                $written = 0;
                $blobLen = strlen($blob);
                while ($written < $blobLen) {
                    $chunk = fwrite($pair[1], substr($blob, $written, 1_048_576));
                    if ($chunk === false || $chunk === 0) break;
                    $written += $chunk;
                }
                fclose($pair[1]);
                exit(0);
            }

            fclose($pair[1]);
            $pipes[$pid] = $pair[0];
            $childPids[] = $pid;
        }

        $buckets = $this->crunch(
            $inputPath, $bounds[$numChunks - 1], $bounds[$numChunks],
            $slugIndex, $dateChars, $numSlugs,
        );

        $result = array_fill(0, $numSlugs, []);
        for ($s = 0; $s < $numSlugs; $s++) {
            if ($buckets[$s] === '') continue;
            $result[$s] = array_count_values(unpack('v*', $buckets[$s]));
        }
        $buckets = null;

        foreach ($childPids as $pid) {
            $blob = stream_get_contents($pipes[$pid]);
            fclose($pipes[$pid]);

            $vals = unpack('V*', $blob);
            $n = count($vals);
            for ($i = 1; $i <= $n; $i += 3) {
                $s = $vals[$i];
                $d = $vals[$i + 1];
                if (isset($result[$s][$d])) {
                    $result[$s][$d] += $vals[$i + 2];
                } else {
                    $result[$s][$d] = $vals[$i + 2];
                }
            }
            unset($blob, $vals);
        }

        while (pcntl_wait($status) > 0) {}

        $this->writeJson($result, $numSlugs, $numDates, $slugLabels, $dateLabels, $outputPath);
    }

    private function crunch(
        string $path, int $from, int $until,
        array $slugIndex, array $dateChars, int $numSlugs,
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
                $c = strpos($raw, ",", $p + 29);
                $buckets[$slugIndex[substr($raw, $p + 25, $c - $p - 25)]]
                    .= $dateChars[substr($raw, $c + 3, 8)];
                $p = $c + 27;

                $c = strpos($raw, ",", $p + 29);
                $buckets[$slugIndex[substr($raw, $p + 25, $c - $p - 25)]]
                    .= $dateChars[substr($raw, $c + 3, 8)];
                $p = $c + 27;

                $c = strpos($raw, ",", $p + 29);
                $buckets[$slugIndex[substr($raw, $p + 25, $c - $p - 25)]]
                    .= $dateChars[substr($raw, $c + 3, 8)];
                $p = $c + 27;

                $c = strpos($raw, ",", $p + 29);
                $buckets[$slugIndex[substr($raw, $p + 25, $c - $p - 25)]]
                    .= $dateChars[substr($raw, $c + 3, 8)];
                $p = $c + 27;
            }

            while ($p < $end) {
                $c = strpos($raw, ",", $p + 29);
                if ($c === false) break;
                $buckets[$slugIndex[substr($raw, $p + 25, $c - $p - 25)]]
                    .= $dateChars[substr($raw, $c + 3, 8)];
                $p = $c + 27;
            }
        }

        fclose($fh);
        return $buckets;
    }

    private function writeJson(
        array &$result, int $numSlugs, int $numDates,
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
            if (empty($result[$s])) continue;

            $dateCounts = &$result[$s];
            $buf = '';
            $sep = '';
            for ($d = 0; $d < $numDates; $d++) {
                if (!isset($dateCounts[$d])) continue;
                $buf .= $sep . $datePrefixes[$d] . $dateCounts[$d];
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
        $env = getenv('PARSER_WORKERS');
        if ($env !== false && (int)$env > 0) return (int)$env;

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
