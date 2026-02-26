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
use function filesize;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function pack;
use function pcntl_fork;
use function pcntl_waitpid;
use function array_search;
use function array_values;
use function feof;
use function str_replace;
use function stream_select;
use function stream_set_blocking;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function stream_socket_pair;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function unpack;

use const SEEK_CUR;
use const STREAM_IPPROTO_IP;
use const STREAM_PF_UNIX;
use const STREAM_SOCK_STREAM;

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

        // ─── Fork children with socket pairs for IPC ───

        $pipes = [];
        $childPids = [];

        for ($w = 0; $w < $numChunks - 1; $w++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            $pid = pcntl_fork();

            if ($pid === 0) {
                fclose($pair[0]);
                $result = $this->crunch(
                    $inputPath, $bounds[$w], $bounds[$w + 1],
                    $slugIndex, $dateChars, $numSlugs, $numDates,
                );
                $v16 = max($result) <= 65535;
                fwrite($pair[1], $v16 ? "\x00" : "\x01");
                $fmt = $v16 ? 'v*' : 'V*';
                foreach (array_chunk($result, 8192) as $batch) {
                    fwrite($pair[1], pack($fmt, ...$batch));
                }
                fclose($pair[1]);
                exit(0);
            }

            fclose($pair[1]);
            $pipes[$w] = $pair[0];
            $childPids[] = $pid;
        }

        // Parent crunches last chunk (children get a head start)
        $tally = $this->crunch(
            $inputPath, $bounds[$numChunks - 1], $bounds[$numChunks],
            $slugIndex, $dateChars, $numSlugs, $numDates,
        );

        // ─── Merge child results via stream_select (concurrent drain) ───

        $buffers = [];
        $open = [];
        foreach ($pipes as $k => $pipe) {
            stream_set_blocking($pipe, false);
            $buffers[$k] = '';
            $open[$k] = $pipe;
        }

        $stallCount = 0;
        while ($open) {
            $read = array_values($open);
            $w = null;
            $e = null;
            $ready = stream_select($read, $w, $e, 5);

            if ($ready === 0) {
                if (++$stallCount > 6) {
                    break; // 30s with no progress — fail fast
                }
                continue;
            }
            $stallCount = 0;

            foreach ($read as $pipe) {
                $k = array_search($pipe, $open, true);
                $data = fread($pipe, 131072);
                if ($data !== '' && $data !== false) {
                    $buffers[$k] .= $data;
                }
                if (feof($pipe)) {
                    $raw = $buffers[$k];
                    fclose($pipe);
                    unset($open[$k], $buffers[$k]);

                    if ($raw === '') {
                        continue;
                    }

                    $rawLen = strlen($raw);
                    $isV16 = ord($raw[0]) === 0;
                    $fmt = $isV16 ? 'v*' : 'V*';
                    $step = $isV16 ? 16384 : 32768;
                    $j = 0;
                    for ($off = 1; $off < $rawLen; $off += $step) {
                        foreach (unpack($fmt, substr($raw, $off, $step)) as $v) {
                            $tally[$j++] += $v;
                        }
                    }
                }
            }
        }

        foreach ($childPids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        // ─── Emit JSON ───

        // Pre-compute formatted date prefixes and escaped paths
        $datePrefixes = [];
        for ($d = 0; $d < $numDates; $d++) {
            $datePrefixes[$d] = '        "' . $dateLabels[$d] . '": ';
        }

        $escapedPaths = [];
        for ($s = 0; $s < $numSlugs; $s++) {
            $escapedPaths[$s] = '"\\/blog\\/' . str_replace('/', '\\/', $slugLabels[$s]) . '"';
        }

        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 524_288);

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
