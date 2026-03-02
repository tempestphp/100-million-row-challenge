<?php

namespace App;

use App\Commands\Visit;

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
use function getmypid;
use function implode;
use function intdiv;
use function min;
use function pack;
use function pcntl_fork;
use function pcntl_wait;
use function str_replace;
use function stream_socket_pair;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function unpack;

use const SEEK_CUR;

final class Parser
{
    private const int READ_BLOCK_BYTES = 163_840;
    private const int SLUG_SCAN_BYTES   = 2_097_152;
    private const int URL_PREFIX_BYTES  = 25;
    private const int PROCESS_COUNT     = 8;

    public function parse($inputPath, $outputPath)
    {
        $runStartNs = \hrtime(true);
        $profileEnabled = (\getenv('PARSER_PROFILE') === '1');
        $phaseStartNs = $runStartNs;
        $phaseMarks = [];
        $markPhase = static function (string $name) use (&$phaseMarks, &$phaseStartNs, $runStartNs, $profileEnabled): void {
            if (! $profileEnabled) {
                return;
            }

            $now = \hrtime(true);
            $phaseMarks[] = [
                'name' => $name,
                'delta_ms' => ($now - $phaseStartNs) / 1_000_000,
                'total_ms' => ($now - $runStartNs) / 1_000_000,
            ];
            $phaseStartNs = $now;
        };
        $dumpPhases = static function (string $planId, int $workerTotal, int $chunkTotal) use (&$phaseMarks, $profileEnabled): void {
            if (! $profileEnabled) {
                return;
            }

            \fwrite(STDERR, "[parser-profile] plan={$planId} workers={$workerTotal} chunks={$chunkTotal}\n");
            foreach ($phaseMarks as $mark) {
                \fwrite(
                    STDERR,
                    \sprintf(
                        "  %-24s delta=%8.3f ms total=%8.3f ms\n",
                        $mark['name'],
                        $mark['delta_ms'],
                        $mark['total_ms'],
                    ),
                );
            }
        };

        gc_disable();

        $inputBytes   = filesize($inputPath);
        $workerTotal = self::PROCESS_COUNT;
        $planId      = 'default';

        $dayIdByKey   = [];
        $dayKeyById     = [];
        $dateCount = 0;

        for ($y = 21; $y <= 26; $y++) {
            $yStr = (string)$y;
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2           => $y === 24 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default     => 31,
                };
                $mStr  = ($m < 10 ? '0' : '') . $m;
                $ymStr = $yStr . '-' . $mStr . '-';
                for ($d = 1; $d <= $maxD; $d++) {
                    $key               = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $dayIdByKey[$key]     = $dateCount;
                    $dayKeyById[$dateCount] = $key;
                    $dateCount++;
                }
            }
        }

        $dayIdTokens = [];
        foreach ($dayIdByKey as $date => $id) {
            $dayIdTokens[$date] = chr($id & 0xFF) . chr($id >> 8);
        }
        $markPhase('date-maps');

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $raw = fread($handle, min(self::SLUG_SCAN_BYTES, $inputBytes));
        fclose($handle);

        $slugIdByKey   = [];
        $slugKeyById     = [];
        $slugTotal = 0;
        $pos       = 0;
        $lastNl    = strrpos($raw, "\n") ?: 0;

        while ($pos < $lastNl) {
            $nl = strpos($raw, "\n", $pos + 52);
            if ($nl === false) break;

            $slug = substr($raw, $pos + self::URL_PREFIX_BYTES, $nl - $pos - 51);

            if (!isset($slugIdByKey[$slug])) {
                $slugIdByKey[$slug]    = $slugTotal;
                $slugKeyById[$slugTotal] = $slug;
                $slugTotal++;
            }

            $pos = $nl + 1;
        }
        unset($raw);
        $markPhase('slug-scan');

        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, self::URL_PREFIX_BYTES);
            if (!isset($slugIdByKey[$slug])) {
                $slugIdByKey[$slug]    = $slugTotal;
                $slugKeyById[$slugTotal] = $slug;
                $slugTotal++;
            }
        }
        $markPhase('visit-merge');

        $chunkOffsets = [0];
        $bh = fopen($inputPath, 'rb');
        for ($i = 1; $i < $workerTotal; $i++) {
            fseek($bh, intdiv($inputBytes * $i, $workerTotal));
            fgets($bh);
            $chunkOffsets[] = ftell($bh);
        }
        fclose($bh);
        $chunkOffsets[] = $inputBytes;
        $markPhase('chunk-offsets');

        $sockets  = [];
        $childMap = [];

        for ($w = 0; $w < $workerTotal - 1; $w++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
            $pid  = pcntl_fork();
            if ($pid === -1) throw new \RuntimeException('pcntl_fork failed');

            if ($pid === 0) {
                fclose($pair[0]);

                $buckets = array_fill(0, $slugTotal, '');
                $fh      = fopen($inputPath, 'rb');
                stream_set_read_buffer($fh, 0);

                self::consumeRangeIntoBuckets($fh, $chunkOffsets[$w], $chunkOffsets[$w + 1], $slugIdByKey, $dayIdTokens, $buckets);

                fclose($fh);

                $counts = self::reduceBucketsToCounts($buckets, $slugTotal, $dateCount);
                $packed = pack('v*', ...$counts);

                fwrite($pair[1], $packed);
                fclose($pair[1]);

                exit(0);
            }

            fclose($pair[1]);
            $sockets[$pid] = $pair[0];
            $childMap[$pid] = $w;
        }

        $parentIdx = $workerTotal - 1;
        $buckets   = array_fill(0, $slugTotal, '');
        $fh        = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);

        self::consumeRangeIntoBuckets($fh, $chunkOffsets[$parentIdx], $chunkOffsets[$parentIdx + 1], $slugIdByKey, $dayIdTokens, $buckets);

        fclose($fh);

        $counts = self::reduceBucketsToCounts($buckets, $slugTotal, $dateCount);
        $n      = $slugTotal * $dateCount;

        // Read all child data via stream_select (prevents deadlock from full socket buffers)
        $childData = [];
        $remaining = $sockets; // [pid => stream]

        while ($remaining) {
            $read = array_values($remaining);
            $w = null;
            $e = null;
            stream_select($read, $w, $e, 300);

            foreach ($read as $sock) {
                $pid = array_search($sock, $remaining, true);
                $chunk = fread($sock, 131072);
                if ($chunk === '' || $chunk === false) {
                    fclose($sock);
                    unset($remaining[$pid]);
                } else {
                    $childData[$pid] = ($childData[$pid] ?? '') . $chunk;
                }
            }
        }

        // Reap children
        while ($childMap) {
            $pid = pcntl_wait($status);
            if (isset($childMap[$pid])) unset($childMap[$pid]);
        }

        // Merge child counts
        foreach ($childData as $packed) {
            $childCounts = unpack('v*', $packed);
            for ($j = 0, $k = 1; $j < $n; $j++, $k++) {
                if ($v = $childCounts[$k]) {
                    $counts[$j] += $v;
                }
            }
        }
        $markPhase('parse-and-reduce');

        self::flushJsonOutput($outputPath, $counts, $slugKeyById, $dayKeyById, $dateCount);
        $markPhase('json-output');

        $dumpPhases($planId, $workerTotal, $workerTotal);
    }

    private static function consumeRangeIntoBuckets($handle, $start, $end, $slugIdByKey, $dayIdTokens, &$buckets)
    {
        fseek($handle, $start);

        $remaining = $end - $start;
        $bufSize   = self::READ_BLOCK_BYTES;
        $prefixLen = self::URL_PREFIX_BYTES;

        while ($remaining > 0) {
            $toRead = $remaining > $bufSize ? $bufSize : $remaining;
            $chunk  = fread($handle, $toRead);
            if (!$chunk) break;

            $chunkLen   = strlen($chunk);
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) continue;

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $p     = $prefixLen;
            $fence = $lastNl - 594;

            while ($p < $fence) {
                $sep = strpos($chunk, ',', $p);
                $buckets[$slugIdByKey[substr($chunk, $p, $sep - $p)]] .= $dayIdTokens[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$slugIdByKey[substr($chunk, $p, $sep - $p)]] .= $dayIdTokens[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$slugIdByKey[substr($chunk, $p, $sep - $p)]] .= $dayIdTokens[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$slugIdByKey[substr($chunk, $p, $sep - $p)]] .= $dayIdTokens[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$slugIdByKey[substr($chunk, $p, $sep - $p)]] .= $dayIdTokens[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$slugIdByKey[substr($chunk, $p, $sep - $p)]] .= $dayIdTokens[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;
            }

            while ($p < $lastNl) {
                $sep = strpos($chunk, ',', $p);
                if ($sep === false || $sep >= $lastNl) break;
                $buckets[$slugIdByKey[substr($chunk, $p, $sep - $p)]] .= $dayIdTokens[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;
            }
        }
    }

    private static function reduceBucketsToCounts(&$buckets, $slugTotal, $dateCount)
    {
        $counts = array_fill(0, $slugTotal * $dateCount, 0);
        $base   = 0;
        foreach ($buckets as $bucket) {
            if ($bucket !== '') {
                foreach (array_count_values(unpack('v*', $bucket)) as $did => $cnt) {
                    $counts[$base + $did] += $cnt;
                }
            }
            $base += $dateCount;
        }
        return $counts;
    }

    private static function flushJsonOutput($outputPath, $counts, $slugKeyById, $dayKeyById, $dateCount)
    {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);

        $slugTotal    = count($slugKeyById);
        $datePrefixes = [];
        $escapedPaths = [];

        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "20' . $dayKeyById[$d] . '": ';
        }

        for ($p = 0; $p < $slugTotal; $p++) {
            $escapedPaths[$p] = "\"\\/blog\\/" . str_replace('/', '\\/', $slugKeyById[$p]) . '"';
        }

        fwrite($out, '{');
        $firstPath = true;
        $base      = 0;

        for ($p = 0; $p < $slugTotal; $p++) {
            $dateEntries = [];

            for ($d = 0; $d < $dateCount; $d++) {
                $count = $counts[$base + $d];
                if ($count !== 0) {
                    $dateEntries[] = $datePrefixes[$d] . $count;
                }
            }

            if (empty($dateEntries)) {
                $base += $dateCount;
                continue;
            }

            $sep2      = $firstPath ? '' : ',';
            $firstPath = false;

            fwrite($out,
                $sep2 .
                "\n    " . $escapedPaths[$p] . ": {\n" .
                implode(",\n", $dateEntries) .
                "\n    }"
            );

            $base += $dateCount;
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}