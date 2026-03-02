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
use function sem_acquire;
use function sem_get;
use function sem_release;
use function sem_remove;
use function shmop_delete;
use function shmop_open;
use function shmop_read;
use function shmop_write;
use function str_replace;
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
    private const int K0 = 163_840;
    private const int K1   = 2_097_152;
    private const int K2  = 25;
    private const int K3     = 8;
    private const int K4       = 16;

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
        $workerTotal = self::K3;
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
        $raw = fread($handle, min(self::K1, $inputBytes));
        fclose($handle);

        $slugIdByKey   = [];
        $slugKeyById     = [];
        $slugTotal = 0;
        $pos       = 0;
        $lastNl    = strrpos($raw, "\n") ?: 0;

        while ($pos < $lastNl) {
            $nl = strpos($raw, "\n", $pos + 52);
            if ($nl === false) break;

            $slug = substr($raw, $pos + self::K2, $nl - $pos - 51);

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
            $slug = substr($visit->uri, self::K2);
            if (!isset($slugIdByKey[$slug])) {
                $slugIdByKey[$slug]    = $slugTotal;
                $slugKeyById[$slugTotal] = $slug;
                $slugTotal++;
            }
        }
        $markPhase('visit-merge');

        $numChunks    = self::K4;
        $chunkOffsets = [0];
        $bh = fopen($inputPath, 'rb');
        for ($i = 1; $i < $numChunks; $i++) {
            fseek($bh, intdiv($inputBytes * $i, $numChunks));
            fgets($bh);
            $chunkOffsets[] = ftell($bh);
        }
        fclose($bh);
        $chunkOffsets[] = $inputBytes;
        $markPhase('chunk-offsets');

        $myPid       = getmypid();
        $semKey      = $myPid + 1;
        $queueShmKey = $myPid + 2;

        $sem      = sem_get($semKey, 1, 0644, true);
        $queueShm = shmop_open($queueShmKey, 'c', 0644, 4);
        shmop_write($queueShm, pack('V', 0), 0);

        $shmSegSize = $slugTotal * $dateCount * 2;
        $shmHandles = [];

        for ($w = 0; $w < $workerTotal - 1; $w++) {
            $shmHandles[$w] = shmop_open($myPid * 100 + $w, 'c', 0644, $shmSegSize);
        }

        $childMap = [];

        for ($w = 0; $w < $workerTotal - 1; $w++) {
            $pid = pcntl_fork();
            if ($pid === -1) throw new \RuntimeException('pcntl_fork failed');

            if ($pid === 0) {
                $buckets = array_fill(0, $slugTotal, '');
                $fh      = fopen($inputPath, 'rb');
                stream_set_read_buffer($fh, 0);

                while (($ci = self::q0($queueShm, $sem, $numChunks)) !== -1) {
                    self::q2($fh, $chunkOffsets[$ci], $chunkOffsets[$ci + 1], $slugIdByKey, $dayIdTokens, $buckets);
                }

                fclose($fh);

                $counts = self::q3($buckets, $slugTotal, $dateCount);
                shmop_write($shmHandles[$w], pack('v*', ...$counts), 0);

                exit(0);
            }

            $childMap[$pid] = $w;
        }

        $buckets = array_fill(0, $slugTotal, '');
        $fh      = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);

        while (($ci = self::q0($queueShm, $sem, $numChunks)) !== -1) {
            self::q2($fh, $chunkOffsets[$ci], $chunkOffsets[$ci + 1], $slugIdByKey, $dayIdTokens, $buckets);
        }

        fclose($fh);

        $counts = self::q3($buckets, $slugTotal, $dateCount);

        while ($childMap) {
            $pid = pcntl_wait($status);
            if (!isset($childMap[$pid])) continue;

            $w = $childMap[$pid];
            unset($childMap[$pid]);

            $packed = shmop_read($shmHandles[$w], 0, $shmSegSize);
            shmop_delete($shmHandles[$w]);

            $j = 0;
            foreach (unpack('v*', $packed) as $v) {
                $counts[$j] += $v;
                $j++;
            }
        }
        shmop_delete($queueShm);
        sem_remove($sem);
        $markPhase('parse-and-reduce');

        self::q4($outputPath, $counts, $slugKeyById, $dayKeyById, $dateCount);
        $markPhase('json-output');

        $dumpPhases($planId, $workerTotal, $workerTotal);
    }

    private static function q0($queueShm, $sem, $numChunks)
    {
        sem_acquire($sem);
        $idx = unpack('V', shmop_read($queueShm, 0, 4))[1];
        if ($idx >= $numChunks) {
            sem_release($sem);
            return -1;
        }
        shmop_write($queueShm, pack('V', $idx + 1), 0);
        sem_release($sem);
        return $idx;
    }

    private static function q2($handle, $start, $end, $slugIdByKey, $dayIdTokens, &$buckets)
    {
        fseek($handle, $start);

        $remaining = $end - $start;
        $bufSize   = self::K0;
        $prefixLen = self::K2;

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
            $fence = $lastNl - 792;

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

    private static function q3(&$buckets, $slugTotal, $dateCount)
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

    private static function q4($outputPath, $counts, $slugKeyById, $dayKeyById, $dateCount)
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