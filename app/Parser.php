<?php

namespace App;

use App\Commands\Visit;
use function fopen;
use function fclose;
use function fread;
use function fseek;
use function fgets;
use function ftell;
use function fwrite;
use function filesize;
use function strlen;
use function substr;
use function strpos;
use function strrpos;
use function array_fill;
use function array_count_values;
use function chr;
use function unpack;
use function pack;
use function file_put_contents;
use function file_get_contents;
use function str_replace;
use function implode;
use function pcntl_fork;
use function pcntl_wait;
use function getmypid;
use function sys_get_temp_dir;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function unlink;
use function count;
use function flock;
use function fflush;
use function gc_disable;
use const LOCK_EX;
use const LOCK_UN;
use const SEEK_CUR;

final class Parser
{
    private const WORKERS = 8;
    private const CHUNKS = 16;
    private const READ_CHUNK = 163_840;

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();
        self::run($inputPath, $outputPath, filesize($inputPath));
    }

    private static function run(string $inputPath, string $outputPath, int $fileSize): void
    {
        $dateIds = [];
        $dates = [];
        $dateCount = 0;
        for ($y = 21; $y <= 26; $y++) {
            $yStr = (string) $y;
            for ($m = 1; $m <= 12; $m++) {
                $md = match ($m) { 2 => $y === 24 ? 29 : 28, 4, 6, 9, 11 => 30, default => 31 };
                $mStr = ($m < 10 ? '0' : '') . $m;
                $ymStr = $yStr . '-' . $mStr . '-';
                for ($d = 1; $d <= $md; $d++) {
                    $key = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $dateIds[$key] = $dateCount;
                    $dates[$dateCount] = $key;
                    $dateCount++;
                }
            }
        }

        $dateIdChars = [];
        foreach ($dateIds as $date => $id) {
            $dateIdChars[$date] = chr($id & 0xFF) . chr($id >> 8);
        }

        $f = fopen($inputPath, 'rb');
        stream_set_read_buffer($f, 0);
        $raw = fread($f, 2_097_152);
        fclose($f);

        $pathIds = [];
        $paths = [];
        $pathCount = 0;
        $pos = 0;
        $lastNl = strrpos($raw, "\n") ?: 0;
        while ($pos < $lastNl) {
            $nlPos = strpos($raw, "\n", $pos + 52);
            if ($nlPos === false) break;
            $slug = substr($raw, $pos + 25, $nlPos - $pos - 51);
            if (isset($pathIds[$slug])) {
                $pos = $nlPos + 1;
                continue;
            }
            $pathIds[$slug] = $pathCount;
            $paths[$pathCount] = $slug;
            $pathCount++;
            $pos = $nlPos + 1;
        }
        unset($raw);

        foreach (Visit::all() as $v) {
            $slug = substr($v->uri, 25);
            if (isset($pathIds[$slug])) continue;
            $pathIds[$slug] = $pathCount;
            $paths[$pathCount] = $slug;
            $pathCount++;
        }

        $numChunks = self::CHUNKS;
        $offsets = [0];
        $fh = fopen($inputPath, 'rb');
        for ($i = 1; $i < $numChunks; $i++) {
            fseek($fh, (int)($fileSize * $i / $numChunks));
            fgets($fh);
            $offsets[] = ftell($fh);
        }
        fclose($fh);
        $offsets[] = $fileSize;

        $me = getmypid();
        $tmp = sys_get_temp_dir();
        $w = self::WORKERS;
        $ch = [];
        $totalSize = $pathCount * $dateCount;
        $segSize = $totalSize * 2;

        $useShmop = false;
        $shm = [];
        if (function_exists('shmop_open')) {
            set_error_handler(static fn() => true);
            try {
                $test = \shmop_open(($me << 4) + 1, 'c', 0644, $segSize);
                if ($test instanceof \Shmop) {
                    $useShmop = true;
                    $shm[0] = $test;
                    for ($i = 1; $i < $w; $i++) {
                        $shm[$i] = \shmop_open(($me << 4) + $i + 1, 'c', 0644, $segSize);
                    }
                }
            } catch (\Throwable) {
                $useShmop = false;
                $shm = [];
            } finally {
                restore_error_handler();
            }
        }

        $useSem = false;
        $sem = null;
        $queueShm = null;
        $queueFile = '';

        if (function_exists('sem_get') && $useShmop) {
            set_error_handler(static fn() => true);
            try {
                $sem = \sem_get($me + 1, 1, 0644, true);
                $queueShm = \shmop_open($me + 2, 'c', 0644, 4);
                if ($sem !== false && $queueShm instanceof \Shmop) {
                    \shmop_write($queueShm, pack('V', 0), 0);
                    $useSem = true;
                }
            } catch (\Throwable) {
                $useSem = false;
            } finally {
                restore_error_handler();
            }
        }

        if (!$useSem) {
            $queueFile = "$tmp/q{$me}";
            file_put_contents($queueFile, pack('V', 0));
        }

        for ($i = 0; $i < $w; $i++) {
            $pid = pcntl_fork();
            if ($pid === 0) {
                $cnt = $useSem
                    ? self::workerLoopSem($inputPath, $queueShm, $sem, $offsets, $numChunks, $pathIds, $dateIdChars, $pathCount, $dateCount)
                    : self::workerLoopFlock($inputPath, $queueFile, $offsets, $numChunks, $pathIds, $dateIdChars, $pathCount, $dateCount);
                $packed = pack('v*', ...$cnt);
                if ($useShmop) {
                    \shmop_write($shm[$i], $packed, 0);
                } else {
                    file_put_contents("$tmp/p{$me}w$i", $packed);
                }
                exit(0);
            }
            $ch[$pid] = $i;
        }

        $cnt = array_fill(0, $totalSize, 0);
        $pending = $w;
        while ($pending > 0) {
            $pid = pcntl_wait($status);
            $idx = $ch[$pid];

            if ($useShmop) {
                $wc = unpack('v*', \shmop_read($shm[$idx], 0, $segSize));
                \shmop_delete($shm[$idx]);
            } else {
                $tf = "$tmp/p{$me}w$idx";
                $wc = unpack('v*', file_get_contents($tf));
                unlink($tf);
            }

            for ($j = 0, $k = 1; $j < $totalSize; $j++, $k++) {
                $cnt[$j] += $wc[$k];
            }
            $pending--;
        }

        if ($useSem) {
            \shmop_delete($queueShm);
            \sem_remove($sem);
        } else {
            unlink($queueFile);
        }

        self::writeJson($outputPath, $cnt, $paths, $dates, $dateCount);
    }

    private static function claimChunkSem(mixed $queueShm, mixed $sem, int $numChunks): int
    {
        \sem_acquire($sem);
        $ci = unpack('V', \shmop_read($queueShm, 0, 4))[1];
        if ($ci >= $numChunks) {
            \sem_release($sem);
            return -1;
        }
        \shmop_write($queueShm, pack('V', $ci + 1), 0);
        \sem_release($sem);
        return $ci;
    }

    private static function claimChunkFlock(mixed $qf, int $numChunks): int
    {
        flock($qf, LOCK_EX);
        fseek($qf, 0);
        $ci = unpack('V', fread($qf, 4))[1];
        if ($ci >= $numChunks) {
            flock($qf, LOCK_UN);
            return -1;
        }
        fseek($qf, 0);
        fwrite($qf, pack('V', $ci + 1));
        fflush($qf);
        flock($qf, LOCK_UN);
        return $ci;
    }

    private static function workerLoopSem(string $f, mixed $queueShm, mixed $sem, array $offsets, int $numChunks, array $pi, array $dc, int $pc, int $dateCount): array
    {
        $bk = array_fill(0, $pc, '');
        $h = fopen($f, 'rb');
        stream_set_read_buffer($h, 0);

        while (($ci = self::claimChunkSem($queueShm, $sem, $numChunks)) !== -1) {
            self::parseRange($h, $offsets[$ci], $offsets[$ci + 1], $pi, $dc, $bk);
        }

        fclose($h);

        $cnt = array_fill(0, $pc * $dateCount, 0);
        for ($i = 0; $i < $pc; $i++) {
            if ($bk[$i] === '') continue;
            $off = $i * $dateCount;
            foreach (array_count_values(unpack('v*', $bk[$i])) as $did => $c) $cnt[$off + $did] += $c;
        }
        return $cnt;
    }

    private static function workerLoopFlock(string $f, string $queueFile, array $offsets, int $numChunks, array $pi, array $dc, int $pc, int $dateCount): array
    {
        $bk = array_fill(0, $pc, '');
        $h = fopen($f, 'rb');
        stream_set_read_buffer($h, 0);
        $qf = fopen($queueFile, 'c+b');

        while (($ci = self::claimChunkFlock($qf, $numChunks)) !== -1) {
            self::parseRange($h, $offsets[$ci], $offsets[$ci + 1], $pi, $dc, $bk);
        }

        fclose($qf);
        fclose($h);

        $cnt = array_fill(0, $pc * $dateCount, 0);
        for ($i = 0; $i < $pc; $i++) {
            if ($bk[$i] === '') continue;
            $off = $i * $dateCount;
            foreach (array_count_values(unpack('v*', $bk[$i])) as $did => $c) $cnt[$off + $did] += $c;
        }
        return $cnt;
    }

    private static function parseRange(mixed $h, int $s, int $e, array $pi, array $dc, array &$bk): void
    {
        fseek($h, $s);
        $r = $e - $s;

        while ($r > 0) {
            $toRead = $r > self::READ_CHUNK ? self::READ_CHUNK : $r;
            $d = fread($h, $toRead);
            $l = strlen($d);
            $r -= $l;

            $ln = strrpos($d, "\n");
            if ($ln === false) break;

            $tail = $l - $ln - 1;
            if ($tail > 0) {
                fseek($h, -$tail, SEEK_CUR);
                $r += $tail;
            }

            $p = 25;
            $fc = $ln - 792;

            while ($p < $fc) {
                $x = strpos($d, ',', $p); $bk[$pi[substr($d, $p, $x - $p)]] .= $dc[substr($d, $x + 3, 8)]; $p = $x + 52;
                $x = strpos($d, ',', $p); $bk[$pi[substr($d, $p, $x - $p)]] .= $dc[substr($d, $x + 3, 8)]; $p = $x + 52;
                $x = strpos($d, ',', $p); $bk[$pi[substr($d, $p, $x - $p)]] .= $dc[substr($d, $x + 3, 8)]; $p = $x + 52;
                $x = strpos($d, ',', $p); $bk[$pi[substr($d, $p, $x - $p)]] .= $dc[substr($d, $x + 3, 8)]; $p = $x + 52;
                $x = strpos($d, ',', $p); $bk[$pi[substr($d, $p, $x - $p)]] .= $dc[substr($d, $x + 3, 8)]; $p = $x + 52;
                $x = strpos($d, ',', $p); $bk[$pi[substr($d, $p, $x - $p)]] .= $dc[substr($d, $x + 3, 8)]; $p = $x + 52;
                $x = strpos($d, ',', $p); $bk[$pi[substr($d, $p, $x - $p)]] .= $dc[substr($d, $x + 3, 8)]; $p = $x + 52;
                $x = strpos($d, ',', $p); $bk[$pi[substr($d, $p, $x - $p)]] .= $dc[substr($d, $x + 3, 8)]; $p = $x + 52;
            }

            while ($p < $ln) {
                $x = strpos($d, ',', $p);
                $bk[$pi[substr($d, $p, $x - $p)]] .= $dc[substr($d, $x + 3, 8)];
                $p = $x + 52;
            }
        }
    }

    private static function writeJson(string $outputPath, array $cnt, array $paths, array $dates, int $dateCount): void
    {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);
        fwrite($out, '{');

        $datePrefixes = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "20' . $dates[$d] . '": ';
        }

        $pc = count($paths);
        $escapedPaths = [];
        for ($p = 0; $p < $pc; $p++) {
            $escapedPaths[$p] = '"\\/blog\\/' . str_replace('/', '\\/', $paths[$p]) . '"';
        }

        $first = true;
        for ($p = 0; $p < $pc; $p++) {
            $base = $p * $dateCount;
            $de = [];
            for ($d = 0; $d < $dateCount; $d++) {
                $c = $cnt[$base + $d];
                if ($c === 0) continue;
                $de[] = $datePrefixes[$d] . $c;
            }
            if (!$de) continue;

            $buf = $first ? "\n    " : ",\n    ";
            $first = false;
            $buf .= $escapedPaths[$p] . ": {\n" . implode(",\n", $de) . "\n    }";
            fwrite($out, $buf);
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
