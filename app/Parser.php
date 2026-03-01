<?php

namespace App;

use App\Commands\Visit;

use function array_fill;
use function fclose;
use function fflush;
use function fgets;
use function file_put_contents;
use function filesize;
use function flock;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function getmypid;
use function implode;
use function min;
use function pack;
use function pcntl_fork;
use function pcntl_wait;
use function posix_getpid;
use function posix_kill;
use function str_replace;
use function stream_set_read_buffer;
use function stream_socket_pair;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function sys_get_temp_dir;
use function unpack;

use const LOCK_EX;
use const LOCK_UN;
use const SEEK_CUR;
use const SEEK_SET;
use const STREAM_IPPROTO_IP;
use const STREAM_PF_UNIX;
use const STREAM_SOCK_STREAM;

final class Parser
{
    private const int WORKERS = 10;
    private const int CHUNKS = 40;
    private const int READ_BUF = 262_144;
    private const int DISCOVER_SIZE = 2_097_152;

    public function parse(string $inputPath, string $outputPath): void
    {
        self::run($inputPath, $outputPath);
    }

    public static function run(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $fileSize = filesize($inputPath);

        // ── Phase 1: Build slug + date mappings BEFORE forking ──

        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);
        $sample = fread($fh, min(self::DISCOVER_SIZE, $fileSize));
        fclose($fh);

        $slugToId = [];
        $slugOrder = [];
        $slugCount = 0;

        $pos = 0;
        $lastNl = strrpos($sample, "\n");
        if ($lastNl === false) $lastNl = 0;

        // Discover slugs in first-seen order from first 2MB
        while ($pos < $lastNl) {
            $nl = strpos($sample, "\n", $pos + 52);
            if ($nl === false) break;
            // Slug is at offset 25 (after "https://stitcher.io/blog/") to comma
            $slug = substr($sample, $pos + 25, $nl - $pos - 51);
            if (!isset($slugToId[$slug])) {
                $slugToId[$slug] = $slugCount;
                $slugOrder[$slugCount] = $slug;
                $slugCount++;
            }
            $pos = $nl + 1;
        }
        unset($sample);

        // Fill remaining slugs from Visit::all()
        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, 25);
            if (!isset($slugToId[$slug])) {
                $slugToId[$slug] = $slugCount;
                $slugOrder[$slugCount] = $slug;
                $slugCount++;
            }
        }

        // Build date mapping: "YY-MM-DD" (8 chars) → sequential int
        $dateToId = [];
        $dateStr = [];
        $dateCount = 0;
        for ($y = 21; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => $y === 24 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $ms = $m < 10 ? "0{$m}" : (string)$m;
                for ($d = 1; $d <= $maxD; $d++) {
                    $ds = $d < 10 ? "0{$d}" : (string)$d;
                    $key = "{$y}-{$ms}-{$ds}";
                    $dateToId[$key] = $dateCount;
                    $dateStr[$dateCount] = "20{$y}-{$ms}-{$ds}";
                    $dateCount++;
                }
            }
        }

        $totalEntries = $slugCount * $dateCount;

        // Pre-build JSON fragments (done once, used at output)
        $jsonDatePrefix = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $jsonDatePrefix[$d] = '        "' . $dateStr[$d] . '": ';
        }
        $jsonSlugHeader = [];
        for ($s = 0; $s < $slugCount; $s++) {
            $jsonSlugHeader[$s] = '"\\/blog\\/' . str_replace('/', '\\/', $slugOrder[$s]) . '"';
        }

        // ── Phase 2: Split file into chunks at newline boundaries ──

        $numChunks = self::CHUNKS;
        $splits = [0];
        $fh = fopen($inputPath, 'rb');
        for ($i = 1; $i < $numChunks; $i++) {
            fseek($fh, (int)($fileSize * $i / $numChunks));
            fgets($fh);
            $splits[] = ftell($fh);
        }
        fclose($fh);
        $splits[] = $fileSize;

        // ── Phase 3: Work-stealing queue (atomic file counter) ──

        $tmpDir = sys_get_temp_dir();
        $queueFile = $tmpDir . '/wq_' . getmypid();
        $qfh = fopen($queueFile, 'c+b');
        fwrite($qfh, pack('V', 0));
        fflush($qfh);

        // ── Phase 4: Socket pairs for zero-copy IPC ──

        $childCount = self::WORKERS - 1;
        $sockets = [];
        for ($i = 0; $i < $childCount; $i++) {
            $sockets[$i] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        }

        // ── Phase 5: Fork children ──

        for ($w = 0; $w < $childCount; $w++) {
            $pid = pcntl_fork();
            if ($pid === 0) {
                // Child: close read ends
                fclose($sockets[$w][0]);
                for ($j = 0; $j < $childCount; $j++) {
                    if ($j !== $w) {
                        fclose($sockets[$j][0]);
                        fclose($sockets[$j][1]);
                    }
                }

                $counts = array_fill(0, $totalEntries, 0);
                $fh = fopen($inputPath, 'rb');
                stream_set_read_buffer($fh, 0);
                $myQ = fopen($queueFile, 'c+b');

                while (true) {
                    $ci = self::grabChunk($myQ, $numChunks);
                    if ($ci === -1) break;
                    self::processChunk($fh, $splits[$ci], $splits[$ci + 1], $slugToId, $dateToId, $dateCount, $counts);
                }

                fclose($fh);
                fclose($myQ);

                // Send packed counts via socket
                $packed = pack('V*', ...$counts);
                $len = strlen($packed);
                $off = 0;
                $sock = $sockets[$w][1];
                while ($off < $len) {
                    $n = fwrite($sock, substr($packed, $off, 131_072));
                    if ($n === false || $n === 0) break;
                    $off += $n;
                }
                fclose($sock);

                posix_kill(posix_getpid(), 9);
            }
        }

        // Parent: close write ends
        for ($w = 0; $w < $childCount; $w++) {
            fclose($sockets[$w][1]);
        }

        // ── Phase 6: Parent processes chunks too ──

        $counts = array_fill(0, $totalEntries, 0);
        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);

        while (true) {
            $ci = self::grabChunk($qfh, $numChunks);
            if ($ci === -1) break;
            self::processChunk($fh, $splits[$ci], $splits[$ci + 1], $slugToId, $dateToId, $dateCount, $counts);
        }

        fclose($fh);
        fclose($qfh);

        // ── Phase 7: Merge child results from sockets ──

        $expectedBytes = $totalEntries * 4;
        for ($w = 0; $w < $childCount; $w++) {
            $sock = $sockets[$w][0];
            $raw = '';
            $need = $expectedBytes;
            while ($need > 0) {
                $buf = fread($sock, $need > 131_072 ? 131_072 : $need);
                if ($buf === false || $buf === '') break;
                $raw .= $buf;
                $need -= strlen($buf);
            }
            fclose($sock);

            $childCounts = unpack('V*', $raw);
            $idx = 1; // unpack is 1-indexed
            for ($j = 0; $j < $totalEntries; $j++) {
                $counts[$j] += $childCounts[$idx++];
            }
        }
        unset($raw, $childCounts);

        // Wait for zombie children
        for ($w = 0; $w < $childCount; $w++) {
            pcntl_wait($status);
        }
        @unlink($queueFile);

        // ── Phase 8: Write JSON output ──

        $out = '{';
        $first = true;

        for ($s = 0; $s < $slugCount; $s++) {
            $base = $s * $dateCount;
            $dateEntries = [];

            for ($d = 0; $d < $dateCount; $d++) {
                $n = $counts[$base + $d];
                if ($n === 0) continue;
                $dateEntries[] = $jsonDatePrefix[$d] . $n;
            }

            if ($dateEntries === []) continue;

            $out .= ($first ? '' : ',') . "\n    " . $jsonSlugHeader[$s] . ": {\n"
                . implode(",\n", $dateEntries)
                . "\n    }";
            $first = false;
        }

        $out .= "\n}";
        file_put_contents($outputPath, $out);
    }

    private static function grabChunk($qfh, int $numChunks): int
    {
        flock($qfh, LOCK_EX);
        fseek($qfh, 0, SEEK_SET);
        $idx = unpack('V', fread($qfh, 4))[1];
        if ($idx >= $numChunks) {
            flock($qfh, LOCK_UN);
            return -1;
        }
        fseek($qfh, 0, SEEK_SET);
        fwrite($qfh, pack('V', $idx + 1));
        fflush($qfh);
        flock($qfh, LOCK_UN);
        return $idx;
    }

    private static function processChunk(
        $fh,
        int $start,
        int $end,
        array &$slugToId,
        array &$dateToId,
        int $dateCount,
        array &$counts,
    ): void {
        fseek($fh, $start, SEEK_SET);
        $remaining = $end - $start;
        $bufSize = self::READ_BUF;

        while ($remaining > 0) {
            $toRead = $remaining > $bufSize ? $bufSize : $remaining;
            $chunk = fread($fh, $toRead);
            $cLen = strlen($chunk);
            if ($cLen === 0) break;
            $remaining -= $cLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) break;

            // Seek back for any partial line after last newline
            $tail = $cLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($fh, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            // $p points to slug start (offset 25 from line start)
            // First line in chunk: skip the URL prefix
            $p = 25;
            $fence = $lastNl - 600;

            // ── 8x unrolled fast path ──
            if ($p < $fence) {
                do {
                    $sep = strpos($chunk, ',', $p);
                    $counts[$slugToId[substr($chunk, $p, $sep - $p)] * $dateCount + $dateToId[substr($chunk, $sep + 3, 8)]]++;
                    $p = $sep + 52;

                    $sep = strpos($chunk, ',', $p);
                    $counts[$slugToId[substr($chunk, $p, $sep - $p)] * $dateCount + $dateToId[substr($chunk, $sep + 3, 8)]]++;
                    $p = $sep + 52;

                    $sep = strpos($chunk, ',', $p);
                    $counts[$slugToId[substr($chunk, $p, $sep - $p)] * $dateCount + $dateToId[substr($chunk, $sep + 3, 8)]]++;
                    $p = $sep + 52;

                    $sep = strpos($chunk, ',', $p);
                    $counts[$slugToId[substr($chunk, $p, $sep - $p)] * $dateCount + $dateToId[substr($chunk, $sep + 3, 8)]]++;
                    $p = $sep + 52;

                    $sep = strpos($chunk, ',', $p);
                    $counts[$slugToId[substr($chunk, $p, $sep - $p)] * $dateCount + $dateToId[substr($chunk, $sep + 3, 8)]]++;
                    $p = $sep + 52;

                    $sep = strpos($chunk, ',', $p);
                    $counts[$slugToId[substr($chunk, $p, $sep - $p)] * $dateCount + $dateToId[substr($chunk, $sep + 3, 8)]]++;
                    $p = $sep + 52;

                    $sep = strpos($chunk, ',', $p);
                    $counts[$slugToId[substr($chunk, $p, $sep - $p)] * $dateCount + $dateToId[substr($chunk, $sep + 3, 8)]]++;
                    $p = $sep + 52;

                    $sep = strpos($chunk, ',', $p);
                    $counts[$slugToId[substr($chunk, $p, $sep - $p)] * $dateCount + $dateToId[substr($chunk, $sep + 3, 8)]]++;
                    $p = $sep + 52;
                } while ($p < $fence);
            }

            // ── Safe tail loop for boundary lines ──
            while ($p < $lastNl) {
                $sep = strpos($chunk, ',', $p);
                if ($sep === false || $sep >= $lastNl) break;
                $counts[$slugToId[substr($chunk, $p, $sep - $p)] * $dateCount + $dateToId[substr($chunk, $sep + 3, 8)]]++;
                $p = $sep + 52;
            }
        }
    }
}
