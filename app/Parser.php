<?php

declare(strict_types=1);

namespace App;

use App\Commands\Visit;
use RuntimeException;

final class Parser
{
    // 16 MB chunks: good throughput, ~16 MB peak RAM per worker for the raw buffer.
    // explode("\n") on a 64 MB chunk creates ~1 M PHP strings (~80 MB heap) — avoid.
    // With strpos-loop we never materialise all lines at once.
    private const int READ_CHUNK     = 16_777_216; // 16 MB
    private const int DISCOVER_SIZE  = 16_777_216; // 16 MB
    private const int URL_PREFIX_LEN = 25;

    // Every line ends with ",YYYY-MM-DDTHH:MM:SS+00:00\n" = 27 chars
    // commaPos = nlPos - LINE_TAIL  (LINE_TAIL = 26: comma + 25 chars timestamp)
    // dateKey  = commaPos + 3  (skip ',', '2', '0' → lands on "YY-MM-DD")
    private const int LINE_TAIL = 26;

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        if (! is_file($inputPath)) {
            throw new RuntimeException("Input file does not exist: {$inputPath}");
        }

        $fileSize = filesize($inputPath);
        if ($fileSize === false) {
            throw new RuntimeException("Unable to read file size: {$inputPath}");
        }
        if ($fileSize === 0) {
            file_put_contents($outputPath, "{}\n");
            return;
        }

        [$dateIds, $dates, $dateCount] = $this->buildDateMap();
        [$pathIds, $paths, $pathCount] = $this->discoverPaths($inputPath, $fileSize, $dateCount);

        $workers = $this->detectCores();

        // ── ext-parallel (true threads, zero file IPC) ───────────────────────
        if (
            class_exists(\parallel\Runtime::class)
            && $fileSize > self::DISCOVER_SIZE
            && $workers > 1
        ) {
            $this->parseParallel(
                $inputPath, $outputPath, $fileSize,
                $pathIds, $paths, $pathCount,
                $dateIds, $dates, $dateCount,
                $workers,
            );
            return;
        }

        // ── pcntl_fork + shmop (zero file IPC) ──────────────────────────────
        if (
            function_exists('pcntl_fork')
            && function_exists('shmop_open')
            && $fileSize > self::DISCOVER_SIZE
            && $workers > 1
        ) {
            $this->parseParallelShmop(
                $inputPath, $outputPath, $fileSize,
                $pathIds, $paths, $pathCount,
                $dateIds, $dates, $dateCount,
                min($workers, 8),
            );
            return;
        }

        // ── pcntl_fork + /dev/shm file IPC (legacy fallback) ────────────────
        if (
            function_exists('pcntl_fork')
            && $fileSize > self::DISCOVER_SIZE
            && $workers > 1
        ) {
            $this->parseParallelFork(
                $inputPath, $outputPath, $fileSize,
                $pathIds, $paths, $pathCount,
                $dateIds, $dates, $dateCount,
                min($workers, 8),
            );
            return;
        }

        // ── Single-process fallback ──────────────────────────────────────────
        $counts = $this->parseRange($inputPath, 0, $fileSize, $pathIds, $dateIds, $pathCount, $dateCount);
        $this->writeJson($outputPath, $paths, $pathCount, $dates, $dateCount, $counts);
    }

    // =========================================================================
    // Core hot loop — strpos-based, memory-bounded
    // =========================================================================

    /**
     * Parse a byte range into a flat uint32 counts array.
     *
     * Memory per call:
     *   $counts  : pathCount × dateCount × ~80 B  (CoW copy, freed after merge)
     *   $chunk   : READ_CHUNK = 16 MB raw buffer  (overwritten each iteration)
     *   NO line array materialised — strpos walks the chunk in-place.
     *
     * Hot-loop geometry (each CSV line):
     *   https://stitcher.io/blog/<slug>,<timestamp>\n
     *   |←─ URL_PREFIX_LEN=25 ─→|←slug→|←─ LINE_TAIL+1=27 ─→|
     *
     *   nlPos    = strpos($chunk, "\n", $pos)
     *   commaPos = nlPos - LINE_TAIL           (= nlPos - 26)
     *   slug     = substr($chunk, $pos, commaPos - $pos)
     *   dateKey  = substr($chunk, commaPos + 3, 8)   (skip ',20' → "YY-MM-DD")
     *   next pos = nlPos + URL_PREFIX_LEN + 1
     *
     * @param array<string,int> $pathIds
     * @param array<string,int> $dateIds
     * @return array<int,int>
     */
    private function parseRange(
        string $inputPath,
        int    $start,
        int    $end,
        array  $pathIds,
        array  $dateIds,
        int    $pathCount,
        int    $dateCount,
    ): array {
        $counts    = array_fill(0, $pathCount * $dateCount, 0);
        $readChunk = self::READ_CHUNK;
        $prefixLen = self::URL_PREFIX_LEN;
        $lineTail  = self::LINE_TAIL;

        $handle = fopen($inputPath, 'rb');
        if ($handle === false) {
            throw new RuntimeException("Unable to open: {$inputPath}");
        }

        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $toRead   = $remaining > $readChunk ? $readChunk : $remaining;
            $chunk    = fread($handle, $toRead);
            $chunkLen = strlen($chunk);
            if ($chunkLen === 0) break;

            $remaining -= $chunkLen;
            $lastNl     = strrpos($chunk, "\n");

            if ($lastNl === false) {
                fseek($handle, -$chunkLen, SEEK_CUR);
                $remaining += $chunkLen;
                break;
            }

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            // ── Tight strpos loop ────────────────────────────────────────────
            $pos = $prefixLen;
            while ($pos < $lastNl) {
                $nlPos    = strpos($chunk, "\n", $pos);
                $commaPos = $nlPos - $lineTail;
                $slug     = substr($chunk, $pos, $commaPos - $pos);
                $dateKey  = substr($chunk, $commaPos + 3, 8);
                $counts[$pathIds[$slug] + $dateIds[$dateKey]]++;
                $pos = $nlPos + $prefixLen + 1;
            }
        }

        fclose($handle);
        return $counts;
    }

    // =========================================================================
    // ext-parallel path (true threads, zero IPC overhead)
    // =========================================================================

    /** @param array<string,int> $pathIds @param array<int,string> $paths @param array<string,int> $dateIds @param array<int,string> $dates */
    private function parseParallel(
        string $inputPath,
        string $outputPath,
        int    $fileSize,
        array  $pathIds,
        array  $paths,
        int    $pathCount,
        array  $dateIds,
        array  $dates,
        int    $dateCount,
        int    $workers,
    ): void {
        $boundaries = $this->buildBoundaries($inputPath, $fileSize, $workers);
        $autoloader = $this->findAutoloader();

        $task = static function (
            string $autoloader,
            string $inputPath,
            int    $start,
            int    $end,
            array  $pathIds,
            array  $dateIds,
            int    $pathCount,
            int    $dateCount,
            int    $readChunk,
            int    $prefixLen,
            int    $lineTail,
        ): array {
            require_once $autoloader;
            gc_disable();

            $counts = array_fill(0, $pathCount * $dateCount, 0);
            $handle = fopen($inputPath, 'rb');
            stream_set_read_buffer($handle, 0);
            fseek($handle, $start);
            $remaining = $end - $start;

            while ($remaining > 0) {
                $toRead   = $remaining > $readChunk ? $readChunk : $remaining;
                $chunk    = fread($handle, $toRead);
                $chunkLen = strlen($chunk);
                if ($chunkLen === 0) break;
                $remaining -= $chunkLen;
                $lastNl = strrpos($chunk, "\n");
                if ($lastNl === false) {
                    fseek($handle, -$chunkLen, SEEK_CUR);
                    $remaining += $chunkLen;
                    break;
                }
                $tail = $chunkLen - $lastNl - 1;
                if ($tail > 0) {
                    fseek($handle, -$tail, SEEK_CUR);
                    $remaining += $tail;
                }
                $pos = $prefixLen;
                while ($pos < $lastNl) {
                    $nlPos    = strpos($chunk, "\n", $pos);
                    $commaPos = $nlPos - $lineTail;
                    $slug     = substr($chunk, $pos, $commaPos - $pos);
                    $dateKey  = substr($chunk, $commaPos + 3, 8);
                    $counts[$pathIds[$slug] + $dateIds[$dateKey]]++;
                    $pos = $nlPos + $prefixLen + 1;
                }
            }

            fclose($handle);
            return $counts;
        };

        $futures = [];
        for ($w = 0; $w < $workers - 1; $w++) {
            $runtime     = new \parallel\Runtime($autoloader);
            $futures[$w] = $runtime->run(
                $task,
                $autoloader,
                $inputPath,
                $boundaries[$w],
                $boundaries[$w + 1],
                $pathIds,
                $dateIds,
                $pathCount,
                $dateCount,
                self::READ_CHUNK,
                self::URL_PREFIX_LEN,
                self::LINE_TAIL,
            );
        }

        $counts = $this->parseRange(
            $inputPath,
            $boundaries[$workers - 1],
            $boundaries[$workers],
            $pathIds, $dateIds, $pathCount, $dateCount,
        );

        foreach ($futures as $future) {
            $partial = $future->value();
            $len     = count($counts);
            for ($i = 0; $i < $len; $i++) {
                $counts[$i] += $partial[$i];
            }
        }

        $this->writeJsonParallel($outputPath, $paths, $pathCount, $dates, $dateCount, $counts, $workers);
    }

    // =========================================================================
    // pcntl_fork + shmop (zero-copy shared memory IPC)
    // =========================================================================

    /**
     * Memory budget per child:
     *   $counts array  : pathCount × dateCount × ~80 B  (CoW fork copy)
     *   $chunk buffer  : READ_CHUNK = 16 MB
     *   shmop segment  : pathCount × dateCount × 4 B  (kernel RAM, not PHP heap)
     *
     * @param array<string,int> $pathIds @param array<int,string> $paths @param array<string,int> $dateIds @param array<int,string> $dates
     */
    private function parseParallelShmop(
        string $inputPath,
        string $outputPath,
        int    $fileSize,
        array  $pathIds,
        array  $paths,
        int    $pathCount,
        array  $dateIds,
        array  $dates,
        int    $dateCount,
        int    $workers,
    ): void {
        $boundaries = $this->buildBoundaries($inputPath, $fileSize, $workers);
        $cellCount  = $pathCount * $dateCount;
        $shmSize    = $cellCount * 4;
        $myPid      = getmypid();
        $shmKeys    = [];
        $shmIds     = [];

        for ($w = 0; $w < $workers - 1; $w++) {
            $key   = 0x100000 + ($myPid & 0xFFFF) * 16 + $w;
            $shmId = $this->tryShmOpen($key, 'n', 0600, $shmSize);
            if ($shmId === false) {
                for ($x = 0; $x < $w; $x++) {
                    shmop_delete($shmIds[$x]);
                    shmop_close($shmIds[$x]);
                }
                $this->parseParallelFork(
                    $inputPath, $outputPath, $fileSize,
                    $pathIds, $paths, $pathCount,
                    $dateIds, $dates, $dateCount,
                    $workers,
                );
                return;
            }
            $shmKeys[$w] = $key;
            $shmIds[$w]  = $shmId;
        }

        $children = [];
        for ($w = 0; $w < $workers - 1; $w++) {
            $pid = pcntl_fork();
            if ($pid === 0) {
                $wCounts  = $this->parseRange(
                    $inputPath, $boundaries[$w], $boundaries[$w + 1],
                    $pathIds, $dateIds, $pathCount, $dateCount,
                );
                $bin      = pack('V*', ...$wCounts);
                $childShm = $this->tryShmOpen($shmKeys[$w], 'w', 0, 0);
                if ($childShm !== false) {
                    shmop_write($childShm, $bin, 0);
                    shmop_close($childShm);
                }
                exit(0);
            }
            if ($pid < 0) throw new RuntimeException('Fork failed');
            $children[$w] = $pid;
        }

        $counts = $this->parseRange(
            $inputPath, $boundaries[$workers - 1], $boundaries[$workers],
            $pathIds, $dateIds, $pathCount, $dateCount,
        );

        foreach ($children as $w => $pid) {
            pcntl_waitpid($pid, $status);
            $ok = pcntl_wifexited($status) && pcntl_wexitstatus($status) === 0;

            if ($ok) {
                $bin     = shmop_read($shmIds[$w], 0, $shmSize);
                $wCounts = is_string($bin) ? unpack('V*', $bin) : false;
                if (is_array($wCounts) && count($wCounts) === $cellCount) {
                    $j = 1;
                    for ($i = 0; $i < $cellCount; $i++) {
                        $counts[$i] += $wCounts[$j++];
                    }
                    shmop_delete($shmIds[$w]);
                    shmop_close($shmIds[$w]);
                    continue;
                }
            }

            $fallback = $this->parseRange(
                $inputPath, $boundaries[$w], $boundaries[$w + 1],
                $pathIds, $dateIds, $pathCount, $dateCount,
            );
            for ($i = 0; $i < $cellCount; $i++) {
                $counts[$i] += $fallback[$i];
            }
            shmop_delete($shmIds[$w]);
            shmop_close($shmIds[$w]);
        }

        $this->writeJsonParallel($outputPath, $paths, $pathCount, $dates, $dateCount, $counts, $workers);
    }

    // =========================================================================
    // pcntl_fork + /dev/shm file IPC (legacy fallback)
    // =========================================================================

    /** @param array<string,int> $pathIds @param array<int,string> $paths @param array<string,int> $dateIds @param array<int,string> $dates */
    private function parseParallelFork(
        string $inputPath,
        string $outputPath,
        int    $fileSize,
        array  $pathIds,
        array  $paths,
        int    $pathCount,
        array  $dateIds,
        array  $dates,
        int    $dateCount,
        int    $workers,
    ): void {
        $boundaries = $this->buildBoundaries($inputPath, $fileSize, $workers);
        $tmpDir     = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
        $myPid      = getmypid();
        $children   = [];

        for ($w = 0; $w < $workers - 1; $w++) {
            $tmpFile = "{$tmpDir}/p100m_{$myPid}_{$w}";
            $pid     = pcntl_fork();
            if ($pid === 0) {
                $wCounts = $this->parseRange(
                    $inputPath, $boundaries[$w], $boundaries[$w + 1],
                    $pathIds, $dateIds, $pathCount, $dateCount,
                );
                file_put_contents($tmpFile, pack('V*', ...$wCounts));
                exit(0);
            }
            if ($pid < 0) throw new RuntimeException('Fork failed');
            $children[] = ['pid' => $pid, 'start' => $boundaries[$w], 'end' => $boundaries[$w + 1], 'tmpFile' => $tmpFile];
        }

        $counts    = $this->parseRange(
            $inputPath, $boundaries[$workers - 1], $boundaries[$workers],
            $pathIds, $dateIds, $pathCount, $dateCount,
        );
        $cellCount = $pathCount * $dateCount;

        foreach ($children as $child) {
            pcntl_waitpid($child['pid'], $status);
            $ok      = pcntl_wifexited($status) && pcntl_wexitstatus($status) === 0;
            $payload = $ok ? file_get_contents($child['tmpFile']) : false;
            @unlink($child['tmpFile']);

            if ($payload !== false && $payload !== '') {
                $wCounts = unpack('V*', $payload);
                if (is_array($wCounts)) {
                    $j = 1;
                    for ($i = 0; $i < $cellCount; $i++) {
                        $counts[$i] += $wCounts[$j++];
                    }
                    continue;
                }
            }

            $fallback = $this->parseRange(
                $inputPath, $child['start'], $child['end'],
                $pathIds, $dateIds, $pathCount, $dateCount,
            );
            for ($i = 0; $i < $cellCount; $i++) {
                $counts[$i] += $fallback[$i];
            }
        }

        $this->writeJsonParallel($outputPath, $paths, $pathCount, $dates, $dateCount, $counts, $workers);
    }

    // =========================================================================
    // Parallel JSON write
    // =========================================================================

    /**
     * Fork $workers children, each writing a disjoint path slice to a temp file.
     * Path blocks are separated by \x00 (NUL) — invalid in JSON, safe sentinel.
     * Parent stitches files in order, inserting commas and indentation between blocks.
     *
     * @param array<int,string> $paths
     * @param array<int,string> $dates
     * @param array<int,int>    $counts
     */
    private function writeJsonParallel(
        string $outputPath,
        array  $paths,
        int    $pathCount,
        array  $dates,
        int    $dateCount,
        array  $counts,
        int    $workers,
    ): void {
        if (! function_exists('pcntl_fork') || $pathCount < $workers * 2) {
            $this->writeJson($outputPath, $paths, $pathCount, $dates, $dateCount, $counts);
            return;
        }

        $tmpDir = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
        $myPid  = getmypid();

        // Build lookup tables once in parent; children inherit via CoW
        $datePrefixes = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "20' . $dates[$d] . '": ';
        }
        $pathHeaders = [];
        for ($p = 0; $p < $pathCount; $p++) {
            $pathHeaders[$p] = "    \"\\/blog\\/" . str_replace('/', '\\/', $paths[$p]) . "\": {\n";
        }

        $sliceSize = (int) ceil($pathCount / $workers);
        $children  = [];
        $tmpFiles  = [];

        for ($w = 0; $w < $workers; $w++) {
            $pStart = $w * $sliceSize;
            $pEnd   = min($pStart + $sliceSize, $pathCount);
            if ($pStart >= $pathCount) break;

            $tmpFile    = "{$tmpDir}/p100m_json_{$myPid}_{$w}";
            $tmpFiles[] = $tmpFile;
            $pid        = pcntl_fork();

            if ($pid === 0) {
                $out = fopen($tmpFile, 'wb');
                if ($out === false) exit(1);
                stream_set_write_buffer($out, self::READ_CHUNK);

                for ($p = $pStart; $p < $pEnd; $p++) {
                    $base = $p * $dateCount;
                    $body = '';
                    $sep  = '';
                    for ($d = 0; $d < $dateCount; $d++) {
                        $c = $counts[$base + $d];
                        if ($c === 0) continue;
                        $body .= $sep . $datePrefixes[$d] . $c;
                        $sep   = ",\n";
                    }
                    if ($body === '') continue;
                    fwrite($out, $pathHeaders[$p] . $body . "\n    }\x00");
                }

                fclose($out);
                exit(0);
            }

            if ($pid < 0) throw new RuntimeException('Fork failed');
            $children[$w] = $pid;
        }

        foreach ($children as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $out = fopen($outputPath, 'wb');
        if ($out === false) throw new RuntimeException("Cannot open output: {$outputPath}");
        stream_set_write_buffer($out, self::READ_CHUNK);
        fwrite($out, '{');

        $firstBlock = true;
        foreach ($tmpFiles as $tmpFile) {
            if (! file_exists($tmpFile)) continue;
            $content = file_get_contents($tmpFile);
            @unlink($tmpFile);
            if ($content === false || $content === '') continue;

            foreach (array_filter(explode("\x00", $content)) as $block) {
                fwrite($out, ($firstBlock ? "\n    " : ",\n    ") . $block);
                $firstBlock = false;
            }
        }

        fwrite($out, "\n}");
        fclose($out);
    }

    // =========================================================================
    // Single-process JSON write (fallback / small datasets)
    // =========================================================================

    /** @param array<int,string> $paths @param array<int,string> $dates @param array<int,int> $counts */
    private function writeJson(
        string $outputPath,
        array  $paths,
        int    $pathCount,
        array  $dates,
        int    $dateCount,
        array  $counts,
    ): void {
        $datePrefixes = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "20' . $dates[$d] . '": ';
        }
        $pathHeaders = [];
        for ($p = 0; $p < $pathCount; $p++) {
            $pathHeaders[$p] = "\n    \"\\/blog\\/" . str_replace('/', '\\/', $paths[$p]) . "\": {\n";
        }

        $out = fopen($outputPath, 'wb');
        if ($out === false) {
            throw new RuntimeException("Unable to write output file: {$outputPath}");
        }
        stream_set_write_buffer($out, self::READ_CHUNK);
        fwrite($out, '{');

        $firstPath = true;
        for ($p = 0; $p < $pathCount; $p++) {
            $base = $p * $dateCount;
            $body = '';
            $sep  = '';
            for ($d = 0; $d < $dateCount; $d++) {
                $count = $counts[$base + $d];
                if ($count === 0) continue;
                $body .= $sep . $datePrefixes[$d] . $count;
                $sep   = ",\n";
            }
            if ($body === '') continue;
            fwrite($out, ($firstPath ? '' : ',') . $pathHeaders[$p] . $body . "\n    }");
            $firstPath = false;
        }

        fwrite($out, "\n}");
        fclose($out);
    }

    // =========================================================================
    // Discovery & utilities
    // =========================================================================

    /** @return array{0: array<string,int>, 1: array<int,string>, 2:int} */
    private function buildDateMap(): array
    {
        $dateIds   = [];
        $dates     = [];
        $dateCount = 0;

        for ($y = 20; $y <= 26; $y++) {
            $yStr = ($y < 10 ? '0' : '') . $y;
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2           => (($y + 2000) % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default     => 31,
                };
                $mStr  = ($m < 10 ? '0' : '') . $m;
                $ymStr = $yStr . '-' . $mStr . '-';
                for ($d = 1; $d <= $maxD; $d++) {
                    $key               = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $dateIds[$key]     = $dateCount;
                    $dates[$dateCount] = $key;
                    $dateCount++;
                }
            }
        }

        return [$dateIds, $dates, $dateCount];
    }

    /** @return array{0: array<string,int>, 1: array<int,string>, 2:int} */
    private function discoverPaths(string $inputPath, int $fileSize, int $dateCount): array
    {
        $pathIds   = [];
        $paths     = [];
        $pathCount = 0;

        // Seed from Visit::all() first — known paths get low IDs → better cache locality
        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, self::URL_PREFIX_LEN);
            if (! isset($pathIds[$slug])) {
                $pathIds[$slug]    = $pathCount * $dateCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }
        }

        // Scan a 16 MB window to catch any additional paths
        $handle = fopen($inputPath, 'rb');
        if ($handle === false) {
            throw new RuntimeException("Unable to open input file: {$inputPath}");
        }
        stream_set_read_buffer($handle, 0);
        $chunk = fread($handle, min($fileSize, self::DISCOVER_SIZE));
        fclose($handle);

        if (is_string($chunk) && $chunk !== '') {
            $lastNl = strrpos($chunk, "\n");
            if ($lastNl !== false) {
                $pos = self::URL_PREFIX_LEN;
                while ($pos < $lastNl) {
                    $nlPos    = strpos($chunk, "\n", $pos);
                    $commaPos = $nlPos - self::LINE_TAIL;
                    $slug     = substr($chunk, $pos, $commaPos - $pos);
                    if (! isset($pathIds[$slug])) {
                        $pathIds[$slug]    = $pathCount * $dateCount;
                        $paths[$pathCount] = $slug;
                        $pathCount++;
                    }
                    $pos = $nlPos + self::URL_PREFIX_LEN + 1;
                }
            }
        }

        return [$pathIds, $paths, $pathCount];
    }

    /** @return array<int,int> */
    private function buildBoundaries(string $inputPath, int $fileSize, int $workers): array
    {
        $boundaries = [0];
        $bh         = fopen($inputPath, 'rb');
        if ($bh === false) throw new RuntimeException("Cannot open: {$inputPath}");

        for ($i = 1; $i < $workers; $i++) {
            fseek($bh, (int) ($fileSize * $i / $workers));
            fgets($bh);
            $boundaries[] = (int) ftell($bh);
        }

        fclose($bh);
        $boundaries[] = $fileSize;
        return $boundaries;
    }

    private function detectCores(): int
    {
        if (function_exists('shell_exec')) {
            $n = (int) shell_exec('nproc 2>/dev/null || sysctl -n hw.logicalcpu 2>/dev/null');
            if ($n > 0) return min($n, 16);
        }
        if (is_readable('/proc/cpuinfo')) {
            $c = substr_count((string) file_get_contents('/proc/cpuinfo'), "\nprocessor\t:");
            if ($c > 0) return min($c, 16);
        }
        return 4;
    }

    private function tryShmOpen(int $key, string $mode, int $permissions, int $size): mixed
    {
        $previous = set_error_handler(static fn (): bool => true);
        try {
            return shmop_open($key, $mode, $permissions, $size);
        } catch (\Throwable) {
            return false;
        } finally {
            if ($previous !== null) {
                set_error_handler($previous);
            } else {
                restore_error_handler();
            }
        }
    }

    private function findAutoloader(): string
    {
        $dir = __DIR__;
        for ($i = 0; $i < 6; $i++) {
            $f = $dir . '/vendor/autoload.php';
            if (file_exists($f)) return $f;
            $dir = dirname($dir);
        }
        return dirname(__DIR__) . '/vendor/autoload.php';
    }
}
