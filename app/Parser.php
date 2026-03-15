<?php

namespace App;

use RuntimeException;

final class Parser
{
    /**
     * "https://stitcher.io" is exactly 19 bytes on every input line.
     */
    private const int PREFIX_LEN = 19;

    /** 64 KB read buffer per fread() call. */
    private const int BUF_SIZE = 65536;

    /** Segments smaller than this get slurped via file_get_contents(). */
    private const int SLURP_LIMIT = 16 * 1024 * 1024;

    /** Target bytes per parallel worker. */
    private const int BYTES_PER_WORKER = 20 * 1024 * 1024;

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $fileSize = filesize($inputPath);

        if ($fileSize === false || $fileSize === 0) {
            file_put_contents($outputPath, "{\n}");
            return;
        }

        $cpus = $this->cpuCount();
        $numWorkers = min($cpus, max(2, (int) ceil($fileSize / self::BYTES_PER_WORKER)));

        if ($numWorkers > 1 && $fileSize > self::BYTES_PER_WORKER && function_exists('pcntl_fork')) {
            $stats = $this->parallel($inputPath, $fileSize, $numWorkers);
        } else {
            $stats = $this->segment($inputPath, 0, $fileSize);
        }

        // Keys are already pre-escaped (slashes replaced during parsing).
        // Sort paths alphabetically, dates ascending within each path.
        ksort($stats, SORT_STRING);
        foreach ($stats as &$dates) {
            ksort($dates, SORT_STRING);
        }
        unset($dates);

        $this->writeJson($outputPath, $stats);
    }

    private function cpuCount(): int
    {
        static $c = null;
        if ($c !== null) return $c;

        $n = 0;
        if (function_exists('shell_exec')) {
            $n = (int) @shell_exec('nproc 2>/dev/null');
            if ($n < 2) $n = (int) @shell_exec('sysctl -n hw.logicalcpu 2>/dev/null');
        }
        if ($n < 2 && @is_readable('/proc/cpuinfo')) {
            $info = @file_get_contents('/proc/cpuinfo');
            if ($info !== false) $n = substr_count($info, 'processor');
        }

        return $c = ($n > 1 ? min($n, 16) : 8);
    }

    // ----------------------------------------------------------------
    //  Parallel execution via pcntl_fork
    // ----------------------------------------------------------------

    private function parallel(string $inputPath, int $fileSize, int $numWorkers): array
    {
        $segments = $this->split($inputPath, $fileSize, $numWorkers);
        $workerCount = count($segments);
        $igb = function_exists('igbinary_serialize');
        $tmpFiles = [];
        $pids = [];
        $forked = 0;

        for ($w = 0; $w < $workerCount; $w++) {
            $tmp = tempnam(sys_get_temp_dir(), 'tp-');
            if ($tmp === false) throw new RuntimeException('tempnam failed');
            $tmpFiles[$w] = $tmp;

            $pid = pcntl_fork();
            if ($pid === -1) break;

            if ($pid === 0) {
                gc_disable();
                $r = $this->segment($inputPath, $segments[$w][0], $segments[$w][1]);
                file_put_contents($tmp, $igb ? igbinary_serialize($r) : serialize($r));
                exit(0);
            }

            $pids[$w] = $pid;
            $forked++;
        }

        // Collect finished children
        $partials = [];
        $map = array_flip($pids);
        $left = count($pids);

        while ($left > 0) {
            $pid = pcntl_waitpid(-1, $st, WNOHANG);
            if ($pid > 0 && isset($map[$pid])) {
                $w = $map[$pid];
                $left--;
                $raw = file_get_contents($tmpFiles[$w]);
                @unlink($tmpFiles[$w]);
                if ($raw !== false) {
                    $p = $igb ? igbinary_unserialize($raw) : unserialize($raw);
                    if (is_array($p)) $partials[$w] = $p;
                }
            } elseif ($pid === 0) {
                usleep(50);
            }
        }

        // Handle any workers that failed to fork
        for ($w = $forked; $w < $workerCount; $w++) {
            @unlink($tmpFiles[$w]);
            $partials[$w] = $this->segment($inputPath, $segments[$w][0], $segments[$w][1]);
        }

        // Merge partial results
        ksort($partials);
        $merged = [];

        foreach ($partials as $partial) {
            foreach ($partial as $path => $dates) {
                if (!isset($merged[$path])) {
                    $merged[$path] = $dates;
                    continue;
                }
                $ref = &$merged[$path];
                foreach ($dates as $date => $count) {
                    if (isset($ref[$date])) {
                        $ref[$date] += $count;
                    } else {
                        $ref[$date] = $count;
                    }
                }
                unset($ref);
            }
        }

        return $merged;
    }

    /**
     * Split file into byte-range segments aligned to newline boundaries.
     */
    private function split(string $inputPath, int $fileSize, int $n): array
    {
        $chunk = (int) ceil($fileSize / $n);
        $h = fopen($inputPath, 'rb');
        if ($h === false) throw new RuntimeException("Cannot open: {$inputPath}");

        $segs = [];
        $pos = 0;

        for ($i = 0; $i < $n && $pos < $fileSize; $i++) {
            $start = $pos;
            $end = min($pos + $chunk, $fileSize);

            if ($end < $fileSize) {
                fseek($h, $end);
                $peek = fread($h, 512);
                if ($peek !== false) {
                    $nl = strpos($peek, "\n");
                    $end = ($nl !== false) ? $end + $nl + 1 : $fileSize;
                }
            }

            $segs[] = [$start, $end];
            $pos = $end;
        }

        fclose($h);
        return $segs;
    }

    // ----------------------------------------------------------------
    //  Core I/O: fread with carry buffer
    // ----------------------------------------------------------------

    private function segment(string $inputPath, int $start, int $end): array
    {
        $size = $end - $start;
        $stats = [];

        if ($size <= self::SLURP_LIMIT) {
            $content = file_get_contents($inputPath, false, null, $start, $size);
            if ($content === false || $content === '') return [];
            $this->extract($content, $stats);
            return $stats;
        }

        $h = fopen($inputPath, 'rb');
        if ($h === false) throw new RuntimeException("Cannot open: {$inputPath}");
        if ($start > 0) fseek($h, $start);

        $remaining = $size;
        $carry = '';

        while ($remaining > 0) {
            $read = min(self::BUF_SIZE, $remaining);
            $chunk = fread($h, $read);
            if ($chunk === false || $chunk === '') break;
            $remaining -= strlen($chunk);

            $buf = $carry . $chunk;
            $lastNl = strrpos($buf, "\n");

            if ($lastNl === false) {
                $carry = $buf;
                continue;
            }

            $carry = ($lastNl + 1 < strlen($buf)) ? substr($buf, $lastNl + 1) : '';
            $this->extract(substr($buf, 0, $lastNl), $stats);
        }

        if ($carry !== '') {
            $this->extract($carry, $stats);
        }

        fclose($h);
        return $stats;
    }

    // ----------------------------------------------------------------
    //  Hot loop: fixed-offset extraction with pre-escaped path keys
    // ----------------------------------------------------------------

    /**
     * Parse a buffer of complete lines and aggregate into stats.
     *
     * Each line: "https://stitcher.io{path},{YYYY-MM-DD}T{rest}"
     *
     * Path keys are stored with "/" already replaced by "\/" so the
     * JSON writer can emit them directly without any per-key escaping.
     *
     * The path lookup table ($pathMap) caches the raw->escaped mapping
     * so str_replace runs once per unique path rather than once per line.
     * With ~500 unique blog paths across 100M rows, this effectively
     * eliminates str_replace from the hot path.
     */
    private function extract(string $buf, array &$stats): void
    {
        /** @var array<string, string> raw path -> escaped path cache */
        static $pathMap = [];

        $offset = 0;
        $len = strlen($buf);

        while ($offset < $len) {
            $nl = strpos($buf, "\n", $offset);
            if ($nl === false) $nl = $len;

            // Comma is the separator between URL and datetime.
            // Search starts at offset+19 to skip "https://stitcher.io".
            $comma = strpos($buf, ',', $offset + 19);

            if ($comma !== false && $comma < $nl) {
                // Extract raw path and date
                $rawPath = substr($buf, $offset + 19, $comma - $offset - 19);
                $date = substr($buf, $comma + 1, 10);

                // Look up or create the pre-escaped path key.
                // The escaped version has "/" -> "\/" for JSON output.
                if (isset($pathMap[$rawPath])) {
                    $path = $pathMap[$rawPath];
                } else {
                    $path = str_replace('/', '\\/', $rawPath);
                    $pathMap[$rawPath] = $path;
                }

                // Aggregate: three-branch access avoids unnecessary
                // reference creation on the most common case (increment).
                if (!isset($stats[$path])) {
                    $stats[$path] = [$date => 1];
                } elseif (!isset($stats[$path][$date])) {
                    $stats[$path][$date] = 1;
                } else {
                    ++$stats[$path][$date];
                }
            }

            $offset = $nl + 1;
        }
    }

    // ----------------------------------------------------------------
    //  Custom JSON writer: bypasses json_encode() entirely
    // ----------------------------------------------------------------

    /**
     * Write stats as pretty-printed JSON identical to
     * json_encode($data, JSON_PRETTY_PRINT) output.
     *
     * Path keys are already pre-escaped with "\/" from the parsing phase,
     * so the writer emits them directly. Date keys contain only digits and
     * hyphens, and values are integers, so neither needs any escaping.
     *
     * Output is buffered in a 64 KB string and flushed periodically to
     * minimize fwrite() syscalls.
     */
    private function writeJson(string $outputPath, array $stats): void
    {
        $h = fopen($outputPath, 'wb');
        if ($h === false) throw new RuntimeException("Cannot open output: {$outputPath}");

        $buf = "{\n";
        $bufLimit = 65536;

        $pathKeys = array_keys($stats);
        $lastPath = count($pathKeys) - 1;

        for ($p = 0; $p <= $lastPath; $p++) {
            $pathKey = $pathKeys[$p];
            $dates = $stats[$pathKey];

            // Path key is already escaped from parsing phase
            $buf .= '    "' . $pathKey . "\": {\n";

            $dateKeys = array_keys($dates);
            $lastDate = count($dateKeys) - 1;

            for ($d = 0; $d <= $lastDate; $d++) {
                $buf .= '        "' . $dateKeys[$d] . '": ' . $dates[$dateKeys[$d]];
                $buf .= ($d < $lastDate) ? ",\n" : "\n";
            }

            $buf .= ($p < $lastPath) ? "    },\n" : "    }\n";

            // Flush when buffer exceeds limit (using isset on offset is
            // faster than strlen comparison)
            if (isset($buf[$bufLimit])) {
                fwrite($h, $buf);
                $buf = '';
            }
        }

        $buf .= '}';
        fwrite($h, $buf);
        fclose($h);
    }
}
