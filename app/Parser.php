<?php

namespace App;

use RuntimeException;

final class Parser
{
    /** Read size for buffered `fread()` parsing. */
    private const int BUF_SIZE = 65536;

    /** Segments at or below this size are read in a single call. */
    private const int SLURP_LIMIT = 16 * 1024 * 1024;

    /** Target segment size when splitting work across workers. */
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

        if ($numWorkers > 1 && $fileSize > self::BYTES_PER_WORKER) {
            if (class_exists(\parallel\Runtime::class)) {
                $stats = $this->parallelExt($inputPath, $fileSize, $numWorkers);
            } elseif (function_exists('pcntl_fork')) {
                $stats = $this->parallelFork($inputPath, $fileSize, $numWorkers);
            } else {
                $stats = $this->segment($inputPath, 0, $fileSize);
            }
        } else {
            $stats = $this->segment($inputPath, 0, $fileSize);
        }

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
    //  Parallel extension path
    //
    //  Uses `parallel\Runtime` workers when the extension is available.
    //  The worker closure must be self-contained, so the parsing logic
    //  used in this path is defined inline.
    // ----------------------------------------------------------------

    private function parallelExt(string $inputPath, int $fileSize, int $numWorkers): array
    {
        $segments = $this->split($inputPath, $fileSize, $numWorkers);
        $workerCount = count($segments);

        $futures = [];

        for ($w = 0; $w < $workerCount; $w++) {
            $runtime = new \parallel\Runtime();

            $segStart = $segments[$w][0];
            $segEnd = $segments[$w][1];

            $futures[$w] = $runtime->run(static function (string $file, int $start, int $end): array {
                gc_disable();

                $size = $end - $start;
                $stats = [];
                $pathCache = [];

                // Self-contained parser for the worker runtime.
                $parse = static function (string $buf) use (&$stats, &$pathCache): void {
                    $offset = 0;
                    $len = strlen($buf);

                    while ($offset < $len) {
                        $nl = strpos($buf, "\n", $offset);
                        if ($nl === false) $nl = $len;

                        $comma = strpos($buf, ',', $offset + 19);

                        if ($comma !== false && $comma < $nl) {
                            $rawPath = substr($buf, $offset + 19, $comma - $offset - 19);
                            $date = substr($buf, $comma + 1, 10);

                            if (isset($pathCache[$rawPath])) {
                                $path = $pathCache[$rawPath];
                            } else {
                                $path = str_replace('/', '\\/', $rawPath);
                                $pathCache[$rawPath] = $path;
                            }

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
                };

                // Small segments are read in a single call.
                if ($size <= 16 * 1024 * 1024) {
                    $content = file_get_contents($file, false, null, $start, $size);
                    if ($content !== false && $content !== '') {
                        $parse($content);
                    }
                    return $stats;
                }

                // Large segments are processed with buffered reads.
                $h = fopen($file, 'rb');
                if ($h === false) return [];
                if ($start > 0) fseek($h, $start);

                $remaining = $size;
                $carry = '';
                $bufSize = 65536;

                while ($remaining > 0) {
                    $read = min($bufSize, $remaining);
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
                    $parse(substr($buf, 0, $lastNl));
                }

                if ($carry !== '') {
                    $parse($carry);
                }

                fclose($h);
                return $stats;
            }, [$inputPath, $segStart, $segEnd]);
        }

        // Collect worker results in segment order before merging.
        $merged = [];
        for ($w = 0; $w < $workerCount; $w++) {
            $partial = $futures[$w]->value();

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

    // ----------------------------------------------------------------
    //  Process-based parallel fallback
    //
    //  Uses `pcntl_fork()` workers and exchanges partial results through
    //  temporary files. `/dev/shm` is preferred when available.
    // ----------------------------------------------------------------

    private function parallelFork(string $inputPath, int $fileSize, int $numWorkers): array
    {
        $segments = $this->split($inputPath, $fileSize, $numWorkers);
        $workerCount = count($segments);
        $igb = function_exists('igbinary_serialize');

        $tmpDir = is_writable('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();

        $tmpFiles = [];
        $pids = [];
        $forked = 0;

        for ($w = 0; $w < $workerCount; $w++) {
            $tmp = tempnam($tmpDir, 'tp-');
            if ($tmp === false) throw new RuntimeException('tempnam failed');
            $tmpFiles[$w] = $tmp;

            $pid = pcntl_fork();
            if ($pid === -1) break;

            if ($pid === 0) {
                gc_disable();
                $result = $this->segment($inputPath, $segments[$w][0], $segments[$w][1]);
                file_put_contents($tmp, $igb ? igbinary_serialize($result) : serialize($result));
                exit(0);
            }

            $pids[$w] = $pid;
            $forked++;
        }

        $pidMap = array_flip($pids);
        $left = count($pids);
        while ($left > 0) {
            $pid = pcntl_waitpid(-1, $st, WNOHANG);
            if ($pid > 0 && isset($pidMap[$pid])) {
                $left--;
            } elseif ($pid === 0) {
                usleep(50);
            }
        }

        $partials = [];
        for ($w = 0; $w < $forked; $w++) {
            $raw = file_get_contents($tmpFiles[$w]);
            @unlink($tmpFiles[$w]);
            if ($raw !== false) {
                $p = $igb ? igbinary_unserialize($raw) : unserialize($raw);
                if (is_array($p)) $partials[$w] = $p;
            }
        }

        for ($w = $forked; $w < $workerCount; $w++) {
            @unlink($tmpFiles[$w]);
            $partials[$w] = $this->segment($inputPath, $segments[$w][0], $segments[$w][1]);
        }

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

    // ----------------------------------------------------------------
    //  Segment planning
    //
    //  Splits the input into newline-aligned byte ranges that can be
    //  processed independently by either parallel strategy.
    // ----------------------------------------------------------------

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
    //  Segment parsing
    //
    //  Shared by single-threaded execution and the `pcntl_fork()` path.
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
    //  Line extraction
    //
    //  Parses complete lines from an in-memory buffer and updates the
    //  aggregated visit counts keyed by escaped path and date.
    // ----------------------------------------------------------------

    private function extract(string $buf, array &$stats): void
    {
        static $pathCache = [];

        $offset = 0;
        $len = strlen($buf);

        while ($offset < $len) {
            $nl = strpos($buf, "\n", $offset);
            if ($nl === false) $nl = $len;

            $comma = strpos($buf, ',', $offset + 19);

            if ($comma !== false && $comma < $nl) {
                $rawPath = substr($buf, $offset + 19, $comma - $offset - 19);
                $date = substr($buf, $comma + 1, 10);

                if (isset($pathCache[$rawPath])) {
                    $path = $pathCache[$rawPath];
                } else {
                    $path = str_replace('/', '\\/', $rawPath);
                    $pathCache[$rawPath] = $path;
                }

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
    //  JSON output
    //
    //  Streams the final structure directly to disk using the required
    //  pretty-printed layout.
    // ----------------------------------------------------------------

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

            $buf .= '    "' . $pathKey . "\": {\n";

            $dateKeys = array_keys($dates);
            $lastDate = count($dateKeys) - 1;

            for ($d = 0; $d <= $lastDate; $d++) {
                $buf .= '        "' . $dateKeys[$d] . '": ' . $dates[$dateKeys[$d]];
                $buf .= ($d < $lastDate) ? ",\n" : "\n";
            }

            $buf .= ($p < $lastPath) ? "    },\n" : "    }\n";

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
