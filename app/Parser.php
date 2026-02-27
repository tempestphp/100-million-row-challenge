<?php

namespace App;

use Exception;

final class Parser
{
    private const WORKERS = 8;
    private const BUF_SIZE = 16 * 1024 * 1024; // 16 MB

    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);

        if ($fileSize === 0) {
            file_put_contents($outputPath, '{}');
            return;
        }

        // Pre-compute line-aligned chunk start/end byte offsets
        $fh = fopen($inputPath, 'rb');
        $starts = [0];
        for ($i = 1; $i < self::WORKERS; $i++) {
            fseek($fh, (int) ($i * $fileSize / self::WORKERS));
            fgets($fh); // skip partial line to align to next full line
            $starts[$i] = ftell($fh);
        }
        fclose($fh);

        $ends = [];
        for ($i = 0; $i < self::WORKERS - 1; $i++) {
            $ends[$i] = $starts[$i + 1];
        }
        $ends[self::WORKERS - 1] = $fileSize;

        // Temp file paths (PID-namespaced to avoid collisions)
        $myPid = getmypid();
        $tmpFiles = [];
        for ($i = 0; $i < self::WORKERS; $i++) {
            $tmpFiles[$i] = sys_get_temp_dir() . "/parser_{$myPid}_{$i}.tmp";
        }

        $useIgbinary = function_exists('igbinary_serialize');

        // Fork workers
        $pids = [];
        for ($i = 0; $i < self::WORKERS; $i++) {
            if ($starts[$i] >= $ends[$i]) {
                // Empty chunk (file smaller than WORKERS lines)
                $empty = $useIgbinary ? igbinary_serialize([]) : serialize([]);
                file_put_contents($tmpFiles[$i], $empty);
                continue;
            }

            $pid = pcntl_fork();
            if ($pid === -1) {
                throw new Exception('pcntl_fork() failed');
            }
            if ($pid === 0) {
                // Child: process this chunk and exit
                $this->processChunk($inputPath, $starts[$i], $ends[$i], $tmpFiles[$i], $useIgbinary);
                exit(0);
            }
            $pids[$i] = $pid;
        }

        // Wait for all children
        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        // Merge worker results in order 0..WORKERS-1 (preserves first-seen URL ordering)
        $merged = [];
        for ($i = 0; $i < self::WORKERS; $i++) {
            $raw = file_get_contents($tmpFiles[$i]);
            $wdata = $useIgbinary ? igbinary_unserialize($raw) : unserialize($raw);
            unlink($tmpFiles[$i]);
            foreach ($wdata as $path => $dates) {
                foreach ($dates as $date => $count) {
                    if (isset($merged[$path][$date])) {
                        $merged[$path][$date] += $count;
                    } else {
                        $merged[$path][$date] = $count;
                    }
                }
            }
        }

        // Sort dates ascending within each URL
        foreach ($merged as $path => $_) {
            ksort($merged[$path]);
        }

        file_put_contents($outputPath, json_encode($merged, JSON_PRETTY_PRINT));
    }

    private function processChunk(string $inputPath, int $start, int $end, string $tmpFile, bool $useIgbinary): void
    {
        $fh = fopen($inputPath, 'rb');
        fseek($fh, $start);
        $toRead = $end - $start;
        $leftover = '';
        $data = [];

        while ($toRead > 0) {
            $raw = fread($fh, min(self::BUF_SIZE, $toRead));
            $toRead -= strlen($raw);

            // Handle junction line: leftover bytes from previous buffer + start of $raw
            if ($leftover !== '') {
                $firstNL = strpos($raw, "\n");
                if ($firstNL !== false) {
                    $jl = $leftover . substr($raw, 0, $firstNL);
                    $jLen = strlen($jl);
                    if ($jLen >= 45) {
                        // path: offset 19 (https://stitcher.io = 19 chars), length = jLen - 45
                        // date: offset jLen - 25 (commaPos + 1 = jLen - 26 + 1), length = 10
                        $path = substr($jl, 19, $jLen - 45);
                        $date = substr($jl, $jLen - 25, 10);
                        if (isset($data[$path][$date])) {
                            $data[$path][$date]++;
                        } else {
                            $data[$path][$date] = 1;
                        }
                    }
                    $scanStart = $firstNL + 1;
                } else {
                    // Entire raw is still part of the leftover partial line
                    $leftover .= $raw;
                    continue;
                }
            } else {
                $scanStart = 0;
            }

            // New leftover: bytes after last \n in $raw
            $lastNL = strrpos($raw, "\n");
            $leftover = $lastNL !== false ? substr($raw, $lastNL + 1) : $raw;

            // Scan $raw directly — no concat, no explode, no intermediate array
            $pos = $scanStart;
            while (($nlPos = strpos($raw, "\n", $pos)) !== false && $nlPos <= $lastNL) {
                $lineLen = $nlPos - $pos;
                if ($lineLen >= 45) {
                    // URL base 'https://stitcher.io' is always 19 bytes (fixed offset)
                    // comma always at lineLen - 26 (timestamp = 25 chars + 1 comma)
                    // path: [pos+19 .. pos+lineLen-27], length = lineLen - 45
                    // date: [pos+lineLen-25 .. pos+lineLen-16], length = 10
                    $path = substr($raw, $pos + 19, $lineLen - 45);
                    $date = substr($raw, $pos + $lineLen - 25, 10);
                    if (isset($data[$path][$date])) {
                        $data[$path][$date]++;
                    } else {
                        $data[$path][$date] = 1;
                    }
                }
                $pos = $nlPos + 1;
            }
        }

        fclose($fh);

        // Final leftover: partial line at chunk end (no trailing \n)
        if ($leftover !== '') {
            $len = strlen($leftover);
            if ($len >= 45) {
                $path = substr($leftover, 19, $len - 45);
                $date = substr($leftover, $len - 25, 10);
                if (isset($data[$path][$date])) {
                    $data[$path][$date]++;
                } else {
                    $data[$path][$date] = 1;
                }
            }
        }

        $payload = $useIgbinary ? igbinary_serialize($data) : serialize($data);
        file_put_contents($tmpFile, $payload);
    }
}
