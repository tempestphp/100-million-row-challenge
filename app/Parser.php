<?php

namespace App;

use Exception;

final class Parser
{
    private const WORKERS = 8;
    private const BUF_SIZE = 8 * 1024 * 1024; // 8 MB

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

        // Fork workers
        $pids = [];
        for ($i = 0; $i < self::WORKERS; $i++) {
            if ($starts[$i] >= $ends[$i]) {
                // Empty chunk (file smaller than WORKERS lines)
                file_put_contents($tmpFiles[$i], serialize([]));
                continue;
            }

            $pid = pcntl_fork();
            if ($pid === -1) {
                throw new Exception('pcntl_fork() failed');
            }
            if ($pid === 0) {
                // Child: process this chunk and exit
                $this->processChunk($inputPath, $starts[$i], $ends[$i], $tmpFiles[$i]);
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
            $wdata = unserialize(file_get_contents($tmpFiles[$i]));
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

    private function processChunk(string $inputPath, int $start, int $end, string $tmpFile): void
    {
        $fh = fopen($inputPath, 'rb');
        fseek($fh, $start);
        $toRead = $end - $start;
        $leftover = '';
        $data = [];

        while ($toRead > 0) {
            $raw = fread($fh, min(self::BUF_SIZE, $toRead));
            $toRead -= strlen($raw); // actual bytes read (may be less than requested)

            $buf = $leftover . $raw;
            $lastNL = strrpos($buf, "\n");

            if ($lastNL === false) {
                $leftover = $buf;
                continue;
            }

            $leftover = substr($buf, $lastNL + 1);
            $lines = explode("\n", substr($buf, 0, $lastNL + 1));
            $lineCount = count($lines) - 1; // last element is always '' (buf ends with \n)

            for ($i = 0; $i < $lineCount; $i++) {
                $l = $lines[$i];
                $len = strlen($l);
                if ($len < 27) {
                    // guard against empty/malformed lines
                    continue;
                }

                $commaPos = $len - 26; // timestamp always 25 chars + comma = 26 from end
                $pathStart = strpos($l, '/', 8); // 3rd slash: skip past 'https://'
                $path = substr($l, $pathStart, $commaPos - $pathStart);
                $date = substr($l, $commaPos + 1, 10);

                if (isset($data[$path][$date])) {
                    $data[$path][$date]++;
                } else {
                    $data[$path][$date] = 1;
                }
            }
        }

        fclose($fh);

        // Process any leftover bytes (partial line at chunk end, no trailing \n)
        if ($leftover !== '') {
            $len = strlen($leftover);
            if ($len >= 27) {
                $commaPos = $len - 26;
                $pathStart = strpos($leftover, '/', 8);
                $path = substr($leftover, $pathStart, $commaPos - $pathStart);
                $date = substr($leftover, $commaPos + 1, 10);
                if (isset($data[$path][$date])) {
                    $data[$path][$date]++;
                } else {
                    $data[$path][$date] = 1;
                }
            }
        }

        file_put_contents($tmpFile, serialize($data));
    }

}
