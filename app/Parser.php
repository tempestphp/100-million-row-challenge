<?php

declare(strict_types=1);

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        ini_set('memory_limit', '4G');
        gc_disable();

        $cpuCores  = $this->detectCores();
        $fileSize  = filesize($inputPath);
        $chunkSize = (int) ceil($fileSize / $cpuCores);
        $sockets   = [];
        $pids      = [];

        for ($i = 0; $i < $cpuCores; $i++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            if ($pair === false) throw new Exception("socket_pair failed");

            $start = $i * $chunkSize;
            $end   = ($i === $cpuCores - 1) ? $fileSize : ($start + $chunkSize);

            $pid = pcntl_fork();
            if ($pid === -1) throw new Exception("fork failed");

            if ($pid === 0) {
                fclose($pair[0]);
                $partial = $this->processChunk($inputPath, $start, $end);
                $payload = serialize($partial);
                unset($partial);
                fwrite($pair[1], pack('N', strlen($payload)));
                $off = 0; $tot = strlen($payload);
                while ($off < $tot) {
                    $w = fwrite($pair[1], substr($payload, $off, 65536));
                    if ($w === false || $w === 0) break;
                    $off += $w;
                }
                fclose($pair[1]);
                exit(0);
            }

            fclose($pair[1]);
            $pids[]    = $pid;
            $sockets[] = $pair[0];
        }

        // ── Read all child results ─────────────────────────────────────────
        $final = [];
        foreach ($sockets as $sock) {
            $hdr = '';
            while (strlen($hdr) < 4) {
                $c = fread($sock, 4 - strlen($hdr));
                if ($c === false || $c === '') break;
                $hdr .= $c;
            }
            $len = unpack('N', $hdr)[1];
            $buf = '';
            while (strlen($buf) < $len) {
                $c = fread($sock, min(65536, $len - strlen($buf)));
                if ($c === false || $c === '') break;
                $buf .= $c;
            }
            fclose($sock);

            // Merge directly during receive — avoid building intermediate array
            $partial = unserialize($buf);
            foreach ($partial as $path => $dates) {
                if (!isset($final[$path])) {
                    $final[$path] = $dates;
                } else {
                    foreach ($dates as $date => $cnt) {
                        if (isset($final[$path][$date])) {
                            $final[$path][$date] += $cnt;
                        } else {
                            $final[$path][$date] = $cnt;
                        }
                    }
                }
            }
        }

        foreach ($pids as $pid) pcntl_waitpid($pid, $status);

        foreach ($final as &$dates) ksort($dates);
        unset($dates);

        file_put_contents($outputPath, json_encode($final, JSON_PRETTY_PRINT));
    }

    private function processChunk(string $filePath, int $start, int $end): array
    {
        $handle = fopen($filePath, 'rb');

        $results    = [];
        $pathCache  = [];
        $dateCache  = [];
        $pathOffset = null;

        // Seek to chunk start, skip partial line
        if ($start !== 0) {
            fseek($handle, $start - 1);
            fgets($handle);
        }

        // STRLEN TRICK: All lines end with "YYYY-MM-DDTHH:MM:SS+00:00\n" = 27 chars.
        // The comma is always at strlen($line) - 27.
        // This replaces strpos($line, ',') which scans ~45 bytes per line.
        // strlen() is O(1) — PHP stores string length in the zval struct.
        // Verified correct on this dataset (profile5 test C confirmed match).
        //
        // STRING INTERNING: $pathCache[$raw] ??= $raw ensures PHP reuses the
        // same string zval for repeated paths, reducing hash table overhead
        // and GC pressure across 100M iterations.

        while (ftell($handle) < $end && ($line = fgets($handle)) !== false) {
            $cp = strlen($line) - 27;

            if ($pathOffset === null) {
                $pathOffset = strpos($line, '/', 8);
                if ($pathOffset === false) continue;
            }

            $rawPath = substr($line, $pathOffset, $cp - $pathOffset);
            $rawDate = substr($line, $cp + 1, 10);

            $path = $pathCache[$rawPath] ??= $rawPath;
            $date = $dateCache[$rawDate] ??= $rawDate;

            if (isset($results[$path][$date])) {
                $results[$path][$date]++;
            } else {
                $results[$path][$date] = 1;
            }
        }

        fclose($handle);
        return $results;
    }

    private function detectCores(): int
    {
        $n = (int) shell_exec('sysctl -n hw.logicalcpu 2>/dev/null');
        if ($n > 0) return $n;
        $n = (int) shell_exec('nproc 2>/dev/null');
        if ($n > 0) return $n;
        return 8;
    }
}