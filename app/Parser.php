<?php

declare(strict_types=1);

namespace App;

use Exception;
use function Tempest\Support\Str\length;

final class Parser
{
    const DATE_OFFSET_FROM_END = 26;
    const PATH_TRIM_FROM_END   = 27;

    // Tune based on benchmark server (2 vCPUs / M1 has 8 perf cores — use what's available)
    private int $cpuCores;

    public function __construct()
    {
        // Detect logical CPU count; fall back to 8
        $cores = (int) shell_exec('nproc 2>/dev/null || sysctl -n hw.logicalcpu 2>/dev/null || echo 8');
        $this->cpuCores = max(1, min($cores, 16));
    }

    public function parse(string $inputPath, string $outputPath): void
    {
        \ini_set('memory_limit', '4G');
        \gc_disable();

        $fileSize  = \filesize($inputPath);
        $chunkSize = (int) \ceil($fileSize / $this->cpuCores);

        // Detect path offset ONCE from first line
        $fh        = \fopen($inputPath, 'rb');
        $firstLine = \fgets($fh);
        \fclose($fh);
        $pathOffset = \strpos($firstLine, '/', 8); // skip "https://"
        if ($pathOffset === false) {
            throw new Exception("Cannot detect path offset from first line: $firstLine");
        }

        // ── Use parallel extension if available (zero-copy, no sockets) ──────────
        if (\extension_loaded('parallel')) {
            $this->parseWithParallel($inputPath, $outputPath, $fileSize, $chunkSize, $pathOffset);
            return;
        }

        // ── Fallback: fork + Unix socket IPC ─────────────────────────────────────
        $this->parseWithFork($inputPath, $outputPath, $fileSize, $chunkSize, $pathOffset);
    }

    // ── parallel-based implementation ─────────────────────────────────────────────
    private function parseWithParallel(
        string $inputPath,
        string $outputPath,
        int    $fileSize,
        int    $chunkSize,
        int    $pathOffset,
    ): void {
        $channels = [];
        $runtimes = [];

        $workerCode = static function (
            string $filePath,
            int    $start,
            int    $end,
            int    $pathOffset,
            \parallel\Channel $channel,
        ): void {
            $handle  = \fopen($filePath, 'rb');
            $results = [];
            $pCache  = [];
            $dCache  = [];

            \stream_set_read_buffer($handle, 8 * 1024 * 1024);

            if ($start !== 0) {
                \fseek($handle, $start - 1);
                \fgets($handle);
            }

            $dateOff   = 26;
            $pathTrim  = 27;
            $pos       = \ftell($handle);

            while ($pos < $end && ($line = \fgets($handle)) !== false) {
                $pos += \strlen($line);
                $rp  = \substr($line, $pathOffset, -$pathTrim);
                $rd  = \substr($line, -$dateOff, 10);
                $p   = $pCache[$rp] ?? ($pCache[$rp] = $rp);
                $d   = $dCache[$rd] ?? ($dCache[$rd] = $rd);
                $results[$p][$d] = ($results[$p][$d] ?? 0) + 1;
            }

            \fclose($handle);
            $channel->send($results);
        };

        for ($i = 0; $i < $this->cpuCores; $i++) {
            $ch          = \parallel\Channel::make("worker_$i");
            $channels[]  = $ch;
            $start       = $i * $chunkSize;
            $end         = ($i === $this->cpuCores - 1) ? $fileSize : ($start + $chunkSize);
            $runtimes[]  = \parallel\run($workerCode, [$inputPath, $start, $end, $pathOffset, $ch]);
        }

        $final = [];
        foreach ($channels as $ch) {
            $partial = $ch->recv();
            foreach ($partial as $path => $dates) {
                if (!isset($final[$path])) {
                    $final[$path] = $dates;
                } else {
                    foreach ($dates as $date => $cnt) {
                        $final[$path][$date] = ($final[$path][$date] ?? 0) + $cnt;
                    }
                }
            }
        }

        foreach ($runtimes as $r) $r->value(); // join

        $this->writeOutput($final, $outputPath);
    }

    // ── fork + socket IPC (primary path without parallel) ────────────────────────
    private function parseWithFork(
        string $inputPath,
        string $outputPath,
        int    $fileSize,
        int    $chunkSize,
        int    $pathOffset,
    ): void {
        $useIgbinary = \function_exists('igbinary_serialize');

        $sockets = [];
        $pids    = [];

        for ($i = 0; $i < $this->cpuCores; $i++) {
            $pair = \stream_socket_pair(\STREAM_PF_UNIX, \STREAM_SOCK_STREAM, 0);
            if ($pair === false) throw new Exception("stream_socket_pair failed");

            $start = $i * $chunkSize;
            $end   = ($i === $this->cpuCores - 1) ? $fileSize : ($start + $chunkSize);

            $pid = \pcntl_fork();
            if ($pid === -1) throw new Exception("pcntl_fork failed");

            if ($pid === 0) {
                // ── child ──────────────────────────────────────────────────────
                \fclose($pair[0]);

                $partial = $this->processChunk($inputPath, $start, $end, $pathOffset);

                $payload = $useIgbinary
                    ? \igbinary_serialize($partial)
                    : \serialize($partial);

                unset($partial);

                // Write length-prefixed payload
                \fwrite($pair[1], \pack('N', \strlen($payload)));
                $off = 0;
                $tot = \strlen($payload);
                while ($off < $tot) {
                    $written = \fwrite($pair[1], \substr($payload, $off, 524288)); // 512 KB slices
                    if ($written === false || $written === 0) break;
                    $off += $written;
                }
                \fclose($pair[1]);
                exit(0);
            }

            \fclose($pair[1]);
            $sockets[] = $pair[0];
            $pids[]    = $pid;
        }

        // ── parent: drain all sockets concurrently with stream_select ────────────
        $final = [];

        // Non-blocking mode; per-socket state: header accumulator, expected length, payload buffer
        $state = [];
        foreach ($sockets as $i => $sock) {
            \stream_set_blocking($sock, false);
            $state[$i] = ['hdr' => '', 'len' => null, 'buf' => ''];
        }

        $pending = $sockets;

        while (!empty($pending)) {
            $read = $pending;
            $write = $except = null;
            if (\stream_select($read, $write, $except, 5) === false) break;

            foreach ($read as $sock) {
                $i = \array_search($sock, $sockets, true);

                // Read header (4 bytes)
                if ($state[$i]['len'] === null) {
                    $state[$i]['hdr'] .= \fread($sock, 4 - \strlen($state[$i]['hdr']));
                    if (\strlen($state[$i]['hdr']) < 4) continue;
                    $state[$i]['len'] = \unpack('N', $state[$i]['hdr'])[1];
                }

                // Read payload in large chunks
                $remaining = $state[$i]['len'] - \strlen($state[$i]['buf']);
                if ($remaining > 0) {
                    $chunk = \fread($sock, \min(2097152, $remaining)); // 2 MB reads
                    if ($chunk !== false && $chunk !== '') {
                        $state[$i]['buf'] .= $chunk;
                    }
                }

                // Complete — deserialize + merge + free immediately
                if (\strlen($state[$i]['buf']) >= $state[$i]['len']) {
                    \fclose($sock);
                    $pending = \array_values(\array_filter($pending, fn($s) => $s !== $sock));

                    $partial = $useIgbinary
                        ? \igbinary_unserialize($state[$i]['buf'])
                        : \unserialize($state[$i]['buf']);

                    unset($state[$i]);

                    foreach ($partial as $path => $dates) {
                        if (!isset($final[$path])) {
                            $final[$path] = $dates;
                        } else {
                            foreach ($dates as $date => $cnt) {
                                $final[$path][$date] = ($final[$path][$date] ?? 0) + $cnt;
                            }
                        }
                    }
                    unset($partial);
                }
            }
        }

        foreach ($pids as $pid) \pcntl_waitpid($pid, $status);

        $this->writeOutput($final, $outputPath);
    }

    // ── Shared chunk processor ────────────────────────────────────────────────────
    private function processChunk(string $filePath, int $start, int $end, int $pathOffset): array
    {
        $handle = \fopen($filePath, 'rb');
        \stream_set_read_buffer($handle, 8 * 1024 * 1024);

        if ($start !== 0) {
            \fseek($handle, $start - 1);
            \fgets($handle);
        }

        $results = [];
        $pCache  = [];
        $dCache  = [];
        $pos     = \ftell($handle);

        while ($pos < $end && ($line = \fgets($handle)) !== false) {
            $pos += \strlen($line);
            $rp   = \substr($line, $pathOffset, -self::PATH_TRIM_FROM_END);
            $rd   = \substr($line, -self::DATE_OFFSET_FROM_END, 10);
            $p    = $pCache[$rp] ?? ($pCache[$rp] = $rp);
            $d    = $dCache[$rd] ?? ($dCache[$rd] = $rd);
            $results[$p][$d] = ($results[$p][$d] ?? 0) + 1;
        }

        \fclose($handle);
        return $results;
    }

    // ── Output: sort + encode ─────────────────────────────────────────────────────
    private function writeOutput(array &$final, string $outputPath): void
    {
        foreach ($final as &$dates) {
            \ksort($dates);
        }
        unset($dates);

        \file_put_contents(
            $outputPath,
            \json_encode($final, \JSON_PRETTY_PRINT),
        );

        \gc_enable();
    }
}
