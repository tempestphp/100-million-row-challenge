<?php

declare(strict_types=1);

namespace App;

use Exception;
use function Tempest\Support\Str\length;

final class Parser
{
    // After rtrim(), the tail is: ,YYYY-MM-DDTHH:MM:SS+00:00
    // Tail length = 1 (comma) + 10 (date) + 1 (T) + 8 (time) + 6 (offset) = 26 chars
    const TAIL_LENGTH = 26; 
    const DATE_START_FROM_RIGHT = 25; // Skips the comma

    private array $slugToId = [];
    private array $idToPath = [];
    private array $dateMap = [];
    private array $intToDateMap = [];
    private int $cpuCores;

    public function __construct()
    {
        $cores = (int) shell_exec('nproc 2>/dev/null || sysctl -n hw.logicalcpu 2>/dev/null || echo 8');
        $this->cpuCores = max(1, min($cores, 16));
    }

    public function parse(string $inputPath, string $outputPath): void
    {   
        \ini_set('memory_limit', '4G');
        \gc_disable();

        $this->prepareMapping();
        $this->prepareDateMap();

        $fileSize  = \filesize($inputPath);
        $chunkSize = (int) \ceil($fileSize / $this->cpuCores);

        $fh = \fopen($inputPath, 'rb');
        $firstLine = \fgets($fh);
        \fclose($fh);

        // Calculate offset to the start of the slug
        $blogPos = \strpos($firstLine, '/blog/');
        $pathOffset = ($blogPos === false) ? \strpos($firstLine, '/', 8) + 1 : $blogPos + 6;
        
        $this->parseWithFork($inputPath, $outputPath, $fileSize, $chunkSize, $pathOffset);
    }

    private function parseWithFork($inputPath, $outputPath, $fileSize, $chunkSize, $pathOffset): void 
    {
        $useIgbinary = \function_exists('igbinary_serialize');
        $sockets = [];
        $pids    = [];

        for ($i = 0; $i < $this->cpuCores; $i++) {
            $pair = \stream_socket_pair(\STREAM_PF_UNIX, \STREAM_SOCK_STREAM, 0);
            $start = $i * $chunkSize;
            $end   = ($i === $this->cpuCores - 1) ? $fileSize : ($start + $chunkSize);

            $pid = \pcntl_fork();
            if ($pid === 0) {
                \fclose($pair[0]);
                $partial = $this->processChunk($inputPath, $start, $end, $pathOffset);
                $payload = $useIgbinary ? \igbinary_serialize($partial) : \serialize($partial);
                
                \fwrite($pair[1], \pack('N', \strlen($payload)));
                \fwrite($pair[1], $payload);
                \fclose($pair[1]);
                exit(0);
            }
            \fclose($pair[1]);
            $sockets[] = $pair[0];
            $pids[]    = $pid;
        }

        $final = [];
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
                if ($state[$i]['len'] === null) {
                    $state[$i]['hdr'] .= \fread($sock, 4 - \strlen($state[$i]['hdr']));
                    if (\strlen($state[$i]['hdr']) < 4) continue;
                    $state[$i]['len'] = \unpack('N', $state[$i]['hdr'])[1];
                }

                $remaining = $state[$i]['len'] - \strlen($state[$i]['buf']);
                if ($remaining > 0) {
                    $state[$i]['buf'] .= \fread($sock, \min(2 * 1024 * 1024, $remaining));
                }

                if (\strlen($state[$i]['buf']) >= $state[$i]['len']) {
                    \fclose($sock);
                    $pending = \array_values(\array_filter($pending, fn($s) => $s !== $sock));
                    $partial = $useIgbinary ? \igbinary_unserialize($state[$i]['buf']) : \unserialize($state[$i]['buf']);
                    unset($state[$i]['buf']);
                    
                    foreach ($partial as $key => $count) {
                        $final[$key] = ($final[$key] ?? 0) + $count;
                    }
                    unset($partial);
                }
            }
        }

        foreach ($pids as $pid) \pcntl_waitpid($pid, $status);
        $this->writeOutput($final, $outputPath);
    }

    private function processChunk(string $filePath, int $start, int $end, int $pathOffset): array
    {
        $handle = \fopen($filePath, 'rb');
        \fseek($handle, $start);
        
        if ($start !== 0) {
            \fseek($handle, $start - 1);
            if (\fgetc($handle) !== "\n") {
                \fgets($handle); 
            }
        }

        $results = [];
        $dateMap = $this->dateMap; // Local copy for faster access
        while (\ftell($handle) < $end) {
            $line = \fgets($handle);
            if ($line === false) break;

            $line = \rtrim($line);
            if (\strlen($line) < 30) continue; 

            $slug = \substr($line, $pathOffset, -self::TAIL_LENGTH);
            $dateStr = \substr($line, -self::DATE_START_FROM_RIGHT, 10); // "2026-03-05"
            
            // Convert "2026-03-05" to 20260305
            $dateInt = $dateMap[$dateStr] ?? null;
            
            $id = $this->slugToId[$slug] ?? null;
            if ($id === null) continue;

            // Pack key: Date + ID
            // Using 1000 as multiplier assumes IDs won't exceed 999
            $key = ($dateInt * 1000) + $id;

            if (isset($results[$key])) {
                $results[$key]++;
            } else {
                $results[$key] = 1;
            }
        }

        \fclose($handle);
        return $results;
    }

    private function writeOutput(array &$final, string $outputPath): array
    {
        $expanded = [];
        $intToDate = $this->intToDateMap;
        $idToPath = $this->idToPath;
        foreach ($final as $packedKey => $count) {
            // Unpack
            $id = $packedKey % 1000;
            $dateInt = (int)($packedKey / 1000);
            
            // Reformat date: 20260305 -> "2026-03-05"
            $dateStr = $intToDate[$dateInt] ?? 'unknown-date';

            $path = $this->idToPath[$id] ?? '/blog/unknown';
            $expanded[$path][$dateStr] = $count;
        }

        foreach ($expanded as &$dates) {
            \ksort($dates);
        }

        \file_put_contents($outputPath, \json_encode($expanded, \JSON_PRETTY_PRINT));
        return $expanded;
    }

    private function prepareMapping(): void
    {
        $prefix = 'https://stitcher.io/blog/';
        if (!class_exists('\App\Commands\Visit')) return;

        foreach (\App\Commands\Visit::all() as $index => $visit) {
            $slug = \str_replace($prefix, '', $visit->uri);
            $this->slugToId[$slug] = $index;
            $this->idToPath[$index] = '/blog/' . $slug;
        }
    }

    private function prepareDateMap(): void
    {
        // Pre-compute date maps for Feb 2021 - Feb 2027
        $current = new \DateTimeImmutable('2021-01-01');
        $end = new \DateTimeImmutable('2027-01-01');
        while ($current < $end) {
            $dateStr = $current->format('Y-m-d');
            $dateInt = (int)$current->format('Ymd');
            $this->dateMap[$dateStr] = $dateInt;
            $this->intToDateMap[$dateInt] = $dateStr;
            $current = $current->modify('+1 day');
        }
    }
}
