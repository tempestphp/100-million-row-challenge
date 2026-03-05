<?php

declare(strict_types=1);

namespace App;

use Exception;
use function Tempest\Support\Str\length;
final class Parser
{
    const TAIL_LENGTH = 26;
    const DATE_START_FROM_RIGHT = 25;

    private array $slugToId     = [];
    private array $idToPath     = [];
    private array $dateMap      = [];
    private array $intToDateMap = [];
    private int   $cpuCores;

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

        $fh        = \fopen($inputPath, 'rb');
        $firstLine = \fgets($fh);
        \fclose($fh);

        $blogPos    = \strpos($firstLine, '/blog/');
        $pathOffset = ($blogPos === false) 
            ? \strpos($firstLine, '/', 8) + 1 
            : $blogPos + 6;
        
        $this->parseWithFork($inputPath, $outputPath, $fileSize, $chunkSize, $pathOffset);
    }

    private function parseWithFork($inputPath, $outputPath, $fileSize, $chunkSize, $pathOffset): void 
    {
        $useIgbinary = \function_exists('igbinary_serialize');
        $sockets     = [];
        $pids        = [];

        for ($i = 0; $i < $this->cpuCores; $i++) {
            $pair  = \stream_socket_pair(\STREAM_PF_UNIX, \STREAM_SOCK_STREAM, 0);
            $start = $i * $chunkSize;
            $end   = ($i === $this->cpuCores - 1) ? $fileSize : ($start + $chunkSize);

            $pid = \pcntl_fork();
            if ($pid === 0) {
                \fclose($pair[0]);
                $partial = $this->processChunk($inputPath, $start, $end, $pathOffset);
                $payload = $useIgbinary 
                    ? \igbinary_serialize($partial) 
                    : \serialize($partial);
                
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
            $read = $pending; $write = $except = null;
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
                    $partial = $useIgbinary 
                        ? \igbinary_unserialize($state[$i]['buf']) 
                        : \unserialize($state[$i]['buf']);
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

        if ($start !== 0) {
            // Seek to one byte BEFORE start, read that byte
            // If it's \n, we're already at a line boundary — no skip needed
            // If it's not \n, we're mid-line — skip to next \n with fgets
            \fseek($handle, $start - 1);
            $prevByte = \fgetc($handle);
            // fgetc advances position to $start
            // If previous byte was NOT \n, we're mid-line, skip remainder
            if ($prevByte !== "\n") {
                \fgets($handle);
            }
            // If previous byte WAS \n, ftell is now exactly $start — perfect
        }
        // start === 0: read from beginning, no alignment needed

        $results  = [];
        $dateMap  = $this->dateMap;
        $slugToId = $this->slugToId;

        while (\ftell($handle) < $end) {
            $line = \fgets($handle);
            if ($line === false) break;

            $line = \rtrim($line);
            if (\strlen($line) < 30) continue;

            $slug    = \substr($line, $pathOffset, -self::TAIL_LENGTH);
            $dateStr = \substr($line, -self::DATE_START_FROM_RIGHT, 10);

            $dateInt = $dateMap[$dateStr] ?? null;
            $id      = $slugToId[$slug]   ?? null;
            if ($id === null || $dateInt === null) continue;

            $key = ($dateInt * 1000) + $id;
            $results[$key] = ($results[$key] ?? 0) + 1;
        }

        \fclose($handle);
        return $results;
    }

    private function writeOutput(array &$final, string $outputPath): array
    {
        $expanded  = [];
        $intToDate = $this->intToDateMap;
        $idToPath  = $this->idToPath;

        foreach ($final as $packedKey => $count) {
            $id        = $packedKey % 1000;
            $dateIndex = (int)($packedKey / 1000);
            $dateStr   = $intToDate[$dateIndex] ?? 'unknown-date';
            $path      = $idToPath[$id] ?? '/blog/unknown';
            $expanded[$path][$dateStr] = $count;
        }

        foreach ($expanded as &$dates) \ksort($dates);

        \file_put_contents($outputPath, \json_encode($expanded, \JSON_PRETTY_PRINT));
        return $expanded;
    }

    private function prepareMapping(): void
    {
        $prefix = 'https://stitcher.io/blog/';
        if (!\class_exists('\App\Commands\Visit')) return;

        foreach (\App\Commands\Visit::all() as $index => $visit) {
            $slug = \str_replace($prefix, '', $visit->uri);
            $this->slugToId[$slug]  = $index;
            $this->idToPath[$index] = '/blog/' . $slug;
        }
    }

    private function prepareDateMap(): void
    {
        $current = new \DateTimeImmutable('2021-01-01');
        $end     = new \DateTimeImmutable('2027-01-01');
        $index   = 0;
        while ($current < $end) {
            $dateStr = $current->format('Y-m-d');
            $this->dateMap[$dateStr]    = $index;
            $this->intToDateMap[$index] = $dateStr;
            $current = $current->modify('+1 day');
            $index++;
        }
    }
}
