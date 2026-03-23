<?php

namespace App;

use App\Commands\Visit;
use RuntimeException;

use function array_fill;
use function array_slice;
use function array_values;
use function count;
use function fclose;
use function fgets;
use function file_get_contents;
use function file_put_contents;
use function filesize;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function intdiv;
use function json_encode;
use function min;
use function pack;
use function pcntl_fork;
use function pcntl_waitpid;
use function rtrim;
use function shell_exec;
use function sprintf;
use function strlen;
use function strpos;
use function substr;
use function unlink;
use function unpack;

final class Parser
{
    private int $availableWorkers = 8;

    private const NUM_DATE_SLOTS = 3328;

    private array $urlToIndex = [];

    private array $indexToUrl = [];

    private int $flatMapSize;

    private array $encounterOrder = [];

    private array $seen = [];

    public function parse(string $inputPath, string $outputPath): void
    {
        $this->availableWorkers = $this->getCoreCount();
        $pids = [];

        for ($i = 0; $i < $this->availableWorkers; $i++) {
            $pid = pcntl_fork();

            if ($pid === -1) {
                throw new RuntimeException('Fork failed.');
            }

            if ($pid === 0) {
                $this->builUrlVisitsMap();
                $this->processChunk($inputPath, $i);
                exit;
            }

            $pids[] = $pid;
        }

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $this->mergeOutputChunks($outputPath);
    }

    private function processChunk(string $filePath, int $index): void
    {
        $fileSize = filesize($filePath);
        $chunkSize = (int) ($fileSize / $this->availableWorkers);

        $start = $index * $chunkSize;
        $end = ($index === $this->availableWorkers - 1) ? $fileSize : $start + $chunkSize;

        $handle = fopen($filePath, 'r');
        fseek($handle, $start);

        if ($start !== 0) {
            fgets($handle);
        }

        $urlMap = array_fill(0, $this->flatMapSize, 0);
        $bufferSize = 8 * 1024 * 1024;
        $bytesRead = 0;
        $chunkLength = $end - ftell($handle);
        $leftover = '';

        while ($bytesRead < $chunkLength) {
            $readSize = min($bufferSize, $chunkLength - $bytesRead);
            $chunk = fread($handle, $readSize);

            if ($chunk === false || $chunk === '') {
                break;
            }

            $bytesRead += strlen($chunk);

            $this->readChunk(
                urlMap: $urlMap,
                chunk: $chunk,
                leftover: $leftover,
            );
        }

        if ($leftover !== '') {
            $rest = fgets($handle);
            if($rest !== false) {
                $leftover .= rtrim($rest, "\r\n");
            }

            $this->parseCsvRow($urlMap, $leftover);
        }

        $this->writeWorkerChunk($index, $urlMap);
        fclose($handle);

        exit;
    }

    private function writeWorkerChunk(int $index, array $map): void
    {
        $tempPath = sprintf('%s/../tmp/p%s_worker.bin', __DIR__, $index);
        $n = count($this->encounterOrder);
        $binary = pack('V', $n);
        if ($n > 0) {
            $binary .= pack('V*', ...$this->encounterOrder);
        }
        for ($i = 0; $i < $this->flatMapSize; $i += 5000) {
            $binary .= pack('V*', ...array_slice($map, $i, 5000));
        }
        file_put_contents($tempPath, $binary);
    }

    private function readChunk(array &$urlMap, string $chunk, string &$leftover): void {
        $data = $leftover.$chunk;
        $dataLen = strlen($data);
        $offset = 0;

        while(($newLine = strpos($data, "\n", $offset)) !== false) {
            $line = substr($data, $offset, $newLine - $offset);

            if ($line !== '' && $line[-1] === "\r") {
                $line = substr($line, 0, -1);
            }

            if ($line !== '') {
                $this->parseCsvRow($urlMap, $line);
            }

            $offset = $newLine + 1;
        }

        $leftover = $offset < $dataLen ? substr($data, $offset) : '';
    }

    private function parseCsvRow(array &$map, string $line): void
    {
        $urlPath = substr($line, 19, -26);
        $urlIndex = $this->urlToIndex[$urlPath];

        if (!isset($this->seen[$urlIndex])) {
            $this->seen[$urlIndex] = true;
            $this->encounterOrder[] = $urlIndex;
        }

        $datePosition = strlen($line) - 25;
        $dateIndex = ((int) substr($line, $datePosition, 4) - 2020) * 416
                   + (int) substr($line, $datePosition + 5, 2) * 32
                   + (int) substr($line, $datePosition + 8, 2);

        $map[$urlIndex * self::NUM_DATE_SLOTS + $dateIndex]++;
    }

    private function mergeOutputChunks(string $outputPath): void
    {
        $urls = Visit::all();
        [$merged, $urlOrder] = $this->mergeWorkerFiles(count($urls));
        $output = $this->buildOutput($urls, $merged, $urlOrder);
        file_put_contents($outputPath, json_encode($output, JSON_PRETTY_PRINT));
    }

    private function mergeWorkerFiles(int $numUrls): array
    {
        $flatMapSize = $numUrls * self::NUM_DATE_SLOTS;
        $merged = array_fill(0, $flatMapSize, 0);
        $urlOrder = [];
        $urlSeen = [];

        for ($i = 0; $i < $this->availableWorkers; $i++) {
            $tempPath = sprintf('%s/../tmp/p%s_worker.bin', __DIR__, $i);
            $binary = file_get_contents($tempPath);

            $n = unpack('V', $binary, 0)[1];
            $headerSize = 4 + $n * 4;

            for ($j = 0; $j < $n; $j++) {
                $urlIndex = unpack('V', $binary, 4 + $j * 4)[1];
                if (!isset($urlSeen[$urlIndex])) {
                    $urlSeen[$urlIndex] = true;
                    $urlOrder[] = $urlIndex;
                }
            }

            $values = array_values(unpack("V{$flatMapSize}", $binary, $headerSize));
            foreach ($values as $j => $value) {
                $merged[$j] += $value;
            }

            unlink($tempPath);
        }

        return [$merged, $urlOrder];
    }

    private function buildOutput(array $urls, array $merged, array $urlOrder): array
    {
        $output = [];

        foreach ($urlOrder as $index) {
            $base = $index * self::NUM_DATE_SLOTS;

            for ($di = 0; $di < self::NUM_DATE_SLOTS; $di++) {
                $count = $merged[$base + $di];
                if ($count === 0) continue;

                $urlPath = substr($urls[$index]->uri, 19);
                $dateStr = $this->slotToDateString($di);
                $output[$urlPath][$dateStr] = $count;
            }
        }

        return $output;
    }

    private function slotToDateString(int $slot): string
    {
        $year = 2020 + intdiv($slot, 416);
        $rem = $slot % 416;
        $month = intdiv($rem, 32);
        $day = $rem % 32;

        return sprintf('%04d-%02d-%02d', $year, $month, $day);
    }

    private function getCoreCount(): int
    {
        $cores = match (PHP_OS_FAMILY) {
            'Darwin' => (int) shell_exec('sysctl -n hw.physicalcpu'),
            'Linux' => (int) shell_exec('nproc'),
            default => 4,
        };

        return $cores > 0 ? $cores : 4;
    }

    private function builUrlVisitsMap(): void
    {
        $urls = Visit::all();

        foreach ($urls as $i => $visit) {
            $path = substr($visit->uri, 19);
            $this->urlToIndex[$path] = $i;
            $this->indexToUrl[$i] = $path;
        }

        $numUrls = count($urls);
        $this->flatMapSize = $numUrls * self::NUM_DATE_SLOTS;
    }
}