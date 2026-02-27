<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        ini_set('memory_limit', '-1');
        gc_disable();

        $cpuCount = 8;
        $fileSize = filesize($inputPath);
        $chunkSize = (int) ($fileSize / $cpuCount);

        // Increase shared memory to 1GB for better results handling
        $shmId = shmop_open(ftok(__FILE__, 'p'), "c", 0644, 1024 * 1024 * 1024);
        if (!$shmId) {
            throw new Exception("Failed to open shared memory");
        }

        $pids = [];
        for ($i = 0; $i < $cpuCount; $i++) {
            $pid = pcntl_fork();

            if ($pid === -1) {
                throw new Exception("Failed to create child process");
            }

            if ($pid === 0) {
                // Child process
                $start = $i * $chunkSize;
                $end = ($i === $cpuCount - 1) ? $fileSize : ($i + 1) * $chunkSize;
                $results = $this->processChunk($inputPath, $start, $end);
                
                if (function_exists('igbinary_serialize')) {
                    $data = igbinary_serialize($results);
                } else {
                    $data = serialize($results);
                }
                $length = strlen($data);
                
                // Each process writes into its own segment (120MB per process)
                $offset = $i * (1024 * 1024 * 120);
                shmop_write($shmId, pack('L', $length) . $data, $offset);
                
                exit(0);
            } else {
                $pids[] = $pid;
            }
        }

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $finalResults = [];
        for ($i = 0; $i < $cpuCount; $i++) {
            $offset = $i * (1024 * 1024 * 120);
            $header = shmop_read($shmId, $offset, 4);
            if (!$header) continue;
            
            $length = unpack('L', $header)[1];
            if ($length > 0) {
                $data = shmop_read($shmId, $offset + 4, $length);
                if (function_exists('igbinary_unserialize')) {
                    $partial = igbinary_unserialize($data);
                } else {
                    $partial = unserialize($data, ['allowed_classes' => false]);
                }

                // Optimized merge
                foreach ($partial as $url => $dates) {
                    if (!isset($finalResults[$url])) {
                        $finalResults[$url] = $dates;
                        continue;
                    }
                    foreach ($dates as $date => $count) {
                        $finalResults[$url][$date] = ($finalResults[$url][$date] ?? 0) + $count;
                    }
                }
            }
        }

        shmop_delete($shmId);

        // Sorting and saving
        foreach ($finalResults as &$dates) {
            ksort($dates, SORT_STRING);
        }

        file_put_contents($outputPath, json_encode($finalResults, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
    }

    private function processChunk(string $path, int $start, int $end): array
    {
        $handle = fopen($path, 'r');
        fseek($handle, $start);
        
        if ($start > 0) {
            fgets($handle); // Skip to the beginning of a full line
        }

        $currentPos = ftell($handle);
        $results = [];
        $pathCache = [];
        $dateCache = [];
        $prefixLen = 19; // https://stitcher.io

        $bufferSize = 4 * 1024 * 1024; // 4MB buffer
        $remaining = '';

        while ($currentPos < $end) {
            $chunk = fread($handle, $bufferSize);
            if ($chunk === false || $chunk === '') break;

            $currentPos += strlen($chunk);
            $data = $remaining . $chunk;
            $pos = 0;

            while (($commaPos = strpos($data, ',', $pos)) !== false) {
                $eolPos = strpos($data, "\n", $commaPos);
                if ($eolPos === false) break;

                // Extract path: remove prefix
                $pathStr = substr($data, $pos + $prefixLen, $commaPos - $pos - $prefixLen);
                $pathStr = $pathCache[$pathStr] ??= $pathStr;

                // Date is 10 chars after comma (YYYY-MM-DD)
                $date = substr($data, $commaPos + 1, 10);
                $date = $dateCache[$date] ??= $date;
                
                if (isset($results[$pathStr][$date])) {
                    $results[$pathStr][$date]++;
                } else {
                    $results[$pathStr][$date] = 1;
                }

                $pos = $eolPos + 1;
            }
            $remaining = substr($data, $pos);
        }
        
        // Handle last remaining line that started before $end
        if ($remaining !== '') {
            if (strpos($remaining, "\n") === false) {
                $lineFromNext = fgets($handle);
                if ($lineFromNext !== false) {
                    $remaining .= $lineFromNext;
                }
            }
            
            $commaPos = strpos($remaining, ',');
            if ($commaPos !== false) {
                $pathStr = substr($remaining, $prefixLen, $commaPos - $prefixLen);
                $pathStr = $pathCache[$pathStr] ??= $pathStr;
                $date = substr($remaining, $commaPos + 1, 10);
                $date = $dateCache[$date] ??= $date;

                if (isset($results[$pathStr][$date])) {
                    $results[$pathStr][$date]++;
                } else {
                    $results[$pathStr][$date] = 1;
                }
            }
        }

        fclose($handle);
        return $results;
    }
}
