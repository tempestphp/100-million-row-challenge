<?php

namespace App;

/**
 * Parses a CSV of page visits and outputs a JSON file of visit counts per path per date.
 *
 * Input format (one visit per line):
 *   https://stitcher.io/blog/some-path,2023-11-03T03:32:09+00:00
 *   |---- 19 chars -----|-- path ---|,|-- date --|--- 15 chars ---|
 *
 * Output format:
 *   { "/blog/some-path": { "2023-11-03": 42, ... }, ... }
 *
 * Strategy:
 *   - Fork 2 workers (matches the 2 vCPU constraint), each processing half the file
 *   - Workers read in 256MB chunks, extract "path,date" keys via strpos+substr,
 *     then count duplicates with array_count_values (C-level, much faster than PHP loops)
 *   - Workers serialize results to /dev/shm (RAM-backed) using igbinary (2-5x faster than serialize)
 *   - Parent merges worker results and writes sorted JSON output
 */
final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        // Use igbinary for IPC serialization if available (faster), fall back to native serialize
        $serialize = function_exists('igbinary_serialize') ? 'igbinary_serialize' : 'serialize';
        $unserialize = function_exists('igbinary_unserialize') ? 'igbinary_unserialize' : 'unserialize';

        $workerCount = 2;
        $fileSize = filesize($inputPath);

        // Split the file into equal-sized segments, aligned to line boundaries.
        // Seek to approximate midpoint, skip to the next newline, record that byte offset.
        $boundaries = [0];
        $handle = fopen($inputPath, 'r');
        for ($i = 1; $i < $workerCount; $i++) {
            fseek($handle, (int)($fileSize * $i / $workerCount));
            fgets($handle); // advance past partial line
            $boundaries[] = ftell($handle);
        }
        $boundaries[] = $fileSize;
        fclose($handle);

        // /dev/shm is a RAM-backed filesystem — avoids disk I/O for worker result files
        $tmpDir = '/dev/shm';
        $tmpFiles = [];
        $pids = [];

        // Fork worker processes, each responsible for one file segment
        for ($w = 0; $w < $workerCount; $w++) {
            $tmpFile = $tmpDir . '/parse_' . $w . '_' . getmypid();
            $tmpFiles[$w] = $tmpFile;

            $pid = pcntl_fork();
            if ($pid === 0) {
                // Child: process our segment and write serialized results to shared memory
                $data = $this->processChunk($inputPath, $boundaries[$w], $boundaries[$w + 1]);
                file_put_contents($tmpFile, $serialize($data));
                exit(0);
            }
            $pids[$w] = $pid;
        }

        // Wait for all workers to finish
        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        // Merge results: use first worker's data as base, add subsequent workers' counts
        $merged = $unserialize(file_get_contents($tmpFiles[0]));
        unlink($tmpFiles[0]);

        for ($w = 1; $w < $workerCount; $w++) {
            $data = $unserialize(file_get_contents($tmpFiles[$w]));
            unlink($tmpFiles[$w]);

            foreach ($data as $path => $dates) {
                if (!isset($merged[$path])) {
                    $merged[$path] = $dates;
                } else {
                    foreach ($dates as $date => $count) {
                        $merged[$path][$date] = ($merged[$path][$date] ?? 0) + $count;
                    }
                }
            }
        }

        // Sort dates within each path for consistent output
        foreach ($merged as &$dates) {
            ksort($dates);
        }

        file_put_contents($outputPath, json_encode($merged, JSON_PRETTY_PRINT));
    }

    /**
     * Process a byte range of the CSV file and return nested path => date => count.
     *
     * The inner loop extracts a flat "path,date" key from each line using fixed offsets:
     *   - Skip first 19 chars ("https://stitcher.io")
     *   - Skip last 15 chars of each line ("THH:MM:SS+00:00")
     *   - Result: "/blog/some-path,2023-11-03"
     *
     * Instead of incrementing a counter per line (5M PHP hash ops per worker),
     * we collect all keys into a flat array and call array_count_values() once.
     * This moves the counting into C, reducing PHP iterations from ~5M to ~102K
     * unique (path,date) pairs per chunk.
     */
    private function processChunk(string $inputPath, int $startOffset, int $endOffset): array
    {
        $data = [];
        $handle = fopen($inputPath, 'r');
        fseek($handle, $startOffset);

        $bytesRemaining = $endOffset - $startOffset;
        $chunkSize = 268435456; // 256MB — sweet spot balancing fewer fread calls vs memory pressure
        $remainder = '';

        while ($bytesRemaining > 0) {
            $toRead = min($chunkSize, $bytesRemaining);
            $chunk = fread($handle, $toRead);
            if ($chunk === false || $chunk === '') break;
            $bytesRemaining -= strlen($chunk);

            // Prepend any leftover bytes from the previous chunk (partial last line)
            $chunk = $remainder . $chunk;
            $chunkLen = strlen($chunk);
            $lastNewline = strrpos($chunk, "\n");

            if ($lastNewline === false) {
                // No complete line in this chunk — carry everything forward
                $remainder = $chunk;
                continue;
            }

            // Save any bytes after the last newline for the next iteration
            $remainder = ($lastNewline + 1 < $chunkLen) ? substr($chunk, $lastNewline + 1) : '';

            // Extract flat "path,date" keys from each line.
            // Line layout: https://stitcher.io{path},{date}T{time}+00:00\n
            //              |------ 19 ------|        |10| |--- 15 ---|
            // So key = substr(offset+19, lineEnd-offset-34) gives "{path},{date}"
            $keys = [];
            $offset = 0;
            while ($offset < $lastNewline) {
                $lineEnd = strpos($chunk, "\n", $offset);
                $keys[] = substr($chunk, $offset + 19, $lineEnd - $offset - 34);
                $offset = $lineEnd + 1;
            }

            // Free the chunk before counting — reduces peak memory by ~256MB
            unset($chunk);

            // C-level counting: collapses ~3.2M lines into ~102K unique (path,date) counts
            foreach (array_count_values($keys) as $key => $count) {
                $data[$key] = ($data[$key] ?? 0) + $count;
            }
            unset($keys);
        }

        // Handle the final partial line (if the file doesn't end with a newline)
        if ($remainder !== '') {
            $key = substr($remainder, 19, strlen($remainder) - 34);
            $data[$key] = ($data[$key] ?? 0) + 1;
        }

        fclose($handle);

        // Convert flat "path,date" => count into nested path => date => count
        // for efficient merging and igbinary serialization (date strings get deduplicated)
        $result = [];
        foreach ($data as $key => $count) {
            $sep = strrpos($key, ',');
            $result[substr($key, 0, $sep)][substr($key, $sep + 1)] = $count;
        }
        return $result;
    }
}
