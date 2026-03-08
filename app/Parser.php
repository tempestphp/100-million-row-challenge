<?php

namespace App;

use Exception;


final class Parser
{
    private const WORKERS = 8;

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();
        $fileSize = filesize($inputPath);
        $chunkSize = (int) ceil($fileSize / self::WORKERS);

        $offsets = [0];
        $handle = fopen($inputPath, 'rb');

        for ($i = 1; $i < self::WORKERS; $i++) {
            fseek($handle, $i * $chunkSize);
            fgets($handle);
            $offsets[$i] = ftell($handle);
        }
        fclose($handle);

        $pids = [];
        $tempFiles = [];

        // Create temp files for each worker
        for ($i = 0; $i < self::WORKERS; $i++) {
            $tmpDir = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
            $tempFiles[$i] = $tmpDir . '/parser_worker_' . $i . '_' . uniqid() . '.dat';
        }

        // Fork workers
        for ($i = 0; $i < self::WORKERS; $i++) {
            $start = $offsets[$i];
            $end = isset($offsets[$i + 1]) ? $offsets[$i + 1] : $fileSize;

            $pid = pcntl_fork();

            if ($pid === -1) {
                throw new Exception('Failed to fork process');
            }

            if ($pid === 0) {
                // Child process
                $visits = $this->processChunk($inputPath, $start, $end);
                $data = igbinary_serialize($visits);


                
                if (file_put_contents($tempFiles[$i], $data) === false) {
                    fwrite(STDERR, "Worker $i: Failed to write temp file\n");
                    exit(1);
                }
                
                exit(0);
            }

            $pids[$i] = $pid;
        }

        // Wait for all workers
        foreach ($pids as $workerIndex => $pid) {
            pcntl_waitpid($pid, $status);
            if (pcntl_wexitstatus($status) !== 0) {
                throw new Exception("Worker $workerIndex failed");
            }
        }

        $merged = [];

        // Read and merge results from temp files
        foreach ($tempFiles as $tempFile) {
            if (!file_exists($tempFile)) {
                continue;
            }

            $data = file_get_contents($tempFile);
            $visits = igbinary_unserialize($data);


            // Merge
            foreach ($visits as $path => $days) {
                foreach ($days as $date => $count) {
                    $merged[$path][$date] = ($merged[$path][$date] ?? 0) + $count;
                }
            }

            // Clean up temp file
            unlink($tempFile);
        }

        foreach ($merged as &$days) {
            ksort($days, SORT_STRING);
        }
        unset($days);

        $json = json_encode($merged, JSON_PRETTY_PRINT);
        file_put_contents($outputPath, $json);
    }

    private function processChunk(string $inputPath, int $start, int $end): array
    {
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        
        fseek($handle, $start);
        $visits = [];
        $remaining = $end - $start;

        while ($remaining > 0) {
            $chunk = fread($handle, min(4194304, $remaining));
            $remaining -= strlen($chunk);
            
            $lines = explode("\n", $chunk);
            $lineCount = count($lines) - 1;
            
            for ($i = 0; $i < $lineCount; $i++) {
                $line = $lines[$i];
                $len = strlen($line);
                
                if ($len < 52) continue; // Min line length
                
                // Extract path: starts at position 19, ends 26 chars before newline
                $path = substr($line, 19, $len - 45);
                
                // Extract date: 10 chars starting 23 chars from end
                $date = substr($line, $len - 25, 10);
                
                $visits[$path][$date] = ($visits[$path][$date] ?? 0) + 1;
            }

            
            if ($remaining > 0 && isset($lines[$lineCount]) && $lines[$lineCount] !== '') {
                fseek($handle, -strlen($lines[$lineCount]), SEEK_CUR);
                $remaining += strlen($lines[$lineCount]);
            }
        }

        fclose($handle);
        return $visits;
    }

}