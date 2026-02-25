<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        ini_set('memory_limit', '-1');
        $fileSize = filesize($inputPath);
        $numWorkers = 4;

        // Small files or no fork support: single process
        if ($fileSize < 10_000_000 || !function_exists('pcntl_fork')) {
            $data = $this->processRange($inputPath, 0, $fileSize);
            $this->writeOutput($data, $outputPath);
            return;
        }

        $chunkSize = intdiv($fileSize, $numWorkers);
        $tempFiles = [];
        $pids = [];

        for ($i = 0; $i < $numWorkers; $i++) {
            $start = $i * $chunkSize;
            $end = ($i === $numWorkers - 1) ? $fileSize : ($i + 1) * $chunkSize;
            $tempFile = sys_get_temp_dir() . '/parser_' . getmypid() . '_' . $i;
            $tempFiles[] = $tempFile;

            $pid = pcntl_fork();

            if ($pid === 0) {
                $data = $this->processRange($inputPath, $start, $end);
                file_put_contents($tempFile, serialize($data));
                exit(0);
            }

            $pids[] = $pid;
        }

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        // Load first worker's result as base (preserves path insertion order)
        $merged = unserialize(file_get_contents($tempFiles[0]));
        unlink($tempFiles[0]);

        // Merge remaining workers
        for ($i = 1; $i < $numWorkers; $i++) {
            $partial = unserialize(file_get_contents($tempFiles[$i]));
            unlink($tempFiles[$i]);

            foreach ($partial as $path => $dates) {
                if (!isset($merged[$path])) {
                    $merged[$path] = $dates;
                } else {
                    foreach ($dates as $date => $count) {
                        $merged[$path][$date] = ($merged[$path][$date] ?? 0) + $count;
                    }
                }
            }
        }

        $this->writeOutput($merged, $outputPath);
    }

    private function processRange(string $inputPath, int $startByte, int $endByte): array
    {
        $data = [];
        $handle = fopen($inputPath, 'r');

        if ($startByte > 0) {
            fseek($handle, $startByte);
            fgets($handle); // skip partial line
        }

        $bytesRead = ftell($handle);
        $readSize = 8 * 1024 * 1024; // 8MB chunks
        $leftover = '';

        while ($bytesRead < $endByte) {
            $chunk = fread($handle, $readSize);
            if ($chunk === false || $chunk === '') {
                break;
            }
            $bytesRead += strlen($chunk);

            // Split chunk into lines via single C-level explode call
            // This replaces per-line strpos("\n") with one bulk operation
            $lines = explode("\n", $chunk);

            // Prepend leftover from previous chunk to first line
            if ($leftover !== '') {
                $lines[0] = $leftover . $lines[0];
            }

            // Last element is partial line (or empty if chunk ended with \n)
            $leftover = array_pop($lines);

            // Process complete lines
            // strlen is a PHP opcode (not a function call) — nearly free
            // This avoids strpos entirely in the hot loop
            foreach ($lines as $line) {
                // Comma is always 26 chars before end (25 datetime + 1 comma)
                $commaPos = strlen($line) - 26;

                $path = substr($line, 19, $commaPos - 19);
                $date = substr($line, $commaPos + 1, 10);

                if (isset($data[$path][$date])) {
                    $data[$path][$date]++;
                } else {
                    $data[$path][$date] = 1;
                }
            }
        }

        fclose($handle);
        return $data;
    }

    private function writeOutput(array &$data, string $outputPath): void
    {
        foreach ($data as &$dates) {
            ksort($dates);
        }
        unset($dates);

        // Manual JSON to avoid json_encode overhead on large structure
        $fp = fopen($outputPath, 'w');
        $buf = "{\n";
        $firstPath = true;

        foreach ($data as $path => $dates) {
            if (!$firstPath) {
                $buf .= ",\n";
            }
            $firstPath = false;
            $buf .= '    "' . str_replace('/', '\\/', $path) . '": {' . "\n";

            $firstDate = true;
            foreach ($dates as $date => $count) {
                if (!$firstDate) {
                    $buf .= ",\n";
                }
                $firstDate = false;
                $buf .= "        \"{$date}\": {$count}";
            }
            $buf .= "\n    }";
        }

        $buf .= "\n}";
        fwrite($fp, $buf);
        fclose($fp);
    }
}
