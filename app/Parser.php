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
        $readSize = 4 * 1024 * 1024; // 4MB chunks
        $leftover = '';

        while (true) {
            if ($bytesRead >= $endByte && $leftover === '') {
                break;
            }

            $chunk = '';
            if ($bytesRead < $endByte) {
                $chunk = fread($handle, $readSize);
                if ($chunk === false || $chunk === '') {
                    break;
                }
                $bytesRead += strlen($chunk);
            }

            // Avoid string copy when no leftover (common case)
            if ($leftover !== '') {
                $buffer = $leftover . $chunk;
                $leftover = '';
            } else {
                $buffer = $chunk;
            }

            $bufLen = strlen($buffer);
            $lastNl = strrpos($buffer, "\n");

            if ($lastNl === false) {
                if (feof($handle) || $bytesRead >= $endByte) {
                    break;
                }
                $leftover = $buffer;
                continue;
            }

            // Parse complete lines directly in buffer.
            // Key insight: ISO 8601 datetime is always 25 chars (YYYY-MM-DDThh:mm:ss+HH:MM)
            // so the comma separator is always at nlPos - 26.
            // This eliminates one strpos call per line (100M calls saved).
            $offset = 0;
            while ($offset < $lastNl) {
                $nlPos = strpos($buffer, "\n", $offset);
                if ($nlPos === false || $nlPos > $lastNl) {
                    break;
                }

                // Comma is exactly 26 chars before the newline
                $commaPos = $nlPos - 26;

                $path = substr($buffer, $offset + 19, $commaPos - $offset - 19);
                $date = substr($buffer, $commaPos + 1, 10);

                if (isset($data[$path][$date])) {
                    $data[$path][$date]++;
                } else {
                    $data[$path][$date] = 1;
                }

                $offset = $nlPos + 1;
            }

            $leftover = ($lastNl + 1 < $bufLen) ? substr($buffer, $lastNl + 1) : '';
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
