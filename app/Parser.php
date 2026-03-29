<?php

namespace App;

final class Parser
{
    private const NUM_WORKERS = 4;
    private const BUF_SIZE = 1 << 20; // 1MB

    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);

        if ($fileSize > 10_000_000 && function_exists('pcntl_fork')) {
            $this->parseParallel($inputPath, $outputPath, $fileSize);
            return;
        }

        $data = $this->processRange($inputPath, 0, $fileSize);
        $this->writeJson($data, $outputPath);
    }

    private function parseParallel(string $inputPath, string $outputPath, int $fileSize): void
    {
        $numWorkers = self::NUM_WORKERS;

        // Find split points aligned to newline boundaries
        $fp = fopen($inputPath, 'rb');
        $splits = [0];
        for ($i = 1; $i < $numWorkers; $i++) {
            fseek($fp, (int)($fileSize * $i / $numWorkers));
            fgets($fp);
            $splits[] = ftell($fp);
        }
        $splits[] = $fileSize;
        fclose($fp);

        // Fork child workers for all chunks except the last
        $tempFiles = [];
        $pids = [];
        for ($w = 0; $w < $numWorkers - 1; $w++) {
            $tf = tempnam(sys_get_temp_dir(), 'p_');
            $tempFiles[$w] = $tf;
            $pid = pcntl_fork();

            if ($pid === -1) {
                // Fork failed — fall back to single-threaded
                // Clean up any already-forked children
                foreach ($pids as $cpid) {
                    pcntl_waitpid($cpid, $s);
                }
                foreach ($tempFiles as $f) {
                    @unlink($f);
                }
                $data = $this->processRange($inputPath, 0, $fileSize);
                $this->writeJson($data, $outputPath);
                return;
            }

            if ($pid === 0) {
                $data = $this->processRange($inputPath, $splits[$w], $splits[$w + 1]);
                file_put_contents($tf, serialize($data));
                exit(0);
            }
            $pids[$w] = $pid;
        }

        // Parent processes the last chunk
        $data = $this->processRange($inputPath, $splits[$numWorkers - 1], $splits[$numWorkers]);

        // Collect children in reverse order and merge (preserves insertion order from chunk 0)
        for ($w = $numWorkers - 2; $w >= 0; $w--) {
            pcntl_waitpid($pids[$w], $status);
            $childData = unserialize(file_get_contents($tempFiles[$w]));
            unlink($tempFiles[$w]);

            // Merge parent's data into child's (child = earlier chunk = base for ordering)
            foreach ($data as $path => $dates) {
                if (isset($childData[$path])) {
                    foreach ($dates as $date => $count) {
                        $childData[$path][$date] = ($childData[$path][$date] ?? 0) + $count;
                    }
                } else {
                    $childData[$path] = $dates;
                }
            }
            $data = $childData;
            unset($childData);
        }

        $this->writeJson($data, $outputPath);
    }

    private function processRange(string $inputPath, int $start, int $end): array
    {
        $data = [];
        $fp = fopen($inputPath, 'rb');

        if ($start > 0) {
            fseek($fp, $start);
        }

        $remaining = $end - $start;
        $bufSize = self::BUF_SIZE;
        $leftover = '';

        while ($remaining > 0) {
            $toRead = min($bufSize, $remaining);
            $raw = fread($fp, $toRead);
            if ($raw === '' || $raw === false) break;
            $remaining -= strlen($raw);

            // Handle leftover: complete the partial line without copying entire buffer
            if ($leftover !== '') {
                $firstNl = strpos($raw, "\n");
                if ($firstNl === false) {
                    $leftover .= $raw;
                    continue;
                }
                $line = $leftover . substr($raw, 0, $firstNl);
                $leftover = '';
                $cp = strpos($line, ',', 19);
                if ($cp !== false) {
                    $p = substr($line, 19, $cp - 19);
                    $d = substr($line, $cp + 1, 10);
                    $data[$p][$d] = ($data[$p][$d] ?? 0) + 1;
                }
                $startOffset = $firstNl + 1;
            } else {
                $startOffset = 0;
            }

            $rawLen = strlen($raw);
            $lastNl = strrpos($raw, "\n");
            if ($lastNl === false || $lastNl < $startOffset) {
                $leftover = substr($raw, $startOffset);
                continue;
            }

            if ($lastNl + 1 < $rawLen) {
                $leftover = substr($raw, $lastNl + 1);
            }

            // Process complete lines directly in the raw buffer
            $offset = $startOffset;
            while ($offset < $lastNl) {
                // Find comma after "https://stitcher.io" (19 chars)
                $cp = strpos($raw, ',', $offset + 19);
                if ($cp === false) break;

                $p = substr($raw, $offset + 19, $cp - $offset - 19);
                $d = substr($raw, $cp + 1, 10);
                $data[$p][$d] = ($data[$p][$d] ?? 0) + 1;

                // Skip: comma(1) + 25-char ISO date + newline(1) = 27
                $offset = $cp + 27;
            }
        }

        fclose($fp);

        // Handle final leftover
        if ($leftover !== '') {
            $cp = strpos($leftover, ',', 19);
            if ($cp !== false) {
                $p = substr($leftover, 19, $cp - 19);
                $d = substr($leftover, $cp + 1, 10);
                $data[$p][$d] = ($data[$p][$d] ?? 0) + 1;
            }
        }

        return $data;
    }

    private function writeJson(array &$data, string $outputPath): void
    {
        // Sort dates ascending for each path
        foreach ($data as &$dates) {
            ksort($dates);
        }
        unset($dates);

        file_put_contents($outputPath, json_encode($data, JSON_PRETTY_PRINT));
    }
}
