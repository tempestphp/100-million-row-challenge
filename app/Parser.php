<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);

        // Find a line-aligned split point near the middle
        $handle = fopen($inputPath, 'rb');
        fseek($handle, (int)($fileSize / 2));
        fgets($handle);
        $splitPoint = ftell($handle);
        fclose($handle);

        $tmpFile = $inputPath . '.tmp';

        $pid = pcntl_fork();

        if ($pid === 0) {
            $data = $this->processChunk($inputPath, 0, $splitPoint);
            file_put_contents($tmpFile, serialize($data));
            exit(0);
        }

        // Parent: second half
        $parentData = $this->processChunk($inputPath, $splitPoint, $fileSize);

        pcntl_waitpid($pid, $status);

        $data = unserialize(file_get_contents($tmpFile));
        unlink($tmpFile);

        // Merge parent's data into child's data (preserves key insertion order)
        foreach ($parentData as $path => $dates) {
            if (isset($data[$path])) {
                foreach ($dates as $date => $count) {
                    if (isset($data[$path][$date])) {
                        $data[$path][$date] += $count;
                    } else {
                        $data[$path][$date] = $count;
                    }
                }
            } else {
                $data[$path] = $dates;
            }
        }

        // Sort dates ascending
        foreach ($data as &$dates) {
            ksort($dates);
        }
        unset($dates);

        file_put_contents($outputPath, json_encode($data, JSON_PRETTY_PRINT));
    }

    private function processChunk(string $inputPath, int $start, int $end): array
    {
        $counts = [];
        $handle = fopen($inputPath, 'rb');

        if ($start > 0) {
            fseek($handle, $start);
        }

        $remaining = $end - $start;
        $readSize = 2 * 1024 * 1024;
        $leftover = '';

        while ($remaining > 0) {
            $toRead = min($readSize, $remaining);
            $chunk = fread($handle, $toRead);
            if ($chunk === false || $chunk === '') {
                break;
            }
            $remaining -= strlen($chunk);

            if ($leftover !== '') {
                $chunk = $leftover . $chunk;
                $leftover = '';
            }

            $chunkLen = strlen($chunk);
            $lastNewline = strrpos($chunk, "\n");

            if ($lastNewline === false) {
                $leftover = $chunk;
                continue;
            }

            if ($lastNewline < $chunkLen - 1) {
                $leftover = substr($chunk, $lastNewline + 1);
            }

            $offset = 0;

            while ($offset <= $lastNewline) {
                $newlinePos = strpos($chunk, "\n", $offset);

                // Extract combined key: /path,YYYY-MM-DD
                // Skip "https://stitcher.io" (19 chars) from start
                // Skip "THH:MM:SS+00:00\n" (16 chars) from end
                $key = substr($chunk, $offset + 19, $newlinePos - $offset - 34);

                if (isset($counts[$key])) {
                    $counts[$key]++;
                } else {
                    $counts[$key] = 1;
                }

                $offset = $newlinePos + 1;
            }
        }

        if ($leftover !== '' && strlen($leftover) > 45) {
            $key = substr($leftover, 19, strlen($leftover) - 34);
            if (isset($counts[$key])) {
                $counts[$key]++;
            } else {
                $counts[$key] = 1;
            }
        }

        fclose($handle);

        // Convert flat counts to nested structure (for efficient serialization + correct key order)
        $data = [];
        foreach ($counts as $key => $count) {
            $path = substr($key, 0, -11);
            $date = substr($key, -10);
            $data[$path][$date] = $count;
        }

        return $data;
    }
}
