<?php

namespace App;

final class Parser
{
    private const BUFFER_SIZE = 32 * 1024 * 1024; // 32MB

    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);
        $splitPoint = $this->findSplitPoint($inputPath, intdiv($fileSize, 2));

        $tmp1 = tempnam(sys_get_temp_dir(), 'p1_');
        $tmp2 = tempnam(sys_get_temp_dir(), 'p2_');

        // Fork worker 1: first half [0, splitPoint)
        $pid1 = pcntl_fork();
        if ($pid1 === 0) {
            $result = $this->processSegment($inputPath, 0, $splitPoint);
            file_put_contents($tmp1, serialize($result));
            exit(0);
        }

        // Fork worker 2: second half [splitPoint, fileSize)
        $pid2 = pcntl_fork();
        if ($pid2 === 0) {
            $result = $this->processSegment($inputPath, $splitPoint, $fileSize);
            file_put_contents($tmp2, serialize($result));
            exit(0);
        }

        // Parent: wait for both workers
        pcntl_waitpid($pid1, $status1);
        pcntl_waitpid($pid2, $status2);

        $data1 = unserialize(file_get_contents($tmp1));
        $data2 = unserialize(file_get_contents($tmp2));
        unlink($tmp1);
        unlink($tmp2);

        // Merge: data1 has correct first-occurrence order from the first half.
        // Any page first seen in the second half gets appended.
        $merged = $data1;
        foreach ($data2 as $path => $dates) {
            if (isset($merged[$path])) {
                foreach ($dates as $date => $count) {
                    $merged[$path][$date] = ($merged[$path][$date] ?? 0) + $count;
                }
            } else {
                $merged[$path] = $dates;
            }
        }

        // Sort each page's dates ascending
        foreach ($merged as &$dates) {
            ksort($dates);
        }
        unset($dates);

        file_put_contents($outputPath, json_encode($merged, JSON_PRETTY_PRINT));
    }

    /**
     * Seek to $offset, scan forward to the next newline, return position after it.
     * This ensures workers always start/end on clean line boundaries.
     */
    private function findSplitPoint(string $path, int $offset): int
    {
        $handle = fopen($path, 'rb');
        fseek($handle, $offset);
        while (!feof($handle)) {
            if (fread($handle, 1) === "\n") {
                break;
            }
        }
        $pos = ftell($handle);
        fclose($handle);
        return $pos;
    }

    /**
     * Stream bytes [$start, $limit) of the file in large chunks,
     * accumulating visit counts keyed by [path][date].
     */
    private function processSegment(string $filePath, int $start, int $limit): array
    {
        $handle = fopen($filePath, 'rb');
        if ($start > 0) {
            fseek($handle, $start);
        }

        $data = [];
        $remainder = '';
        $bytesRead = 0;
        $totalBytes = $limit - $start;

        while ($bytesRead < $totalBytes && !feof($handle)) {
            $toRead = min(self::BUFFER_SIZE, $totalBytes - $bytesRead);
            $raw = fread($handle, $toRead);
            $bytesRead += strlen($raw);

            $chunk = $remainder . $raw;
            $lastNl = strrpos($chunk, "\n");

            if ($lastNl === false) {
                $remainder = $chunk;
                continue;
            }

            $remainder = substr($chunk, $lastNl + 1);

            // Walk every line in [0, $lastNl].
            //
            // The timestamp format (date('c')) is always exactly 25 chars:
            //   YYYY-MM-DDThh:mm:ss+HH:MM
            // So the comma is always at $nl - 26, and the date starts at $nl - 25.
            // This avoids a strpos() scan for the comma on every line.
            $pos = 0;
            while ($pos <= $lastNl) {
                $nl = strpos($chunk, "\n", $pos);
                if ($nl === false) {
                    break;
                }

                // "https://stitcher.io" = 19 chars, path starts at offset 19.
                // path length = $nl - $pos - 19(prefix) - 1(comma) - 25(timestamp) = $nl - $pos - 45
                $path = substr($chunk, $pos + 19, $nl - $pos - 45);
                $date = substr($chunk, $nl - 25, 10);

                if (isset($data[$path][$date])) {
                    $data[$path][$date]++;
                } else {
                    $data[$path][$date] = 1;
                }

                $pos = $nl + 1;
            }
        }

        // Handle any trailing line without a terminating newline
        if ($remainder !== '') {
            $len = strlen($remainder);
            if ($len > 45) {
                $path = substr($remainder, 19, $len - 45);
                $date = substr($remainder, $len - 25, 10);
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
}
