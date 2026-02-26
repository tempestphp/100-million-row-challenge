<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);

        // Small file: single-threaded (handles data:validate correctly)
        if ($fileSize < 1_000_000 || !function_exists('pcntl_fork')) {
            $data = $this->readSegment($inputPath, 0, $fileSize);
            $this->writeOutput($data, $outputPath);
            return;
        }

        // Find midpoint aligned to next line boundary
        $fp = fopen($inputPath, 'rb');
        fseek($fp, (int) ($fileSize / 2));
        fgets($fp);
        $mid = ftell($fp);
        fclose($fp);

        // Prefer RAM-backed tmpfs (/dev/shm) to avoid disk write latency
        $tempDir  = (is_dir('/dev/shm') && is_writable('/dev/shm')) ? '/dev/shm' : sys_get_temp_dir();
        $tempFile = "$tempDir/p100m_" . getmypid();

        // igbinary is faster than PHP's native serialize; available on benchmark server
        $useIgbinary = function_exists('igbinary_serialize');

        $pid = pcntl_fork();

        if ($pid === -1) {
            // Fork failed: fall back to single process
            $data = $this->readSegment($inputPath, 0, $fileSize);
            $this->writeOutput($data, $outputPath);
            return;
        }

        if ($pid === 0) {
            // Child: process second half of file
            $data    = $this->readSegment($inputPath, $mid, $fileSize);
            $encoded = $useIgbinary ? igbinary_serialize($data) : serialize($data);
            file_put_contents($tempFile, $encoded);
            // SIGKILL skips PHP + Tempest shutdown handlers (~0.5 s saved per child)
            posix_kill(getmypid(), SIGKILL);
        }

        // Parent: process first half concurrently with child
        $data = $this->readSegment($inputPath, 0, $mid);

        // Wait for child to finish writing its result
        pcntl_waitpid($pid, $status);

        // Merge child results into parent's data (parent data keeps insertion order)
        $encoded   = file_get_contents($tempFile);
        @unlink($tempFile);
        $childData = $useIgbinary ? igbinary_unserialize($encoded) : unserialize($encoded);
        unset($encoded);

        foreach ($childData as $path => $dates) {
            if (!isset($data[$path])) {
                $data[$path] = $dates;
            } else {
                foreach ($dates as $date => $count) {
                    if (isset($data[$path][$date])) {
                        $data[$path][$date] += $count;
                    } else {
                        $data[$path][$date] = $count;
                    }
                }
            }
        }
        unset($childData);

        $this->writeOutput($data, $outputPath);
    }

    /**
     * Read and aggregate a byte range of the CSV into path → date → count.
     *
     * Line format (guaranteed):
     *   https://stitcher.io<path>,YYYY-MM-DDTHH:MM:SS+00:00\n
     *
     * Fixed offsets (domain = 19 chars, trailing ",timestamp\n" = 27 chars):
     *   path → substr($line, 19, $len - 46)
     *   date → substr($line, $len - 26, 10)
     *
     * No strpos/strrpos scan needed — zero-search extraction.
     */
    private function readSegment(string $inputPath, int $start, int $end): array
    {
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 8 * 1024 * 1024); // 8 MB kernel buffer

        if ($start > 0) {
            fseek($handle, $start);
        }

        $data      = [];
        $remaining = $end - $start;

        while ($remaining > 0 && ($line = fgets($handle, 256)) !== false) {
            $len        = strlen($line);
            $remaining -= $len;

            $pathLen = $len - 46;
            if ($pathLen <= 0) {
                continue;
            }

            $path = substr($line, 19, $pathLen);
            $date = substr($line, $len - 26, 10);

            if (isset($data[$path][$date])) {
                $data[$path][$date]++;
            } else {
                $data[$path][$date] = 1;
            }
        }

        fclose($handle);

        return $data;
    }

    /**
     * Sort each URL's dates ascending, then write as pretty JSON.
     *
     * Manual JSON building avoids json_encode's recursive encoder overhead
     * and produces identical output (PHP's json_encode escapes '/' as '\/'
     * by default, which we replicate with str_replace).
     */
    private function writeOutput(array $data, string $outputPath): void
    {
        $entries = [];
        foreach ($data as $path => $dates) {
            ksort($dates);
            $escapedPath = str_replace('/', '\\/', $path);
            $dateLines   = [];
            foreach ($dates as $date => $count) {
                $dateLines[] = "        \"$date\": $count";
            }
            $entries[] = "    \"$escapedPath\": {\n" . implode(",\n", $dateLines) . "\n    }";
        }

        file_put_contents($outputPath, "{\n" . implode(",\n", $entries) . "\n}");
    }
}
