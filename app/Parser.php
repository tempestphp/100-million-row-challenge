<?php

declare(strict_types=1);

namespace App;

use function array_fill;
use function array_keys;
use function asort;
use function fclose;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function implode;
use function pack;
use function str_replace;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function function_exists;
use function pcntl_fork;
use function pcntl_waitpid;
use function getmypid;
use function sys_get_temp_dir;
use function file_get_contents;
use function file_put_contents;
use function unpack;
use function unlink;
use function sprintf;
use function filesize;

final class Parser
{
    private const int READ_CHUNK_SIZE = 16 * 1024 * 1024; // 16MB
    private const int WRITE_BUFFER_SIZE = 1 * 1024 * 1024; // 1MB
    private const int URI_PREFIX_LENGTH = 19; // Skips "https://stitcher.io" to get "/blog/..."
    private const int DATE_TAIL_LENGTH = 25; // "2026-01-24T01:16:58+00:00"
    private const int WORKERS = 4;
    private const int DATE_STRIDE = 2640; // Enough for 2020-2026 with formula index

    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);

        // 1. Preparation: Boundaries
        // We use a math-based date index to avoid string lookups
        $dates = $this->generateDates();

        // 2. Discovery: Find all paths
        [$pathIds, $pathNames, $pathCount] = $this->discoverPaths($inputPath, min($fileSize, 32 * 1024 * 1024));

        // 3. Execution
        $mergedCounts = $this->executeParallel(
            $inputPath,
            $fileSize,
            $pathIds,
            $pathCount
        );

        // 4. Output
        $this->writeOutput($outputPath, $pathNames, $dates, $mergedCounts);
    }

    private function generateDates(): array
    {
        $dates = [];
        for ($y = 2020; $y <= 2026; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $daysInMonth = match ($m) {
                    2 => ($y % 4 === 0 && ($y % 100 !== 0 || $y % 400 === 0)) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                for ($d = 1; $d <= $daysInMonth; $d++) {
                    $idx = ($y - 2020) * 372 + $m * 31 + $d;
                    $dates[$idx] = sprintf('%04d-%02d-%02d', $y, $m, $d);
                }
            }
        }
        return $dates;
    }

    private function discoverPaths(string $inputPath, int $size): array
    {
        $handle = fopen($inputPath, 'rb');
        $chunk = fread($handle, $size);
        fclose($handle);

        $lastNl = strrpos($chunk, "\n");
        if ($lastNl === false)
            return [[], [], 0];

        $pathIds = [];
        $pathNames = [];
        $pathCount = 0;
        $pos = 0;

        while ($pos < $lastNl) {
            $commaPos = strpos($chunk, ',', $pos);
            if ($commaPos === false || $commaPos > $lastNl)
                break;

            $path = substr($chunk, $pos + self::URI_PREFIX_LENGTH, $commaPos - ($pos + self::URI_PREFIX_LENGTH));

            if (!isset($pathIds[$path])) {
                $pathIds[$path] = $pathCount;
                $pathNames[$pathCount] = $path;
                $pathCount++;
            }

            $pos = strpos($chunk, "\n", $commaPos) + 1;
        }

        return [$pathIds, $pathNames, $pathCount];
    }

    private function executeParallel(
        string $inputPath,
        int $fileSize,
        array $pathIds,
        int $pathCount
    ): array {
        $numWorkers = function_exists('pcntl_fork') ? self::WORKERS : 1;

        if ($numWorkers === 1) {
            return $this->parseRange($inputPath, 0, $fileSize, $pathIds, $pathCount);
        }

        // Multi-processing logic
        $chunkSize = (int) ($fileSize / $numWorkers);
        $boundaries = [0];
        $handle = fopen($inputPath, 'rb');
        for ($i = 1; $i < $numWorkers; $i++) {
            fseek($handle, $i * $chunkSize);
            fgets($handle); // reach end of line
            $boundaries[] = ftell($handle);
        }
        $boundaries[] = $fileSize;
        fclose($handle);

        $tmpDir = sys_get_temp_dir();
        $myPid = getmypid();
        $pids = [];
        $tmpFiles = [];

        for ($i = 0; $i < $numWorkers; $i++) {
            if ($i < $numWorkers - 1) {
                $tmpFile = $tmpDir . "/php_parser_{$myPid}_{$i}.bin";
                $tmpFiles[] = $tmpFile;

                $pid = pcntl_fork();
                if ($pid === 0) {
                    $counts = $this->parseRange($inputPath, $boundaries[$i], $boundaries[$i + 1], $pathIds, $pathCount);
                    file_put_contents($tmpFile, pack('V*', ...$counts));
                    exit(0);
                }
                $pids[] = $pid;
            } else {
                // Last partial is handled by main process
                $mergedCounts = $this->parseRange($inputPath, $boundaries[$i], $boundaries[$i + 1], $pathIds, $pathCount);
            }
        }

        // Wait for workers
        $status = 0;
        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        // Merge results
        foreach ($tmpFiles as $tmpFile) {
            $data = (string) file_get_contents($tmpFile);
            $workerCounts = unpack('V*', $data);
            unlink($tmpFile);

            $j = 0;
            foreach ($workerCounts as $val) {
                $mergedCounts[$j++] += $val;
            }
        }

        return $mergedCounts;
    }

    private function parseRange(
        string $inputPath,
        int $start,
        int $end,
        array $pathIds,
        int $pathCount
    ): array {
        $stride = self::DATE_STRIDE;
        $counts = array_fill(0, $pathCount * $stride, 0);

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);

        $remaining = $end - $start;

        while ($remaining > 0) {
            $bufferSize = min($remaining, self::READ_CHUNK_SIZE);
            $chunk = fread($handle, $bufferSize);
            if ($chunk === '' || $chunk === false)
                break;

            $chunkLen = strlen($chunk);
            $lastNl = strrpos($chunk, "\n");

            if ($lastNl === false) {
                fseek($handle, $start + ($end - $start - $remaining) + $chunkLen);
                break;
            }

            if ($lastNl < $chunkLen - 1) {
                $rewind = $chunkLen - $lastNl - 1;
                fseek($handle, -$rewind, SEEK_CUR);
                $remaining += $rewind;
                $chunk = substr($chunk, 0, $lastNl + 1);
                $chunkLen = $lastNl + 1;
            }

            $remaining -= $chunkLen;

            // V2 INNER LOOP: Comma-free and Math-based
            $pos = 0;
            while ($pos < $chunkLen) {
                // Find next newline. Standard lines are ~60-80 chars.
                $nextNl = strpos($chunk, "\n", $pos + 40);
                if ($nextNl === false)
                    break;

                // Optimization: The date/time suffix is fixed length (25 chars).
                // Example: ,2026-01-24T01:16:58+00:00\n
                // Byte offsets relative to $nextNl:
                // ,    => -26
                // YYYY => -25, -24, -23, -22
                // -    => -21
                // MM   => -20, -19
                // -    => -18
                // DD   => -17, -16

                // Pure Math Date Index (0-2555 range)
                $dateIdx = ((ord($chunk[$nextNl - 25]) - 48) * 1000 + (ord($chunk[$nextNl - 24]) - 48) * 100 + (ord($chunk[$nextNl - 23]) - 48) * 10 + (ord($chunk[$nextNl - 22]) - 48) - 2020) * 372
                    + ((ord($chunk[$nextNl - 20]) - 48) * 10 + (ord($chunk[$nextNl - 19]) - 48)) * 31
                    + ((ord($chunk[$nextNl - 17]) - 48) * 10 + (ord($chunk[$nextNl - 16]) - 48));

                // Path starts at $pos + URI_PREFIX_LENGTH.
                // It ends EXACTLY before the comma at $nextNl - 26.
                $pathLen = ($nextNl - 26) - ($pos + self::URI_PREFIX_LENGTH);
                $path = substr($chunk, $pos + self::URI_PREFIX_LENGTH, $pathLen);

                if (isset($pathIds[$path])) {
                    $counts[$pathIds[$path] * $stride + $dateIdx]++;
                }

                $pos = $nextNl + 1;
            }
        }

        fclose($handle);
        return $counts;
    }

    private function writeOutput(
        string $outputPath,
        array $pathNames,
        array $dates,
        array $counts
    ): void {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, self::WRITE_BUFFER_SIZE);
        fwrite($out, "{\n");

        $firstPath = true;
        foreach ($pathNames as $pathId => $path) {
            if (!$firstPath)
                fwrite($out, ",\n");
            $firstPath = false;

            $escapedPath = str_replace('/', '\\/', $path);
            fwrite($out, "    \"{$escapedPath}\": {\n");

            $firstDate = true;
            $base = $pathId * self::DATE_STRIDE;
            foreach ($dates as $dateIdx => $date) {
                $count = $counts[$base + $dateIdx];
                if ($count > 0) {
                    if (!$firstDate)
                        fwrite($out, ",\n");
                    $firstDate = false;
                    fwrite($out, "        \"{$date}\": {$count}");
                }
            }
            fwrite($out, "\n    }");
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
