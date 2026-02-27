<?php

declare(strict_types=1);

namespace App;

use App\Commands\Visit;
use parallel\Runtime;

use function array_count_values;
use function array_fill;
use function chr;
use function count;
use function fclose;
use function fgets;
use function filesize;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function implode;
use function str_replace;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function unpack;

use const SEEK_CUR;

final class Parser
{
    private const int WORKERS = 8;
    private const int READ_CHUNK = 1_048_576; // 1MB
    private const int DISCOVER_SIZE = 2_097_152; // 2MB
    private const int URI_PREFIX_LENGTH = 25; // Length of "https://stitcher.io"
    private const int DATE_LENGTH = 8; // Length of "26-01-24" (two-digit year)
    private const int LINE_SUFFIX_LENGTH = 26; // Length of ",2026-01-24T01:16:58+00:00\n"

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $fileSize = filesize($inputPath);

        // 1. Generate date mappings
        [$dateIds, $dates, $dateCount] = $this->generateDateMappings();

        // 2. Discover all paths
        [$pathIds, $paths, $pathCount] = $this->discoverPaths($inputPath, $fileSize);

        // Precompute path offsets to avoid multiplication in hot loop
        $pathOffsets = [];
        for ($i = 0; $i < $pathCount; $i++) {
            $pathOffsets[$i] = $i * $dateCount;
        }

        // 3. Determine parallel strategy
        $parallelType = $this->getParallelType();

        // 4. Execute parsing with appropriate parallel strategy
        $counts = $this->executeParsing(
            $parallelType,
            $inputPath,
            $fileSize,
            $pathIds,
            $dateIds,
            $pathCount,
            $dateCount,
            $pathOffsets
        );

        // 5. Write output
        $this->writeJson($outputPath, $counts, $paths, $dates, $dateCount);
    }

    private function generateDateMappings(): array
    {
        $dateIds = [];
        $dates = [];
        $dateCount = 0;
        
        // Years 2020-2026 inclusive
        for ($y = 20; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => (($y + 2000) % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $mStr = ($m < 10 ? '0' : '') . $m;
                $ymStr = $y . '-' . $mStr . '-';
                for ($d = 1; $d <= $maxD; $d++) {
                    $key = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $dateIds[$key] = $dateCount;
                    $dates[$dateCount] = $key;
                    $dateCount++;
                }
            }
        }

        return [$dateIds, $dates, $dateCount];
    }



    private function discoverPaths(string $inputPath, int $fileSize): array
    {
        $pathIds = [];
        $paths = [];
        $pathCount = 0;

        // Read first DISCOVER_SIZE bytes to discover most paths
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $warmUpSize = $fileSize > self::DISCOVER_SIZE ? self::DISCOVER_SIZE : $fileSize;
        $raw = fread($handle, $warmUpSize);
        fclose($handle);

        $pos = 0;
        $lastNl = strrpos($raw, "\n");

        while ($pos < $lastNl) {
            $nlPos = strpos($raw, "\n", $pos + 52); // Minimum line length
            if ($nlPos === false) {
                break;
            }

            // Extract path: between URI prefix and comma
            $pathStart = $pos + self::URI_PREFIX_LENGTH;
            $commaPos = strpos($raw, ',', $pathStart);
            if ($commaPos === false || $commaPos >= $nlPos) {
                $pos = $nlPos + 1;
                continue;
            }

            $slug = substr($raw, $pathStart, $commaPos - $pathStart);
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug] = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }

            $pos = $nlPos + 1;
        }

        unset($raw);

        // Also include paths from Visit::all() to ensure completeness
        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, self::URI_PREFIX_LENGTH);
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug] = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }
        }

        return [$pathIds, $paths, $pathCount];
    }

    private function getParallelType(): string
    {
        if (extension_loaded('parallel') && class_exists('parallel\Runtime')) {
            return 'parallel';
        }
        
        if (function_exists('pcntl_fork')) {
            return 'pcntl';
        }
        
        return 'single';
    }

    private function executeParsing(
        string $parallelType,
        string $inputPath,
        int $fileSize,
        array $pathIds,
        array $dateIds,
        int $pathCount,
        int $dateCount,
        array $pathOffsets
    ): array {
        switch ($parallelType) {
            case 'parallel':
                return $this->executeParallelRuntime(
                    $inputPath,
                    $fileSize,
                    $pathIds,
                    $dateIds,
                    $pathCount,
                    $dateCount,
                    $pathOffsets
                );
            case 'pcntl':
                return $this->executePcntl(
                    $inputPath,
                    $fileSize,
                    $pathIds,
                    $dateIds,
                    $pathCount,
                    $dateCount,
                    $pathOffsets
                );
            default:
                return $this->parseRange(
                    $inputPath,
                    0,
                    $fileSize,
                    $pathIds,
                    $dateIds,
                    $pathCount,
                    $dateCount,
                    $pathOffsets
                );
        }
    }

    private function executeParallelRuntime(
        string $inputPath,
        int $fileSize,
        array $pathIds,
        array $dateIds,
        int $pathCount,
        int $dateCount,
        array $pathOffsets
    ): array {
        // Calculate boundaries at line boundaries
        $boundaries = $this->calculateBoundaries($inputPath, $fileSize, self::WORKERS);
        $futures = [];

        // Launch workers (all but last)
        for ($w = 0; $w < self::WORKERS - 1; $w++) {
            $runtime = new Runtime();
            $wStart = $boundaries[$w];
            $wEnd = $boundaries[$w + 1];
            
            $futures[] = $runtime->run(
                static function (
                    string $inputPath,
                    int $start,
                    int $end,
                    array $pathIds,
                    array $dateIds,
                    int $pathCount,
                    int $dateCount,
                    array $pathOffsets,
                    int $readChunk
                ): array {
                    $counts = array_fill(0, $pathCount * $dateCount, 0);
                    $handle = fopen($inputPath, 'rb');
                    stream_set_read_buffer($handle, 0);
                    fseek($handle, $start);
                    $remaining = $end - $start;

                    while ($remaining > 0) {
                        $toRead = $remaining > $readChunk ? $readChunk : $remaining;
                        $chunk = fread($handle, $toRead);
                        $chunkLen = strlen($chunk);
                        if ($chunkLen === 0) {
                            break;
                        }
                        $remaining -= $chunkLen;

                        $lastNl = strrpos($chunk, "\n");
                        if ($lastNl === false) {
                            break;
                        }

                        // Handle partial line at end of chunk
                        $tail = $chunkLen - $lastNl - 1;
                        if ($tail > 0) {
                            fseek($handle, -$tail, SEEK_CUR);
                            $remaining += $tail;
                        }

                        // Process chunk with unrolled loop
                        $p = self::URI_PREFIX_LENGTH;
                        $fence = $lastNl - 720; // Enough for 6 lines (6 * ~120 chars)

                        // Unrolled loop: process 6 lines per iteration
                        while ($p < $fence) {
                            // Line 1
                            $sep = strpos($chunk, ',', $p);
                            $pathId = $pathIds[substr($chunk, $p, $sep - $p)];
                            $dateKey = substr($chunk, $sep + 3, self::DATE_LENGTH);
                            $counts[$pathOffsets[$pathId] + $dateIds[$dateKey]]++;
                            $p = $sep + self::LINE_SUFFIX_LENGTH + self::URI_PREFIX_LENGTH + 1;

                            // Line 2
                            $sep = strpos($chunk, ',', $p);
                            $pathId = $pathIds[substr($chunk, $p, $sep - $p)];
                            $dateKey = substr($chunk, $sep + 3, self::DATE_LENGTH);
                            $counts[$pathOffsets[$pathId] + $dateIds[$dateKey]]++;
                            $p = $sep + self::LINE_SUFFIX_LENGTH + self::URI_PREFIX_LENGTH + 1;

                            // Line 3
                            $sep = strpos($chunk, ',', $p);
                            $pathId = $pathIds[substr($chunk, $p, $sep - $p)];
                            $dateKey = substr($chunk, $sep + 3, self::DATE_LENGTH);
                            $counts[$pathOffsets[$pathId] + $dateIds[$dateKey]]++;
                            $p = $sep + self::LINE_SUFFIX_LENGTH + self::URI_PREFIX_LENGTH + 1;

                            // Line 4
                            $sep = strpos($chunk, ',', $p);
                            $pathId = $pathIds[substr($chunk, $p, $sep - $p)];
                            $dateKey = substr($chunk, $sep + 3, self::DATE_LENGTH);
                            $counts[$pathOffsets[$pathId] + $dateIds[$dateKey]]++;
                            $p = $sep + self::LINE_SUFFIX_LENGTH + self::URI_PREFIX_LENGTH + 1;

                            // Line 5
                            $sep = strpos($chunk, ',', $p);
                            $pathId = $pathIds[substr($chunk, $p, $sep - $p)];
                            $dateKey = substr($chunk, $sep + 3, self::DATE_LENGTH);
                            $counts[$pathOffsets[$pathId] + $dateIds[$dateKey]]++;
                            $p = $sep + self::LINE_SUFFIX_LENGTH + self::URI_PREFIX_LENGTH + 1;

                            // Line 6
                            $sep = strpos($chunk, ',', $p);
                            $pathId = $pathIds[substr($chunk, $p, $sep - $p)];
                            $dateKey = substr($chunk, $sep + 3, self::DATE_LENGTH);
                            $counts[$pathOffsets[$pathId] + $dateIds[$dateKey]]++;
                            $p = $sep + self::LINE_SUFFIX_LENGTH + self::URI_PREFIX_LENGTH + 1;
                        }

                        // Process remaining lines
                        while ($p < $lastNl) {
                            $sep = strpos($chunk, ',', $p);
                            if ($sep === false || $sep >= $lastNl) {
                                break;
                            }
                            $pathId = $pathIds[substr($chunk, $p, $sep - $p)];
                            $dateKey = substr($chunk, $sep + 3, self::DATE_LENGTH);
                            $counts[$pathOffsets[$pathId] + $dateIds[$dateKey]]++;
                            $p = $sep + self::LINE_SUFFIX_LENGTH + self::URI_PREFIX_LENGTH + 1;
                        }
                    }

                    fclose($handle);

                    return $counts;
                },
                [
                    $inputPath,
                    $wStart,
                    $wEnd,
                    $pathIds,
                    $dateIds,
                    $pathCount,
                    $dateCount,
                    $pathOffsets,
                    self::READ_CHUNK
                ]
            );
        }

        // Process last chunk in main thread
        $counts = $this->parseRange(
            $inputPath,
            $boundaries[self::WORKERS - 1],
            $boundaries[self::WORKERS],
            $pathIds,
            $dateIds,
            $pathCount,
            $dateCount,
            $pathOffsets
        );

        // Merge results from workers
        foreach ($futures as $future) {
            $wCounts = $future->value();
            $j = 0;
            foreach ($wCounts as $v) {
                $counts[$j++] += $v;
            }
        }

        return $counts;
    }

    private function executePcntl(
        string $inputPath,
        int $fileSize,
        array $pathIds,
        array $dateIds,
        int $pathCount,
        int $dateCount
    ): array {
        $numWorkers = min(self::WORKERS, 4); // Limit pcntl workers for stability
        $boundaries = $this->calculateBoundaries($inputPath, $fileSize, $numWorkers);

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
                    // Child process
                    $counts = $this->parseRange(
                        $inputPath,
                        $boundaries[$i],
                        $boundaries[$i + 1],
                        $pathIds,
                        $dateIds,
                        $pathCount,
                        $dateCount
                    );
                    file_put_contents($tmpFile, pack('V*', ...$counts));
                    exit(0);
                }
                $pids[] = $pid;
            } else {
                // Last chunk in main process
                $mergedCounts = $this->parseRange(
                    $inputPath,
                    $boundaries[$i],
                    $boundaries[$i + 1],
                    $pathIds,
                    $dateIds,
                    $pathCount,
                    $dateCount
                );
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

    private function calculateBoundaries(string $inputPath, int $fileSize, int $numWorkers): array
    {
        $boundaries = [0];
        if ($numWorkers <= 1) {
            $boundaries[] = $fileSize;
            return $boundaries;
        }

        $handle = fopen($inputPath, 'rb');
        for ($i = 1; $i < $numWorkers; $i++) {
            fseek($handle, (int) ($fileSize * $i / $numWorkers));
            fgets($handle); // Move to next line boundary
            $boundaries[] = ftell($handle);
        }
        fclose($handle);
        $boundaries[] = $fileSize;

        return $boundaries;
    }

    private function parseRange(
        string $inputPath,
        int $start,
        int $end,
        array $pathIds,
        array $dateIds,
        int $pathCount,
        int $dateCount,
        array $pathOffsets
    ): array {
        $counts = array_fill(0, $pathCount * $dateCount, 0);
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $toRead = $remaining > self::READ_CHUNK ? self::READ_CHUNK : $remaining;
            $chunk = fread($handle, $toRead);
            $chunkLen = strlen($chunk);
            if ($chunkLen === 0) {
                break;
            }
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) {
                break;
            }

            // Handle partial line at end of chunk
            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            // Process chunk with unrolled loop
            $p = self::URI_PREFIX_LENGTH;
            $fence = $lastNl - 720; // Enough for 6 lines

            // Unrolled loop: process 6 lines per iteration
            while ($p < $fence) {
                // Line 1
                $sep = strpos($chunk, ',', $p);
                $pathId = $pathIds[substr($chunk, $p, $sep - $p)];
                $dateKey = substr($chunk, $sep + 3, self::DATE_LENGTH);
                $counts[$pathOffsets[$pathId] + $dateIds[$dateKey]]++;
                $p = $sep + self::LINE_SUFFIX_LENGTH + self::URI_PREFIX_LENGTH + 1;

                // Line 2
                $sep = strpos($chunk, ',', $p);
                $pathId = $pathIds[substr($chunk, $p, $sep - $p)];
                $dateKey = substr($chunk, $sep + 3, self::DATE_LENGTH);
                $counts[$pathOffsets[$pathId] + $dateIds[$dateKey]]++;
                $p = $sep + self::LINE_SUFFIX_LENGTH + self::URI_PREFIX_LENGTH + 1;

                // Line 3
                $sep = strpos($chunk, ',', $p);
                $pathId = $pathIds[substr($chunk, $p, $sep - $p)];
                $dateKey = substr($chunk, $sep + 3, self::DATE_LENGTH);
                $counts[$pathOffsets[$pathId] + $dateIds[$dateKey]]++;
                $p = $sep + self::LINE_SUFFIX_LENGTH + self::URI_PREFIX_LENGTH + 1;

                // Line 4
                $sep = strpos($chunk, ',', $p);
                $pathId = $pathIds[substr($chunk, $p, $sep - $p)];
                $dateKey = substr($chunk, $sep + 3, self::DATE_LENGTH);
                $counts[$pathOffsets[$pathId] + $dateIds[$dateKey]]++;
                $p = $sep + self::LINE_SUFFIX_LENGTH + self::URI_PREFIX_LENGTH + 1;

                // Line 5
                $sep = strpos($chunk, ',', $p);
                $pathId = $pathIds[substr($chunk, $p, $sep - $p)];
                $dateKey = substr($chunk, $sep + 3, self::DATE_LENGTH);
                $counts[$pathOffsets[$pathId] + $dateIds[$dateKey]]++;
                $p = $sep + self::LINE_SUFFIX_LENGTH + self::URI_PREFIX_LENGTH + 1;

                // Line 6
                $sep = strpos($chunk, ',', $p);
                $pathId = $pathIds[substr($chunk, $p, $sep - $p)];
                $dateKey = substr($chunk, $sep + 3, self::DATE_LENGTH);
                $counts[$pathOffsets[$pathId] + $dateIds[$dateKey]]++;
                $p = $sep + self::LINE_SUFFIX_LENGTH + self::URI_PREFIX_LENGTH + 1;
            }

            // Process remaining lines
            while ($p < $lastNl) {
                $sep = strpos($chunk, ',', $p);
                if ($sep === false || $sep >= $lastNl) {
                    break;
                }
                $pathId = $pathIds[substr($chunk, $p, $sep - $p)];
                $dateKey = substr($chunk, $sep + 3, self::DATE_LENGTH);
                $counts[$pathOffsets[$pathId] + $dateIds[$dateKey]]++;
                $p = $sep + self::LINE_SUFFIX_LENGTH + self::URI_PREFIX_LENGTH + 1;
            }
        }

        fclose($handle);

        return $counts;
    }

    private function writeJson(
        string $outputPath,
        array $counts,
        array $paths,
        array $dates,
        int $dateCount
    ): void {
        // Pre-compute date prefixes for faster output
        $datePrefixes = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "20' . $dates[$d] . '": ';
        }

        $pathCount = count($paths);
        $escapedPaths = [];
        for ($p = 0; $p < $pathCount; $p++) {
            $escapedPaths[$p] = '"\\/blog\\/' . str_replace('/', '\/', $paths[$p]) . '"';
        }

        // Build output in memory for faster writing
        $output = "{";
        $firstPath = true;
        for ($p = 0; $p < $pathCount; $p++) {
            $base = $p * $dateCount;
            $dateEntries = [];

            for ($d = 0; $d < $dateCount; $d++) {
                $count = $counts[$base + $d];
                if ($count === 0) {
                    continue;
                }
                $dateEntries[] = $datePrefixes[$d] . $count;
            }

            if ($dateEntries === []) {
                continue;
            }

            if ($firstPath) {
                $output .= "\n    ";
                $firstPath = false;
            } else {
                $output .= ",\n    ";
            }
            $output .= $escapedPaths[$p] . ": {\n" . implode(",\n", $dateEntries) . "\n    }";
        }

        $output .= "\n}";
        
        file_put_contents($outputPath, $output);
    }
}
