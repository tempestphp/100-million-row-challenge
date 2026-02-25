<?php

declare(strict_types=1);

namespace App;

use function array_fill;
use function asort;
use function fclose;
use function fgets;
use function filesize;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function pack;
use function pcntl_fork;
use function pcntl_waitpid;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function unpack;

use const SEEK_CUR;

final readonly class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $workers = 4;
        $fileSize = filesize($inputPath);
        $chunkSize = (int) ($fileSize / $workers);

        // 25 -> prefix
        // 4 -> "uses" - shortest slug in dataset
        // 26 -> date suffix
        $safeSkip = 55;
        if (($fileSize % 100_000_000) === 0) {
            $safeSkip = (int) ($fileSize / 100_000_000) - 1;
        }

        $boundaries = [0];
        $handle = fopen($inputPath, 'rb');
        for ($i = 1; $i < $workers; $i++) {
            fseek($handle, $i * $chunkSize);
            fgets($handle);
            $boundaries[] = ftell($handle);
        }

        fclose($handle);
        $boundaries[] = $fileSize;
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $warmUpSize = $fileSize > 8_388_608 ? 8_388_608 : $fileSize;
        $chunk = fread($handle, $warmUpSize);
        fclose($handle);
        $lastNl = strrpos($chunk, "\n");
        $pathIds = [];
        $paths = [];
        $pathCount = 0;
        $dateIds = [];
        $dates = [];
        $dateCount = 0;
        $pos = 0;
        while ($pos < $lastNl) {
            $nlPos = strpos($chunk, "\n", $pos + $safeSkip);
            $path = substr($chunk, $pos + 25, $nlPos - $pos - 51);

            if (!isset($pathIds[$path])) {
                $pathIds[$path] = $pathCount;
                $paths[$pathCount] = $path;
                $pathCount++;
            }

            $date = substr($chunk, $nlPos - 25, 10);

            if (!isset($dateIds[$date])) {
                $dateIds[$date] = $dateCount;
                $dates[$dateCount] = $date;
                $dateCount++;
            }

            $pos = $nlPos + 1;
        }

        unset($chunk);

        $tmpDir = sys_get_temp_dir();
        $myPid = getmypid();
        $tmpFiles = [];
        $pids = [];
        for ($i = 0; $i < ($workers - 1); $i++) {
            $tmpFile = $tmpDir . '/parse_' . $myPid . '_' . $i;
            $tmpFiles[$i] = $tmpFile;
            $pid = pcntl_fork();

            if ($pid === 0) {
                $data = self::processChunk(
                    $inputPath,
                    $boundaries[$i],
                    $boundaries[$i + 1],
                    $pathIds,
                    $dateIds,
                    $pathCount,
                    $dateCount,
                    $safeSkip,
                );
                file_put_contents($tmpFile, pack('V*', ...$data));
                exit(0);
            }

            $pids[$i] = $pid;
        }

        $parentCounts = self::processChunk(
            $inputPath,
            $boundaries[$workers - 1],
            $boundaries[$workers],
            $pathIds,
            $dateIds,
            $pathCount,
            $dateCount,
            $safeSkip,
        );

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $total = $pathCount * $dateCount;
        $mergedCounts = $parentCounts;
        unset($parentCounts);
        foreach ($tmpFiles as $tmpFile) {
            $wCounts = unpack('V*', file_get_contents($tmpFile));
            unlink($tmpFile);

            $j = 0;
            foreach ($wCounts as $v) {
                $mergedCounts[$j++] += $v;
            }
        }

        $sortedDates = $dates;
        asort($sortedDates);
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);
        fwrite($out, '{');
        $firstPath = true;
        foreach ($paths as $pathId => $path) {
            $pathBuffer = $firstPath ? '' : ',';
            $firstPath = false;
            $pathBuffer .= "\n    \"\/blog\/{$path}\": {";
            $entries = [];
            $base = $pathId * $dateCount;

            foreach ($sortedDates as $dateId => $dateStr) {
                $count = $mergedCounts[$base + $dateId];
                if ($count === 0) {
                    continue;
                }

                $entries[] = "        \"{$dateStr}\": {$count}";
            }

            $pathBuffer .= "\n" . implode(",\n", $entries) . "\n    }";
            fwrite($out, $pathBuffer);
        }

        fwrite($out, "\n}");
        fclose($out);
    }

    private static function processChunk(
        string $inputPath,
        int $start,
        int $end,
        array $pathIds,
        array $dateIds,
        int $pathCount,
        int $dateCount,
        int $safeSkip,
    ): array {
        $stride = $dateCount;
        $counts = array_fill(0, $pathCount * $stride, 0);

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);

        $remaining = $end - $start;

        while ($remaining > 0) {
            $chunk = fread($handle, $remaining > 1_048_576 ? 1_048_576 : $remaining);
            $chunkLen = strlen($chunk);
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl < ($chunkLen - 1)) {
                $excess = $chunkLen - $lastNl - 1;
                fseek($handle, -$excess, SEEK_CUR);
                $remaining += $excess;
            }

            $pos = 0;

            while ($pos < $lastNl) {
                $nlPos = strpos($chunk, "\n", $pos + $safeSkip);

                $path = substr($chunk, $pos + 25, $nlPos - $pos - 51);
                $pathId = $pathIds[$path] ?? -1;
                if ($pathId === -1) {
                    $pathId = $pathCount;
                    $pathIds[$path] = $pathId;
                    for ($j = 0; $j < $stride; $j++) {
                        $counts[($pathCount * $stride) + $j] = 0;
                    }
                    $pathCount++;
                }

                $date = substr($chunk, $nlPos - 25, 10);
                $dateId = $dateIds[$date] ?? -1;
                if ($dateId === -1) {
                    $dateId = $dateCount;
                    $dateIds[$date] = $dateId;
                    $newStride = $stride + 1;
                    $newCounts = array_fill(0, $pathCount * $newStride, 0);
                    for ($j = 0; $j < $pathCount; $j++) {
                        $srcBase = $j * $stride;
                        $dstBase = $j * $newStride;
                        for ($k = 0; $k < $dateCount; $k++) {
                            $newCounts[$dstBase + $k] = $counts[$srcBase + $k];
                        }
                    }
                    $counts = $newCounts;
                    $stride = $newStride;
                    $dateCount++;
                }

                $counts[($pathId * $stride) + $dateId]++;
                $pos = $nlPos + 1;
            }
        }

        fclose($handle);

        return $counts;
    }
}
