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
use function getenv;
use function ord;
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
        $dateIds = array_fill(0, 4096, -1);
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

            $dateKey =
                ((ord($chunk[$nlPos - 22]) - 48) << 9)
                | ((((ord($chunk[$nlPos - 20]) - 48) * 10) + ord($chunk[$nlPos - 19]) - 48) << 5)
                | (((ord($chunk[$nlPos - 17]) - 48) * 10) + ord($chunk[$nlPos - 16]) - 48);

            if ($dateIds[$dateKey] === -1) {
                $dateIds[$dateKey] = $dateCount;
                $dates[$dateCount] = substr($chunk, $nlPos - 23, 8);
                $dateCount++;
            }

            $pos = $nlPos + 1;
        }

        unset($chunk);
        $quickPath = [];
        foreach ($paths as $id => $p) {
            $pLen = strlen($p);
            $fc = $p[0];
            $lc = $p[$pLen - 1];

            if (!isset($quickPath[$pLen][$fc][$lc])) {
                $quickPath[$pLen][$fc][$lc] = $id;
            } else {
                $quickPath[$pLen][$fc][$lc] = -1;
            }
        }

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
                    $quickPath,
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
            $quickPath,
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

                $entries[] = "        \"20{$dateStr}\": {$count}";
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
        array $quickPath,
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

                $pathLen = $nlPos - $pos - 51;
                $pathId = $quickPath[$pathLen][$chunk[$pos + 25]][$chunk[$nlPos - 27]] ?? -1;

                if ($pathId < 0) {
                    $path = substr($chunk, $pos + 25, $pathLen);
                    $pathId = $pathIds[$path] ?? $pathCount;
                    if ($pathId === $pathCount) {
                        $pathIds[$path] = $pathId;
                        for ($j = 0; $j < $stride; $j++) {
                            $counts[($pathCount * $stride) + $j] = 0;
                        }
                        $pathCount++;
                    }
                }

                $dateKey =
                    ((ord($chunk[$nlPos - 22]) - 48) << 9)
                    | ((((ord($chunk[$nlPos - 20]) - 48) * 10) + ord($chunk[$nlPos - 19]) - 48) << 5)
                    | (((ord($chunk[$nlPos - 17]) - 48) * 10) + ord($chunk[$nlPos - 16]) - 48);

                $dateId = $dateIds[$dateKey];
                if ($dateId === -1) {
                    $dateId = $dateCount;
                    $dateIds[$dateKey] = $dateId;
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
