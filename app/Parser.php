<?php

namespace App;

use App\Commands\Visit;

use function array_chunk;
use function array_count_values;
use function array_fill;
use function chr;
use function count;
use function fclose;
use function fgets;
use function file_get_contents;
use function file_put_contents;
use function filesize;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function getmypid;
use function max;
use function min;
use function pack;
use function pcntl_fork;
use function pcntl_waitpid;
use function str_replace;
use function strlen;
use function strpos;
use function strrpos;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function substr;
use function sys_get_temp_dir;
use function unlink;
use function unpack;

use const SEEK_CUR;

final class Parser
{
    private const URL_PREFIX_LENGTH = 25; // https://stitcher.io/blog/
    private const SAMPLE_BYTES = 2_097_152; // 2 MB probe
    private const READ_CHUNK_BYTES = 163_840; // tuned chunk size
    private const DEFAULT_WORKERS = 16;
    private const DATE_START_YEAR = 21;
    private const DATE_END_YEAR = 26;

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $fileSize = filesize($inputPath);

        [$datePacked, $dateLabels, $dateCount] = $this->buildDateTables();
        [$slugIds, $slugLabels, $slugCount] = $this->buildSlugTables($inputPath, $fileSize);

        $workers = $this->resolveWorkerCount($fileSize);
        $readChunkBytes = $this->resolveReadChunkBytes();
        $boundaries = $this->computeBoundaries($inputPath, $fileSize, $workers);

        if ($workers <= 1 || !function_exists('pcntl_fork')) {
            $counts = $this->crunch(
                $inputPath,
                0,
                $fileSize,
                $slugIds,
                $datePacked,
                $slugCount,
                $dateCount,
                $readChunkBytes,
            );

            $this->writeJson($outputPath, $counts, $slugLabels, $dateLabels, $dateCount);

            return;
        }

        $children = [];
        $chunkCount = count($boundaries) - 1;
        $tmpPrefix = sys_get_temp_dir() . '/p100m_' . getmypid() . '_';

        for ($worker = 0; $worker < $chunkCount - 1; $worker++) {
            $pid = pcntl_fork();

            if ($pid === -1) {
                break;
            }

            if ($pid === 0) {
                $result = $this->crunch(
                    $inputPath,
                    $boundaries[$worker],
                    $boundaries[$worker + 1],
                    $slugIds,
                    $datePacked,
                    $slugCount,
                    $dateCount,
                    $readChunkBytes,
                );

                file_put_contents($tmpPrefix . $worker, pack('v*', ...$result));
                exit(0);
            }

            $children[] = [$pid, $tmpPrefix . $worker];
        }

        $counts = $this->crunch(
            $inputPath,
            $boundaries[$chunkCount - 1],
            $boundaries[$chunkCount],
            $slugIds,
            $datePacked,
            $slugCount,
            $dateCount,
            $readChunkBytes,
        );

        foreach ($children as [$pid, $tmpPath]) {
            pcntl_waitpid($pid, $status);

            $workerRaw = file_get_contents($tmpPath);
            if ($workerRaw === false || $workerRaw === '') {
                continue;
            }

            $index = 0;
            foreach (unpack('v*', $workerRaw) as $value) {
                $counts[$index++] += $value;
            }
            unlink($tmpPath);
        }

        $this->writeJson($outputPath, $counts, $slugLabels, $dateLabels, $dateCount);
    }

    private function resolveWorkerCount(int $fileSize): int
    {
        $override = getenv('PARSER_WORKERS');
        if ($override !== false && $override !== '') {
            $workers = (int) $override;
        } else {
            $workers = self::DEFAULT_WORKERS;
            if (function_exists('posix_sysconf')) {
                $cpuCount = (int) posix_sysconf(POSIX_SC_NPROCESSORS_ONLN);
                if ($cpuCount > 0) {
                    // The challenge runner is an M1 host (8 cores); 10 workers performs better there.
                    if ($cpuCount <= 8) {
                        $workers = 10;
                    } else {
                        $workers = min(16, $cpuCount + 4);
                    }
                }
            }
        }

        if ($fileSize < 64 * 1024 * 1024) {
            return 1;
        }

        return max(1, $workers);
    }

    private function resolveReadChunkBytes(): int
    {
        $override = getenv('PARSER_CHUNK_BYTES');
        if ($override !== false && $override !== '') {
            $bytes = (int) $override;
            if ($bytes >= 65_536) {
                return $bytes;
            }
        }

        return self::READ_CHUNK_BYTES;
    }

    private function buildDateTables(): array
    {
        $datePacked = [];
        $dateLabels = [];
        $dateCount = 0;

        for ($year = self::DATE_START_YEAR; $year <= self::DATE_END_YEAR; $year++) {
            for ($month = 1; $month <= 12; $month++) {
                $maxDay = match ($month) {
                    2 => ($year % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };

                $monthPadded = ($month < 10 ? '0' : '') . $month;
                $yearMonth = $year . '-' . $monthPadded . '-';

                for ($day = 1; $day <= $maxDay; $day++) {
                    $date = $yearMonth . (($day < 10 ? '0' : '') . $day);
                    $datePacked[$date] = chr($dateCount & 0xFF) . chr($dateCount >> 8);
                    $dateLabels[$dateCount] = '20' . $date;
                    $dateCount++;
                }
            }
        }

        return [$datePacked, $dateLabels, $dateCount];
    }

    private function buildSlugTables(string $inputPath, int $fileSize): array
    {
        $slugIds = [];
        $slugLabels = [];
        $slugCount = 0;

        $probeSize = min(self::SAMPLE_BYTES, $fileSize);
        if ($probeSize > 0) {
            $handle = fopen($inputPath, 'rb');
            stream_set_read_buffer($handle, 0);
            $sample = fread($handle, $probeSize);
            fclose($handle);

            $lastNewline = strrpos($sample, "\n");
            if ($lastNewline !== false) {
                $position = 0;

                while ($position < $lastNewline) {
                    $newline = strpos($sample, "\n", $position + 52);
                    if ($newline === false) {
                        break;
                    }

                    $slug = substr(
                        $sample,
                        $position + self::URL_PREFIX_LENGTH,
                        $newline - $position - 51,
                    );

                    if (!isset($slugIds[$slug])) {
                        $slugIds[$slug] = $slugCount;
                        $slugLabels[$slugCount] = $slug;
                        $slugCount++;
                    }

                    $position = $newline + 1;
                }
            }
        }

        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, self::URL_PREFIX_LENGTH);

            if (!isset($slugIds[$slug])) {
                $slugIds[$slug] = $slugCount;
                $slugLabels[$slugCount] = $slug;
                $slugCount++;
            }
        }

        return [$slugIds, $slugLabels, $slugCount];
    }

    private function computeBoundaries(string $inputPath, int $fileSize, int $workers): array
    {
        if ($workers <= 1) {
            return [0, $fileSize];
        }

        $boundaries = [0];
        $handle = fopen($inputPath, 'rb');

        for ($index = 1; $index < $workers; $index++) {
            fseek($handle, (int) ($fileSize * $index / $workers));
            fgets($handle);
            $boundaries[] = ftell($handle);
        }

        fclose($handle);
        $boundaries[] = $fileSize;

        return $boundaries;
    }

    private function crunch(
        string $inputPath,
        int $start,
        int $end,
        array $slugIds,
        array $datePacked,
        int $slugCount,
        int $dateCount,
        int $readChunkBytes,
    ): array {
        $buckets = array_fill(0, $slugCount, '');

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $chunk = fread($handle, min($readChunkBytes, $remaining));
            $chunkLength = strlen($chunk);

            if ($chunkLength === 0) {
                break;
            }

            $remaining -= $chunkLength;

            $lastNewline = strrpos($chunk, "\n");
            if ($lastNewline === false) {
                continue;
            }

            $tail = $chunkLength - $lastNewline - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $slugPosition = self::URL_PREFIX_LENGTH;
            $fence = $lastNewline - 720;

            while ($slugPosition < $fence) {
                $separator = strpos($chunk, ',', $slugPosition);
                $buckets[$slugIds[substr($chunk, $slugPosition, $separator - $slugPosition)]]
                    .= $datePacked[substr($chunk, $separator + 3, 8)];
                $slugPosition = $separator + 52;

                $separator = strpos($chunk, ',', $slugPosition);
                $buckets[$slugIds[substr($chunk, $slugPosition, $separator - $slugPosition)]]
                    .= $datePacked[substr($chunk, $separator + 3, 8)];
                $slugPosition = $separator + 52;

                $separator = strpos($chunk, ',', $slugPosition);
                $buckets[$slugIds[substr($chunk, $slugPosition, $separator - $slugPosition)]]
                    .= $datePacked[substr($chunk, $separator + 3, 8)];
                $slugPosition = $separator + 52;

                $separator = strpos($chunk, ',', $slugPosition);
                $buckets[$slugIds[substr($chunk, $slugPosition, $separator - $slugPosition)]]
                    .= $datePacked[substr($chunk, $separator + 3, 8)];
                $slugPosition = $separator + 52;

                $separator = strpos($chunk, ',', $slugPosition);
                $buckets[$slugIds[substr($chunk, $slugPosition, $separator - $slugPosition)]]
                    .= $datePacked[substr($chunk, $separator + 3, 8)];
                $slugPosition = $separator + 52;

                $separator = strpos($chunk, ',', $slugPosition);
                $buckets[$slugIds[substr($chunk, $slugPosition, $separator - $slugPosition)]]
                    .= $datePacked[substr($chunk, $separator + 3, 8)];
                $slugPosition = $separator + 52;
            }

            while ($slugPosition < $lastNewline) {
                $separator = strpos($chunk, ',', $slugPosition);
                if ($separator === false) {
                    break;
                }

                $buckets[$slugIds[substr($chunk, $slugPosition, $separator - $slugPosition)]]
                    .= $datePacked[substr($chunk, $separator + 3, 8)];
                $slugPosition = $separator + 52;
            }
        }

        fclose($handle);

        $counts = array_fill(0, $slugCount * $dateCount, 0);

        for ($slug = 0; $slug < $slugCount; $slug++) {
            if ($buckets[$slug] === '') {
                continue;
            }

            $offset = $slug * $dateCount;

            foreach (array_count_values(unpack('v*', $buckets[$slug])) as $dateId => $visits) {
                $counts[$offset + $dateId] = $visits;
            }
        }

        return $counts;
    }

    private function writeJson(
        string $outputPath,
        array $counts,
        array $slugLabels,
        array $dateLabels,
        int $dateCount,
    ): void {
        $output = fopen($outputPath, 'wb');
        stream_set_write_buffer($output, 1_048_576);

        $datePrefixes = [];
        for ($date = 0; $date < $dateCount; $date++) {
            $datePrefixes[$date] = '        "' . $dateLabels[$date] . '": ';
        }

        $escapedPaths = [];
        $slugCount = count($slugLabels);
        for ($slug = 0; $slug < $slugCount; $slug++) {
            $escapedPaths[$slug] = '"\\/blog\\/' . str_replace('/', '\\/', $slugLabels[$slug]) . '"';
        }

        fwrite($output, '{');
        $first = true;

        for ($slug = 0; $slug < $slugCount; $slug++) {
            $base = $slug * $dateCount;
            $body = '';
            $sep = '';

            for ($date = 0; $date < $dateCount; $date++) {
                $value = $counts[$base + $date];
                if ($value === 0) {
                    continue;
                }
                $body .= $sep . $datePrefixes[$date] . $value;
                $sep = ",\n";
            }

            if ($body === '') {
                continue;
            }

            fwrite(
                $output,
                ($first ? '' : ',') . "\n    " . $escapedPaths[$slug] . ": {\n" . $body . "\n    }",
            );
            $first = false;
        }

        fwrite($output, "\n}");
        fclose($output);
    }
}
