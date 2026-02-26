<?php

declare(strict_types=1);

namespace App;

use App\Commands\Visit;
use function array_fill;
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
use function getmypid;
use function implode;
use function pack;
use function pcntl_fork;
use function pcntl_waitpid;
use function str_replace;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function sys_get_temp_dir;
use function unlink;
use function unpack;
use const SEEK_CUR;

final class Parser
{
    private const int READ_CHUNK_SIZE = 163_840;
    private const int WRITE_BUFFER_SIZE = 0;
    private const int URI_PREFIX_LENGTH = 19;
    private const int DATE_LENGTH = 10;
    private const int DATE_OFFSET_FROM_NL = 25;
    private const int MIN_LINE_LENGTH = 45;
    private const int WORKERS = 8;
    private const int DISCOVER_SIZE = 131_072;

    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);

        $boundaries = $this->calculateBoundaries(
            $inputPath,
            $fileSize,
        );

        [$pathIds, $paths, $pathCount] = $this->discover($inputPath, $fileSize);
        $this->registerMissingPaths($pathIds, $paths, $pathCount);
        [$dateIds, $dates, $dateCount] = $this->buildDates();
        $flatCount = $pathCount * $dateCount;

        $tmpDir = sys_get_temp_dir();
        $myPid = getmypid();
        $tmpFiles = [];
        $pids = [];

        for ($i = 0; $i < self::WORKERS - 1; $i++) {
            $tmpFile = $tmpDir . '/parse_' . $myPid . '_' . $i;
            $tmpFiles[$i] = $tmpFile;

            $pid = pcntl_fork();

            if ($pid === 0) {
                $data = $this->parseRangeFlat(
                    $inputPath,
                    $boundaries[$i],
                    $boundaries[$i + 1],
                    $pathIds,
                    $dateIds,
                    $pathCount,
                    $dateCount,
                );

                file_put_contents($tmpFile, pack('V*', ...$data));

                exit(0);
            }

            if ($pid < 0) {
                throw new \RuntimeException('Unable to fork parser worker');
            }

            $pids[$i] = $pid;
        }

        $mergedCounts = $this->parseRangeFlat(
            $inputPath,
            $boundaries[self::WORKERS - 1],
            $boundaries[self::WORKERS],
            $pathIds,
            $dateIds,
            $pathCount,
            $dateCount,
        );

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        foreach ($tmpFiles as $tmpFile) {
            $wCounts = unpack('V*', (string) file_get_contents($tmpFile));
            unlink($tmpFile);

            $k = 1;

            for ($j = 0; $j < $flatCount; $j++) {
                $mergedCounts[$j] += $wCounts[$k++];
            }
        }

        $this->writeOutput($outputPath, $paths, $dates, $mergedCounts, $dateCount);
    }

    private function discover(string $inputPath, int $fileSize): array
    {
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $warmUpSize = $fileSize > self::DISCOVER_SIZE ? self::DISCOVER_SIZE : $fileSize;
        $chunk = fread($handle, $warmUpSize);
        fclose($handle);

        $lastNl = strrpos($chunk, "\n");
        $pathIds = [];
        $paths = [];
        $pathCount = 0;
        $pos = 0;

        while ($pos < $lastNl) {
            $nlPos = strpos($chunk, "\n", $pos + 50);

            $path = substr(
                $chunk,
                $pos + self::URI_PREFIX_LENGTH,
                $nlPos - $pos - self::MIN_LINE_LENGTH,
            );

            if (! isset($pathIds[$path])) {
                $pathIds[$path] = $pathCount;
                $paths[$pathCount] = $path;
                $pathCount++;
            }

            $pos = $nlPos + 1;
        }

        return [$pathIds, $paths, $pathCount];
    }

    private function buildDates(): array
    {
        $dateIds = [];
        $dates = [];
        $dateCount = 0;

        for ($y = 2020; $y <= 2026; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $daysInMonth = match ($m) {
                    2 => ($y % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };

                for ($d = 1; $d <= $daysInMonth; $d++) {
                    $date = $y . '-' . ($m < 10 ? '0' : '') . $m . '-' . ($d < 10 ? '0' : '') . $d;
                    $dateIds[$date] = $dateCount;
                    $dates[$dateCount] = $date;
                    $dateCount++;
                }
            }
        }

        return [$dateIds, $dates, $dateCount];
    }

    private function registerMissingPaths(array &$pathIds, array &$paths, int &$pathCount): void
    {
        foreach (Visit::all() as $visit) {
            $path = substr($visit->uri, self::URI_PREFIX_LENGTH);

            if (isset($pathIds[$path])) {
                continue;
            }

            $pathIds[$path] = $pathCount;
            $paths[$pathCount] = $path;
            $pathCount++;
        }
    }

    private function calculateBoundaries(
        string $inputPath,
        int $fileSize,
    ): array {
        $chunkSize = (int) ($fileSize / self::WORKERS);
        $boundaries = [0];

        $handle = fopen($inputPath, 'rb');

        for ($i = 1; $i < self::WORKERS; $i++) {
            fseek($handle, $i * $chunkSize);
            fgets($handle);
            $boundaries[] = ftell($handle);
        }

        fclose($handle);

        $boundaries[] = $fileSize;

        return $boundaries;
    }

    private function parseRangeFlat(
        string $inputPath,
        int $start,
        int $end,
        array $pathIds,
        array $dateIds,
        int $pathCount,
        int $dateCount,
    ): array {
        $stride = $dateCount;
        $counts = array_fill(0, $pathCount * $stride, 0);

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);

        $remaining = $end - $start;

        while ($remaining > 0) {
            $chunk = fread($handle, $remaining > self::READ_CHUNK_SIZE ? self::READ_CHUNK_SIZE : $remaining);
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
                $nlPos = strpos($chunk, "\n", $pos + 50);

                $path = substr(
                    $chunk,
                    $pos + self::URI_PREFIX_LENGTH,
                    $nlPos - $pos - self::MIN_LINE_LENGTH,
                );
                $pathId = $pathIds[$path];
                $date = substr(
                    $chunk,
                    $nlPos - self::DATE_OFFSET_FROM_NL,
                    self::DATE_LENGTH,
                );
                $dateId = $dateIds[$date];

                $counts[($pathId * $stride) + $dateId]++;

                $pos = $nlPos + 1;
            }
        }

        fclose($handle);

        return $counts;
    }

    private function writeOutput(
        string $outputPath,
        array $paths,
        array $dates,
        array $counts,
        int $dateCount,
    ): void {
        $datePrefixes = [];

        for ($dateId = 0; $dateId < $dateCount; $dateId++) {
            $datePrefixes[$dateId] = "        \"{$dates[$dateId]}\": ";
        }

        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, self::WRITE_BUFFER_SIZE);
        fwrite($out, '{');

        $firstPath = true;

        foreach ($paths as $pathId => $path) {
            $entries = [];
            $base = $pathId * $dateCount;

            for ($dateId = 0; $dateId < $dateCount; $dateId++) {
                $count = $counts[$base + $dateId];

                if ($count === 0) {
                    continue;
                }

                $entries[] = $datePrefixes[$dateId] . $count;
            }

            if ($entries === []) {
                continue;
            }

            $pathBuffer = $firstPath ? '' : ',';
            $firstPath = false;
            $escapedPath = str_replace('/', '\\/', $path);
            $pathBuffer .= "\n    \"{$escapedPath}\": {";

            $pathBuffer .= "\n" . implode(",\n", $entries) . "\n    }";
            fwrite($out, $pathBuffer);
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
