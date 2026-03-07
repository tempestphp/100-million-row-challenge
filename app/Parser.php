<?php

namespace App;

use function array_fill;
use function fgets;
use function file_get_contents;
use function filesize;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function getmypid;
use function implode;
use function intdiv;
use function ksort;
use function pack;
use function pcntl_fork;
use function pcntl_waitpid;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function unlink;
use function unpack;

use const SEEK_CUR;
use const SEEK_SET;

final class Parser
{
    // private const STR_LENS_FOR_DUMMIES = [
    //     "https://stitcher.io/blog/`" => 25,
    //     "THH:MM:SS+00:00\n" => 16,
    //      "YYYY-MM-DD" => 10
    // ];
    private const int WORKERS = 10;
    private const int BUFFER_SIZE = 163_840;

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $fileSize = filesize($inputPath);

        $bounds = [0];
        $chunkSize = intdiv($fileSize, self::WORKERS);

        $fh = fopen($inputPath, 'rb');

        stream_set_read_buffer($fh, 0);

        for ($i = 1; $i < self::WORKERS; $i++) {
            fseek($fh, $i * $chunkSize, SEEK_SET);
            fgets($fh);
            $bounds[] = ftell($fh);
        }
        $bounds[] = $fileSize;

        fseek($fh, 0);

        $chunk = fread($fh, 4194304);

        $lastLineBreak = strrpos($chunk, "\n");
        $paths = [];
        $pathCount = 0;
        $pos = 25;
        $tPos = strpos($chunk, 'T', $pos);
        $minDate = substr($chunk, $tPos - 7, 7);

        while ($pos < $lastLineBreak) {
            $tPos = strpos($chunk, 'T', $pos);
            $path = substr($chunk, $pos, $tPos - $pos - 11);
            $date = substr($chunk, $tPos - 7, 7);

            if (!isset($paths[$path])) $paths[$path] = $pathCount++;
            $minDate = min($date, $minDate);

            $pos = $tPos + 41;
        }

        $dateCount = ((strtotime($minDate)+ 60 * 60 * 24 * 365 * 5) - strtotime($minDate)) / 86400 + 1;
        $matrixSize = $dateCount * $pathCount;

        $cy = (int)$minDate[0] + 2020;
        $cm = (int)substr($minDate, 2, 2);
        $cd = (int)substr($minDate, 5, 2);

        $dates = [];

        for ($i = 0; $i < $dateCount; $i++) {
            $dates[($cy % 10) . '-' . ($cm < 10 ? '0' : '') . $cm . '-' . ($cd < 10 ? '0' : '') . $cd] = $i;
            $dim = $cm === 2
                ? ($cy % 4 === 0 && ($cy % 100 !== 0 || $cy % 400 === 0) ? 29 : 28)
                : ($cm === 4 || $cm === 6 || $cm === 9 || $cm === 11 ? 30 : 31);
            if (++$cd > $dim) { $cd = 1; if (++$cm > 12) { $cm = 1; ++$cy; } }
        }
        $dateStrings = array_flip($dates);

        unset($dates);

        $dateIds = array_combine($dateStrings, str_split(pack('v*', ...range(0, $dateCount-1)), 2));

        $pid = getmypid();
        $shmDir = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
        $shmFiles = [];

        $merged = array_fill(0, $matrixSize, 0);
        $results = array_fill(0, $matrixSize, '');

        for ($w = 0; $w < self::WORKERS - 1; $w++) {
            $shmFile = "{$shmDir}/part_{$w}";
            $childPid = pcntl_fork();
            if ($childPid === 0) {
                fwrite(fopen($shmFile, 'wb'), self::parseChunk(
                    $inputPath,
                    $bounds[$w],
                    $bounds[$w + 1],
                    $paths,
                    $dateIds,
                    $matrixSize,
                    $dateCount,
                    $merged,
                    $results,
                    )
                    );
                exit(0);
            }
            $shmFiles[$childPid] = $shmFile;
        }

        $merged = self::parseChunk($inputPath, $bounds[$w], $bounds[$w + 1], $paths, $dateIds, $matrixSize, $dateCount, $merged, $results, true);

        $paths = array_keys($paths);

        $remaining = count($shmFiles);

        while ($remaining--) {
            $pid = pcntl_waitpid(-1, $status);
            $data = unpack('v*', file_get_contents($shmFiles[$pid]));
            unlink($shmFiles[$pid]);
            for ($i = 0; $i < $matrixSize; $i++) {
                $merged[$i] += $data[$i + 1];
            }
        }

        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 0);
        fwrite($out, '{');

        $buf = [];
        for ($j = 0; $j < $dateCount; $j++) {
            if ($c=$merged[$j]) {
                $buf[] = "        \"202{$dateStrings[$j]}\": {$c}";
            }
        }
        if ($buf) {
            fwrite($out, "\n    \"\/blog\/{$paths[0]}\": {\n". implode(",\n", $buf) . "\n    }");
        }

        $offset = 1;

        for ($i = $dateCount; $i < $matrixSize; $i+=$dateCount) {
            $curpath = $paths[$offset];
            $buf = [];
            $offset++;

            for ($j = 0; $j < $dateCount; $j++) {
                if ($c=$merged[$i+$j]) {
                    $buf[] = "        \"202{$dateStrings[$j]}\": {$c}";
                }
            }

            if ($buf) {
                fwrite($out, ",\n    \"\/blog\/{$curpath}\": {\n". implode(",\n", $buf) . "\n    }");
            }
        }

        fwrite($out, "\n}");
    }

    private static function parseChunk(
        string $inputPath,
        int $start,
        int $end,
        array $paths,
        array $dateIds,
        int $matrixSize,
        int $dateCount,
        $counts,
        $results,
        bool $master = false,
    ) {
        $pathCount = intdiv($matrixSize, $dateCount);

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);

        $remaining = $end - $start;

        while ($remaining > self::BUFFER_SIZE) {
            $chunk = fread($handle, self::BUFFER_SIZE);

            $remaining -= self::BUFFER_SIZE;

            $lastLineBreak = strrpos($chunk, "\n");

            if ($lastLineBreak < (self::BUFFER_SIZE - 1)) {
                $excess = self::BUFFER_SIZE - 1 - $lastLineBreak;
                fseek($handle, -$excess, SEEK_CUR);
                $remaining += $excess;
            }

            $pos = 25;

            while ($pos < $lastLineBreak) {
                $tPos = strpos($chunk, 'T', $pos);
                $results[$paths[substr($chunk, $pos, $tPos - $pos - 11)]] .= $dateIds[substr($chunk, $tPos - 7, 7)];
                $pos = $tPos + 41;
            }
        }
        if ($remaining) {
            $chunk = fread($handle, $remaining);
            $lastLineBreak = strlen($chunk);
            $pos = 25;

            while ($pos < $lastLineBreak) {
                $tPos = strpos($chunk, 'T', $pos);
                $results[$paths[substr($chunk, $pos, $tPos - $pos - 11)]] .= $dateIds[substr($chunk, $tPos - 7, 7)];
                $pos = $tPos + 41;
            }
        }

        for ($i = 0; $i < $pathCount; $i++) {
            $pathOffset = $i * $dateCount;
            foreach (array_count_values(unpack('v*', $results[$i])) as $dateOffset => $c) {
                $counts[$pathOffset + $dateOffset] = $c;
            }
        }
        if ($master) {
            return $counts;
        }

        return pack('v*', ...$counts);
    }
}
