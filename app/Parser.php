<?php

namespace App;

use function array_fill;
use function chr;
use function fclose;
use function feof;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function ord;
use function pcntl_fork;
use function stream_select;
use function stream_set_chunk_size;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function stream_socket_pair;
use function strlen;
use function strpos;
use function str_replace;
use function strrpos;
use function substr;

final class Parser
{
    private const int WORKERS = 10;
    private const int DISCOVERY_BYTES = 262144;
    private const int READ_CHUNK = 393216;
    private const int MIN_SLUG_LEN = 4;
    private const int FLUSH_THRESHOLD = 4_194_304;
    private const int SOCKET_CHUNK = 262144;
    private const int DISCOVERY_DUPLICATE_LIMIT = 500;

    public static function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $dateIds = [];
        $dates = [];
        $dateCount = 0;

        for ($year = 21; $year <= 26; $year++) {
            for ($month = 1; $month <= 12; $month++) {
                $daysInMonth = match ($month) {
                    2 => $year === 24 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };

                $monthString = ($month < 10 ? '0' : '') . $month;
                $yearMonth = "{$year}-{$monthString}-";

                for ($day = 1; $day <= $daysInMonth; $day++) {
                    $dayString = ($day < 10 ? '0' : '') . $day;
                    $key = $yearMonth . $dayString;
                    $dateIds[$key] = $dateCount;
                    $dates[$dateCount] = '20' . $key;
                    $dateCount++;
                }
            }
        }

        $next = [];
        for ($i = 0; $i < 255; $i++) {
            $next[chr($i)] = chr($i + 1);
        }

        $input = fopen($inputPath, 'rb');
        stream_set_read_buffer($input, 0);
        $probe = fread($input, self::DISCOVERY_BYTES);

        $paths = [];
        $slugBaseMap = [];
        $slugCount = 0;
        $position = 0;
        $lastNewline = strrpos($probe, "\n") ?: 0;
        $duplicateCount = 0;

        while ($position < $lastNewline) {
            $newline = strpos($probe, "\n", $position + 52);
            if ($newline === false) {
                break;
            }

            $slug = substr($probe, $position + 25, $newline - $position - 51);

            if (! isset($slugBaseMap[$slug])) {
                $paths[$slugCount] = $slug;
                $slugBaseMap[$slug] = $slugCount * $dateCount;
                $slugCount++;
                $duplicateCount = 0;
            } elseif (++$duplicateCount > self::DISCOVERY_DUPLICATE_LIMIT) {
                break;
            }

            $position = $newline + 1;
        }

        unset($probe);

        $outputSize = $slugCount * $dateCount;

        stream_set_read_buffer($input, 8192);
        fseek($input, 0, SEEK_END);
        $fileSize = ftell($input);
        $step = $fileSize >> 3;
        $boundaries = [0];

        for ($worker = 1; $worker < self::WORKERS; $worker++) {
            fseek($input, $step * $worker);
            fgets($input);
            $boundaries[] = ftell($input);
        }

        fclose($input);
        $boundaries[] = $fileSize;

        $sockets = [];

        for ($worker = 0; $worker < self::WORKERS; $worker++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_chunk_size($pair[0], self::SOCKET_CHUNK);
            stream_set_read_buffer($pair[0], 0);

            if (pcntl_fork() === 0) {
                fclose($pair[0]);
                stream_set_write_buffer($pair[1], 0);

                $output = self::parseRange(
                    $inputPath,
                    $boundaries[$worker],
                    $boundaries[$worker + 1],
                    $slugBaseMap,
                    $dateIds,
                    $next,
                    $outputSize,
                );

                fwrite($pair[1], $output);
                exit(0);
            }

            fclose($pair[1]);
            $sockets[$worker] = $pair[0];
        }

        $counts = array_fill(0, $outputSize, 0);
        $offsets = array_fill(0, self::WORKERS, 0);
        $write = [];
        $except = [];

        while ($sockets !== []) {
            $read = $sockets;
            stream_select($read, $write, $except, 5);

            foreach ($read as $worker => $socket) {
                $data = fread($socket, $outputSize);

                if ($data !== '') {
                    $offset = $offsets[$worker];
                    $length = strlen($data);

                    for ($index = 0; $index < $length; $index++) {
                        $counts[$offset++] += ord($data[$index]);
                    }

                    $offsets[$worker] = $offset;
                }

                if (feof($socket)) {
                    fclose($socket);
                    unset($sockets[$worker]);
                }
            }
        }

        self::writeJson($outputPath, $counts, $paths, $dates, $dateCount, $slugCount);
    }

    private static function parseRange(
        string $inputPath,
        int $start,
        int $end,
        array $slugBaseMap,
        array $dateIds,
        array $next,
        int $outputSize,
    ): string {
        $output = str_repeat("\0", $outputSize);
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $chunk = fread($handle, $remaining > self::READ_CHUNK ? self::READ_CHUNK : $remaining);
            $chunkLength = strlen($chunk);
            $remaining -= $chunkLength;

            $lastNewline = strrpos($chunk, "\n");
            if ($lastNewline === false) {
                break;
            }

            $tailLength = $chunkLength - $lastNewline - 1;
            if ($tailLength > 0) {
                fseek($handle, -$tailLength, SEEK_CUR);
                $remaining += $tailLength;
            }

            $position = 25;
            $fence = $lastNewline - 1002;

            while ($position < $fence) {
                $separator = strpos($chunk, ',', $position + self::MIN_SLUG_LEN);
                $index = $slugBaseMap[substr($chunk, $position, $separator - $position)] + $dateIds[substr($chunk, $separator + 3, 8)];
                $output[$index] = $next[$output[$index]];
                $position = $separator + 52;

                $separator = strpos($chunk, ',', $position + self::MIN_SLUG_LEN);
                $index = $slugBaseMap[substr($chunk, $position, $separator - $position)] + $dateIds[substr($chunk, $separator + 3, 8)];
                $output[$index] = $next[$output[$index]];
                $position = $separator + 52;

                $separator = strpos($chunk, ',', $position + self::MIN_SLUG_LEN);
                $index = $slugBaseMap[substr($chunk, $position, $separator - $position)] + $dateIds[substr($chunk, $separator + 3, 8)];
                $output[$index] = $next[$output[$index]];
                $position = $separator + 52;

                $separator = strpos($chunk, ',', $position + self::MIN_SLUG_LEN);
                $index = $slugBaseMap[substr($chunk, $position, $separator - $position)] + $dateIds[substr($chunk, $separator + 3, 8)];
                $output[$index] = $next[$output[$index]];
                $position = $separator + 52;

                $separator = strpos($chunk, ',', $position + self::MIN_SLUG_LEN);
                $index = $slugBaseMap[substr($chunk, $position, $separator - $position)] + $dateIds[substr($chunk, $separator + 3, 8)];
                $output[$index] = $next[$output[$index]];
                $position = $separator + 52;

                $separator = strpos($chunk, ',', $position + self::MIN_SLUG_LEN);
                $index = $slugBaseMap[substr($chunk, $position, $separator - $position)] + $dateIds[substr($chunk, $separator + 3, 8)];
                $output[$index] = $next[$output[$index]];
                $position = $separator + 52;

                $separator = strpos($chunk, ',', $position + self::MIN_SLUG_LEN);
                $index = $slugBaseMap[substr($chunk, $position, $separator - $position)] + $dateIds[substr($chunk, $separator + 3, 8)];
                $output[$index] = $next[$output[$index]];
                $position = $separator + 52;

                $separator = strpos($chunk, ',', $position + self::MIN_SLUG_LEN);
                $index = $slugBaseMap[substr($chunk, $position, $separator - $position)] + $dateIds[substr($chunk, $separator + 3, 8)];
                $output[$index] = $next[$output[$index]];
                $position = $separator + 52;

                $separator = strpos($chunk, ',', $position + self::MIN_SLUG_LEN);
                $index = $slugBaseMap[substr($chunk, $position, $separator - $position)] + $dateIds[substr($chunk, $separator + 3, 8)];
                $output[$index] = $next[$output[$index]];
                $position = $separator + 52;

                $separator = strpos($chunk, ',', $position + self::MIN_SLUG_LEN);
                $index = $slugBaseMap[substr($chunk, $position, $separator - $position)] + $dateIds[substr($chunk, $separator + 3, 8)];
                $output[$index] = $next[$output[$index]];
                $position = $separator + 52;
            }

            while ($position < $lastNewline) {
                $separator = strpos($chunk, ',', $position + self::MIN_SLUG_LEN);
                if ($separator === false || $separator >= $lastNewline) {
                    break;
                }

                $index = $slugBaseMap[substr($chunk, $position, $separator - $position)] + $dateIds[substr($chunk, $separator + 3, 8)];
                $output[$index] = $next[$output[$index]];
                $position = $separator + 52;
            }
        }

        fclose($handle);

        return $output;
    }

    private static function writeJson(
        string $outputPath,
        array $counts,
        array $paths,
        array $dates,
        int $dateCount,
        int $slugCount,
    ): void {
        $output = fopen($outputPath, 'wb');
        stream_set_write_buffer($output, 4_194_304);

        $datePrefixes = [];
        for ($date = 0; $date < $dateCount; $date++) {
            $datePrefixes[$date] = '        "' . $dates[$date] . '": ';
        }

        $escapedPaths = [];
        for ($path = 0; $path < $slugCount; $path++) {
            $escapedPaths[$path] = '"\/blog\/' . str_replace('/', '\/', $paths[$path]) . '": {';
        }

        $separator = "\n    ";
        $base = 0;
        $buffer = '{';

        for ($path = 0; $path < $slugCount; $path++) {
            $firstDate = -1;
            $index = $base;

            for ($date = 0; $date < $dateCount; $date++) {
                if ($counts[$index] !== 0) {
                    $firstDate = $date;
                    break;
                }
                $index++;
            }

            if ($firstDate !== -1) {
                $buffer .= $separator . $escapedPaths[$path] . "\n" . $datePrefixes[$firstDate] . $counts[$index];
                $separator = ",\n    ";

                for ($date = $firstDate + 1; $date < $dateCount; $date++) {
                    $index++;
                    if ($counts[$index] === 0) {
                        continue;
                    }

                    $buffer .= ",\n" . $datePrefixes[$date] . $counts[$index];
                }

                $buffer .= "\n    }";

                if (strlen($buffer) > self::FLUSH_THRESHOLD) {
                    fwrite($output, $buffer);
                    $buffer = '';
                }
            }

            $base += $dateCount;
        }

        fwrite($output, $buffer . "\n}");
        fclose($output);
    }
}
