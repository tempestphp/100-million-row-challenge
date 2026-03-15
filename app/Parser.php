<?php

namespace App;

use function array_fill;
use function chr;
use function chunk_split;
use function fclose;
use function feof;
use function fgets;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function intdiv;
use function pcntl_fork;
use function sodium_add;
use function str_repeat;
use function stream_select;
use function stream_set_chunk_size;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function stream_socket_pair;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function unpack;

final class Parser
{
    private const int WORKERS = 10;
    private const int DISCOVERY_BYTES = 262144;
    private const int READ_CHUNK = 393216;
    private const int MIN_SLUG_LEN = 4;
    private const int FLUSH_THRESHOLD = 4_194_304;
    private const int DISCOVERY_DUPLICATE_LIMIT = 500;

    public static function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $dateIds = [];
        $datePrefixes = [];
        $dateCount = 0;

        for ($year = 1; $year <= 6; $year++) {
            for ($month = 1; $month <= 12; $month++) {
                $daysInMonth = match ($month) {
                    2 => $year === 4 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };

                $monthString = $month < 10 ? "0{$month}" : (string) $month;
                $yearMonth = "{$year}-{$monthString}-";

                for ($day = 1; $day <= $daysInMonth; $day++) {
                    $dayString = $day < 10 ? "0{$day}" : (string) $day;
                    $key = $yearMonth . $dayString;
                    $dateIds[$key] = $dateCount;
                    $datePrefixes[$dateCount] = '        "202' . $key . '": ';
                    $dateCount++;
                }
            }
        }

        $next = [];
        for ($i = 0; $i < 255; $i++) {
            $next[chr($i)] = chr($i + 1);
        }
        $next[chr(255)] = chr(255);

        $input = fopen($inputPath, 'rb');
        stream_set_read_buffer($input, 0);
        $probe = fread($input, self::DISCOVERY_BYTES);

        $paths = [];
        $slugCount = 0;
        $position = 0;
        $lastNewline = strrpos($probe, "\n") ?: 0;
        $duplicateCount = 0;
        $seen = [];

        while ($position < $lastNewline) {
            $newline = strpos($probe, "\n", $position + 52);
            if ($newline === false) {
                break;
            }

            $slug = substr($probe, $position + 25, $newline - $position - 51);

            if (! isset($seen[$slug])) {
                $paths[$slugCount] = $slug;
                $seen[$slug] = true;
                $slugCount++;
                $duplicateCount = 0;
            } elseif (++$duplicateCount > self::DISCOVERY_DUPLICATE_LIMIT) {
                break;
            }

            $position = $newline + 1;
        }

        unset($probe);

        $prefix = 'https://stitcher.io/blog/';
        $packedMap = [];
        $maxStride = 0;

        for ($index = 0; $index < $slugCount; $index++) {
            $stride = strlen($paths[$index]) + 52;
            if ($stride > $maxStride) {
                $maxStride = $stride;
            }

            $key = substr($prefix . $paths[$index], -22);
            $packedMap[$key] = ($stride << 20) | ($index * $dateCount);
        }

        $outputSize = $slugCount * $dateCount;
        $socketSize = $outputSize * 2;

        fseek($input, 0, SEEK_END);
        $fileSize = ftell($input);

        $segments = [];
        for ($worker = 0; $worker < self::WORKERS; $worker++) {
            $from = intdiv($fileSize * $worker, self::WORKERS);
            $to = intdiv($fileSize * ($worker + 1), self::WORKERS);

            if ($from > 0) {
                fseek($input, $from);
                fgets($input);
                $from = ftell($input);
            }

            if ($worker < self::WORKERS - 1) {
                fseek($input, $to);
                fgets($input);
                $to = ftell($input);
            } else {
                $to = $fileSize;
            }

            $segments[$worker] = [$from, $to];
        }

        fclose($input);

        $sockets = [];

        for ($worker = 0; $worker < self::WORKERS - 1; $worker++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_chunk_size($pair[0], $socketSize);
            stream_set_chunk_size($pair[1], $socketSize);
            stream_set_read_buffer($pair[0], 0);

            if (pcntl_fork() === 0) {
                fclose($pair[0]);
                stream_set_write_buffer($pair[1], 0);

                [$from, $to] = $segments[$worker];
                $output = self::parseRangePacked(
                    $inputPath,
                    $from,
                    $to,
                    $packedMap,
                    $dateIds,
                    $next,
                    $outputSize,
                    $maxStride,
                );

                fwrite($pair[1], chunk_split($output, 1, "\0"));
                exit(0);
            }

            fclose($pair[1]);
            $sockets[$worker] = $pair[0];
        }

        [$from, $to] = $segments[self::WORKERS - 1];
        $mainOutput = self::parseRangePacked(
            $inputPath,
            $from,
            $to,
            $packedMap,
            $dateIds,
            $next,
            $outputSize,
            $maxStride,
        );

        $merged = chunk_split($mainOutput, 1, "\0");
        $buffers = array_fill(0, self::WORKERS - 1, '');
        $write = [];
        $except = [];

        while ($sockets !== []) {
            $read = $sockets;
            stream_select($read, $write, $except, null);

            foreach ($read as $worker => $socket) {
                $data = fread($socket, $socketSize);
                if ($data !== false && $data !== '') {
                    $buffers[$worker] .= $data;
                }

                if (feof($socket)) {
                    fclose($socket);
                    unset($sockets[$worker]);
                }
            }
        }

        for ($worker = 0; $worker < self::WORKERS - 1; $worker++) {
            sodium_add($merged, $buffers[$worker]);
        }

        $counts = unpack('v*', $merged);
        self::writeJson($outputPath, $counts, $paths, $datePrefixes, $dateCount, $slugCount, 1);
    }

    private static function parseRangePacked(
        string $inputPath,
        int $start,
        int $end,
        array $packedMap,
        array $dateIds,
        array $next,
        int $outputSize,
        int $maxStride,
    ): string {
        $guard = ($maxStride * 12) + 48;
        $guard4 = ($maxStride * 4) + 48;

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

            $p = $lastNewline;

            while ($p > $guard) {
                $packed = $packedMap[substr($chunk, $p - 48, 22)];
                $index = ($packed & 0xFFFFF) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20;
                $output[$index] = $next[$output[$index]];

                $packed = $packedMap[substr($chunk, $p - 48, 22)];
                $index = ($packed & 0xFFFFF) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20;
                $output[$index] = $next[$output[$index]];

                $packed = $packedMap[substr($chunk, $p - 48, 22)];
                $index = ($packed & 0xFFFFF) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20;
                $output[$index] = $next[$output[$index]];

                $packed = $packedMap[substr($chunk, $p - 48, 22)];
                $index = ($packed & 0xFFFFF) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20;
                $output[$index] = $next[$output[$index]];

                $packed = $packedMap[substr($chunk, $p - 48, 22)];
                $index = ($packed & 0xFFFFF) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20;
                $output[$index] = $next[$output[$index]];

                $packed = $packedMap[substr($chunk, $p - 48, 22)];
                $index = ($packed & 0xFFFFF) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20;
                $output[$index] = $next[$output[$index]];

                $packed = $packedMap[substr($chunk, $p - 48, 22)];
                $index = ($packed & 0xFFFFF) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20;
                $output[$index] = $next[$output[$index]];

                $packed = $packedMap[substr($chunk, $p - 48, 22)];
                $index = ($packed & 0xFFFFF) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20;
                $output[$index] = $next[$output[$index]];

                $packed = $packedMap[substr($chunk, $p - 48, 22)];
                $index = ($packed & 0xFFFFF) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20;
                $output[$index] = $next[$output[$index]];

                $packed = $packedMap[substr($chunk, $p - 48, 22)];
                $index = ($packed & 0xFFFFF) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20;
                $output[$index] = $next[$output[$index]];

                $packed = $packedMap[substr($chunk, $p - 48, 22)];
                $index = ($packed & 0xFFFFF) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20;
                $output[$index] = $next[$output[$index]];

                $packed = $packedMap[substr($chunk, $p - 48, 22)];
                $index = ($packed & 0xFFFFF) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20;
                $output[$index] = $next[$output[$index]];
            }

            while ($p > $guard4) {
                $packed = $packedMap[substr($chunk, $p - 48, 22)];
                $index = ($packed & 0xFFFFF) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20;
                $output[$index] = $next[$output[$index]];

                $packed = $packedMap[substr($chunk, $p - 48, 22)];
                $index = ($packed & 0xFFFFF) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20;
                $output[$index] = $next[$output[$index]];

                $packed = $packedMap[substr($chunk, $p - 48, 22)];
                $index = ($packed & 0xFFFFF) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20;
                $output[$index] = $next[$output[$index]];

                $packed = $packedMap[substr($chunk, $p - 48, 22)];
                $index = ($packed & 0xFFFFF) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20;
                $output[$index] = $next[$output[$index]];
            }

            while ($p >= 48) {
                $packed = $packedMap[substr($chunk, $p - 48, 22)];
                $index = ($packed & 0xFFFFF) + $dateIds[substr($chunk, $p - 22, 7)];
                $p -= $packed >> 20;
                $output[$index] = $next[$output[$index]];
            }
        }

        fclose($handle);

        return $output;
    }

    private static function writeJson(
        string $outputPath,
        array $counts,
        array $paths,
        array $datePrefixes,
        int $dateCount,
        int $slugCount,
        int $baseOffset,
    ): void {
        $output = fopen($outputPath, 'wb');
        stream_set_write_buffer($output, 4_194_304);

        $escapedPaths = [];
        for ($path = 0; $path < $slugCount; $path++) {
            $escapedPaths[$path] = '"\/blog\/' . $paths[$path] . '": {';
        }

        $separator = "\n    ";
        $base = $baseOffset;
        $buffer = '{';

        for ($path = 0; $path < $slugCount; $path++) {
            $firstDate = -1;
            $index = $base;

            for ($date = 0; $date < $dateCount; $date++) {
                if (($counts[$index] ?? 0) !== 0) {
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
