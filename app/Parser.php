<?php

namespace App;

use function array_fill;
use function chr;
use function fclose;
use function feof;
use function fgets;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function implode;
use function intdiv;
use function pcntl_fork;
use function stream_select;
use function stream_set_chunk_size;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function stream_socket_pair;
use function strlen;
use function strpos;
use function str_repeat;
use function str_replace;
use function strrpos;
use function substr;
use function unpack;
use const SEEK_CUR;
use const SEEK_END;
use const STREAM_IPPROTO_IP;
use const STREAM_PF_UNIX;
use const STREAM_SOCK_STREAM;

final class Parser
{
    protected const int WORKER_COUNT = 8;
    protected const int READ_CHUNK_SIZE = 163_840;
    protected const int WARMUP_BYTES = 2_097_152;

    public static function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $dateIdentifierByDate = [];
        $datesByIdentifier = [];
        $dateCount = 0;

        for ($year = 21; $year <= 26; $year++) {
            for ($month = 1; $month <= 12; $month++) {
                $maxDay = match ($month) {
                    2 => $year === 24 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };

                $monthString = ($month < 10 ? '0' : '') . $month;
                $yearMonthPrefix = $year . '-' . $monthString . '-';

                for ($day = 1; $day <= $maxDay; $day++) {
                    $date = $yearMonthPrefix . (($day < 10 ? '0' : '') . $day);
                    $dateIdentifierByDate[$date] = $dateCount;
                    $datesByIdentifier[$dateCount] = '20' . $date;
                    $dateCount++;
                }
            }
        }

        $next = [];
        for ($value = 0; $value < 255; $value++) {
            $next[chr($value)] = chr($value + 1);
        }

        $boundaryHandle = fopen($inputPath, 'rb');
        stream_set_read_buffer($boundaryHandle, 0);

        $raw = fread($boundaryHandle, self::WARMUP_BYTES);

        $slugBaseOffsetBySlug = [];
        $slugBySlugIdentifier = [];
        $slugCount = 0;
        $position = 0;
        $lastNewlinePosition = strrpos($raw, "\n");
        if ($lastNewlinePosition === false) {
            $lastNewlinePosition = 0;
        }

        while ($position < $lastNewlinePosition) {
            $newlinePosition = strpos($raw, "\n", $position + 52);
            if ($newlinePosition === false) {
                break;
            }

            $slug = substr($raw, $position + 25, $newlinePosition - $position - 51);

            if (! isset($slugBaseOffsetBySlug[$slug])) {
                $slugBySlugIdentifier[$slugCount] = $slug;
                $slugBaseOffsetBySlug[$slug] = $slugCount * $dateCount;
                $slugCount++;
            }

            $position = $newlinePosition + 1;
        }
        unset($raw);

        fseek($boundaryHandle, 0, SEEK_END);
        $fileSize = ftell($boundaryHandle);
        $step = intdiv($fileSize, self::WORKER_COUNT);

        $boundaries = [0];
        for ($worker = 1; $worker < self::WORKER_COUNT; $worker++) {
            fseek($boundaryHandle, $step * $worker);
            fgets($boundaryHandle);
            $boundaries[] = ftell($boundaryHandle);
        }
        $boundaries[] = $fileSize;
        fclose($boundaryHandle);

        $outputSize = $slugCount * $dateCount;
        $sockets = [];

        for ($worker = 0; $worker < self::WORKER_COUNT; $worker++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_chunk_size($pair[0], $outputSize);
            stream_set_chunk_size($pair[1], $outputSize);

            if (pcntl_fork() === 0) {
                $output = self::parseRange(
                    $inputPath,
                    $boundaries[$worker],
                    $boundaries[$worker + 1],
                    $slugBaseOffsetBySlug,
                    $dateIdentifierByDate,
                    $next,
                    $outputSize,
                );
                fwrite($pair[1], $output);
                fclose($pair[1]);
                exit(0);
            }

            fclose($pair[1]);
            $sockets[$worker] = $pair[0];
        }

        $counts = array_fill(0, $outputSize, 0);
        $offsetByWorker = array_fill(0, self::WORKER_COUNT, 0);

        while ($sockets !== []) {
            $read = $sockets;
            $write = [];
            $except = [];
            stream_select($read, $write, $except, 5);

            foreach ($read as $worker => $socket) {
                $data = fread($socket, $outputSize);

                if ($data !== '' && $data !== false) {
                    $offset = $offsetByWorker[$worker];
                    foreach (unpack('C*', $data) as $value) {
                        $counts[$offset] += $value;
                        $offset++;
                    }
                    $offsetByWorker[$worker] = $offset;
                }

                if (feof($socket)) {
                    fclose($socket);
                    unset($sockets[$worker]);
                }
            }
        }

        self::writeJson(
            $outputPath,
            $counts,
            $slugBySlugIdentifier,
            $datesByIdentifier,
            $dateCount,
            $slugCount,
        );
    }

    protected static function parseRange(
        string $inputPath,
        int $start,
        int $end,
        array $slugBaseOffsetBySlug,
        array $dateIdentifierByDate,
        array $next,
        int $outputSize,
    ): string {
        $output = str_repeat("\0", $outputSize);

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $readLength = $remaining > self::READ_CHUNK_SIZE ? self::READ_CHUNK_SIZE : $remaining;
            $chunk = fread($handle, $readLength);
            $chunkLength = strlen($chunk);
            $remaining -= $chunkLength;

            $lastNewlinePosition = strrpos($chunk, "\n");
            if ($lastNewlinePosition === false) {
                break;
            }

            $tailLength = $chunkLength - $lastNewlinePosition - 1;
            if ($tailLength > 0) {
                fseek($handle, -$tailLength, SEEK_CUR);
                $remaining += $tailLength;
            }

            $position = 25;
            $unrolledFence = $lastNewlinePosition - 1010;

            while ($position < $unrolledFence) {
                for ($i = 0; $i < 10; $i++) {
                    $separatorPosition = strpos($chunk, ',', $position);
                    $index = $slugBaseOffsetBySlug[substr($chunk, $position, $separatorPosition - $position)] + $dateIdentifierByDate[substr($chunk, $separatorPosition + 3, 8)];
                    $output[$index] = $next[$output[$index]];
                    $position = $separatorPosition + 52;
                }
            }

            while ($position < $lastNewlinePosition) {
                $separatorPosition = strpos($chunk, ',', $position);
                if ($separatorPosition === false || $separatorPosition >= $lastNewlinePosition) {
                    break;
                }

                $index = $slugBaseOffsetBySlug[substr($chunk, $position, $separatorPosition - $position)] + $dateIdentifierByDate[substr($chunk, $separatorPosition + 3, 8)];
                $output[$index] = $next[$output[$index]];
                $position = $separatorPosition + 52;
            }
        }

        fclose($handle);

        return $output;
    }

    protected static function writeJson(
        string $outputPath,
        array $counts,
        array $slugBySlugIdentifier,
        array $datesByIdentifier,
        int $dateCount,
        int $slugCount,
    ): void {
        $outputHandle = fopen($outputPath, 'wb');
        stream_set_write_buffer($outputHandle, 1_048_576);
        fwrite($outputHandle, '{');

        $datePrefixByDateIdentifier = [];
        for ($dateIdentifier = 0; $dateIdentifier < $dateCount; $dateIdentifier++) {
            $datePrefixByDateIdentifier[$dateIdentifier] = '        "' . $datesByIdentifier[$dateIdentifier] . '": ';
        }

        $escapedPathBySlugIdentifier = [];
        for ($slugIdentifier = 0; $slugIdentifier < $slugCount; $slugIdentifier++) {
            $escapedPathBySlugIdentifier[$slugIdentifier] = '"\/blog\/' . str_replace('/', '\/', $slugBySlugIdentifier[$slugIdentifier]) . '": {';
        }

        $isFirstPath = true;
        $base = 0;

        for ($slugIdentifier = 0; $slugIdentifier < $slugCount; $slugIdentifier++) {
            $firstNonZeroDateIdentifier = -1;

            for ($dateIdentifier = 0; $dateIdentifier < $dateCount; $dateIdentifier++) {
                if ($counts[$base + $dateIdentifier] !== 0) {
                    $firstNonZeroDateIdentifier = $dateIdentifier;
                    break;
                }
            }

            if ($firstNonZeroDateIdentifier === -1) {
                $base += $dateCount;
                continue;
            }

            $buffer = $isFirstPath ? "\n    " : ",\n    ";
            $isFirstPath = false;
            $buffer .= $escapedPathBySlugIdentifier[$slugIdentifier] . "\n" . $datePrefixByDateIdentifier[$firstNonZeroDateIdentifier] . $counts[$base + $firstNonZeroDateIdentifier];

            for ($dateIdentifier = $firstNonZeroDateIdentifier + 1; $dateIdentifier < $dateCount; $dateIdentifier++) {
                $count = $counts[$base + $dateIdentifier];
                if ($count === 0) {
                    continue;
                }

                $buffer .= ",\n" . $datePrefixByDateIdentifier[$dateIdentifier] . $count;
            }

            $buffer .= "\n    }";
            fwrite($outputHandle, $buffer);
            $base += $dateCount;
        }

        fwrite($outputHandle, "\n}");
        fclose($outputHandle);
    }
}
