<?php

namespace App;

use Exception;
use Throwable;

final class Parser
{
    private const URI_PREFIX_LENGTH = 19; // "https://stitcher.io"
    private const READ_BUFFER_SIZE = 8 * 1024 * 1024;
    private const PARALLEL_MIN_BYTES = 16 * 1024 * 1024;

    public function parse(string $inputPath, string $outputPath): void
    {
        $counts = $this->shouldRunInParallel($inputPath)
            ? $this->parseInParallel($inputPath)
            : $this->parseRange($inputPath, 0, null);

        foreach ($counts as &$daily) {
            ksort($daily, SORT_STRING);
        }
        unset($daily);

        $json = json_encode($counts, JSON_PRETTY_PRINT | JSON_THROW_ON_ERROR);

        if (file_put_contents($outputPath, $json) === false) {
            throw new Exception("Failed to write output file: {$outputPath}");
        }
    }

    private function shouldRunInParallel(string $inputPath): bool
    {
        if (! function_exists('pcntl_fork') || ! function_exists('pcntl_waitpid')) {
            return false;
        }

        if (! function_exists('stream_socket_pair') || ! defined('STREAM_PF_UNIX') || ! defined('STREAM_SOCK_STREAM')) {
            return false;
        }

        $size = filesize($inputPath);

        return $size !== false && $size >= self::PARALLEL_MIN_BYTES;
    }

    /** @return array<string, array<string, int>> */
    private function parseInParallel(string $inputPath): array
    {
        $size = filesize($inputPath);
        if ($size === false) {
            throw new Exception("Failed to read file size: {$inputPath}");
        }

        $splitOffset = $this->findSplitOffset($inputPath, (int) ($size / 2));

        if ($splitOffset <= 0 || $splitOffset >= $size) {
            return $this->parseRange($inputPath, 0, null);
        }

        $workers = [];
        $socketIndex = [];

        try {
            for ($worker = 0; $worker < 2; $worker++) {
                $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
                if ($pair === false) {
                    throw new Exception('Failed to create IPC channel for worker');
                }

                $pid = pcntl_fork();

                if ($pid === -1) {
                    fclose($pair[0]);
                    fclose($pair[1]);
                    throw new Exception('Failed to fork parser worker');
                }

                if ($pid === 0) {
                    fclose($pair[0]);
                    $start = $worker === 0 ? 0 : $splitOffset;
                    $end = $worker === 0 ? $splitOffset : null;

                    try {
                        $counts = $this->parseRange($inputPath, $start, $end);
                        $serialized = serialize($counts);

                        $written = 0;
                        $length = strlen($serialized);

                        while ($written < $length) {
                            $chunkWritten = fwrite($pair[1], substr($serialized, $written, 262144));

                            if ($chunkWritten === false || $chunkWritten === 0) {
                                fclose($pair[1]);
                                exit(1);
                            }

                            $written += $chunkWritten;
                        }

                        fclose($pair[1]);
                        exit(0);
                    } catch (Throwable $exception) {
                        fclose($pair[1]);
                        fwrite(STDERR, $exception->getMessage() . PHP_EOL);
                        exit(1);
                    }
                }

                fclose($pair[1]);
                stream_set_blocking($pair[0], false);

                $workers[$worker] = [
                    'pid' => $pid,
                    'socket' => $pair[0],
                    'payload' => '',
                ];
                $socketIndex[(int) $pair[0]] = $worker;
            }

            while ($socketIndex !== []) {
                $readSockets = [];
                foreach ($socketIndex as $resourceId => $worker) {
                    $readSockets[] = $workers[$worker]['socket'];
                }

                $writeSockets = null;
                $exceptSockets = null;
                $selected = stream_select($readSockets, $writeSockets, $exceptSockets, 1);
                if ($selected === false) {
                    throw new Exception('Failed while reading worker IPC stream');
                }

                if ($selected === 0) {
                    continue;
                }

                foreach ($readSockets as $socket) {
                    $resourceId = (int) $socket;
                    $worker = $socketIndex[$resourceId] ?? null;
                    if ($worker === null) {
                        continue;
                    }

                    $chunk = fread($socket, 262144);
                    if ($chunk === false) {
                        throw new Exception('Failed to read worker output stream');
                    }

                    if ($chunk === '') {
                        if (feof($socket)) {
                            fclose($socket);
                            unset($socketIndex[$resourceId]);
                        }

                        continue;
                    }

                    $workers[$worker]['payload'] .= $chunk;
                }
            }

            foreach ($workers as $workerData) {
                $status = 0;
                if (pcntl_waitpid($workerData['pid'], $status) === -1) {
                    throw new Exception('Failed to wait parser worker');
                }

                if (! pcntl_wifexited($status) || pcntl_wexitstatus($status) !== 0) {
                    throw new Exception('Parser worker failed');
                }
            }

            $merged = [];

            for ($worker = 0; $worker < 2; $worker++) {
                $payload = $workers[$worker]['payload'];
                $partial = unserialize($payload, ['allowed_classes' => false]);
                if (! is_array($partial)) {
                    throw new Exception('Invalid worker output');
                }

                $this->mergeCounts($merged, $partial);
            }

            return $merged;
        } finally {
            foreach ($workers as $workerData) {
                if (isset($workerData['socket']) && is_resource($workerData['socket'])) {
                    fclose($workerData['socket']);
                }
            }
        }
    }

    /** @return array<string, array<string, int>> */
    private function parseRange(string $inputPath, int $startOffset, ?int $endOffset): array
    {
        $handle = fopen($inputPath, 'rb');
        if ($handle === false) {
            throw new Exception("Failed to open input file: {$inputPath}");
        }

        stream_set_read_buffer($handle, self::READ_BUFFER_SIZE);

        if ($startOffset > 0 && fseek($handle, $startOffset) !== 0) {
            fclose($handle);
            throw new Exception("Failed to seek input file: {$inputPath}");
        }

        $counts = [];
        $position = $startOffset;
        $carry = '';

        try {
            while (true) {
                if ($endOffset !== null && $position >= $endOffset) {
                    break;
                }

                $bytesToRead = self::READ_BUFFER_SIZE;
                if ($endOffset !== null) {
                    $remaining = $endOffset - $position;
                    if ($remaining <= 0) {
                        break;
                    }

                    if ($remaining < $bytesToRead) {
                        $bytesToRead = $remaining;
                    }
                }

                $chunk = fread($handle, $bytesToRead);
                if ($chunk === false || $chunk === '') {
                    break;
                }

                $position += strlen($chunk);
                $data = $carry . $chunk;
                $start = 0;

                while (($newline = strpos($data, "\n", $start)) !== false) {
                    $lineLength = $newline - $start;
                    if ($lineLength <= 0) {
                        $start = $newline + 1;
                        continue;
                    }

                    if ($data[$newline - 1] === "\r") {
                        $lineLength--;
                        if ($lineLength <= 0) {
                            $start = $newline + 1;
                            continue;
                        }
                    }

                    $lineEnd = $start + $lineLength;
                    $comma = strpos($data, ',', $start);

                    if ($comma !== false && $comma < $lineEnd && $comma > $start + self::URI_PREFIX_LENGTH - 1 && ($comma + 10) < $lineEnd) {
                        $pathStart = $start + self::URI_PREFIX_LENGTH;
                        $path = substr($data, $pathStart, $comma - $pathStart);
                        $date = substr($data, $comma + 1, 10);

                        if (! isset($counts[$path])) {
                            $counts[$path] = [$date => 1];
                        } elseif (! isset($counts[$path][$date])) {
                            $counts[$path][$date] = 1;
                        } else {
                            $counts[$path][$date]++;
                        }
                    }

                    $start = $newline + 1;
                }

                if ($start === 0) {
                    $carry = $data;
                } elseif ($start < strlen($data)) {
                    $carry = substr($data, $start);
                } else {
                    $carry = '';
                }
            }
        } finally {
            fclose($handle);
        }

        if ($endOffset === null && $carry !== '') {
            $comma = strpos($carry, ',');
            if ($comma !== false && $comma > self::URI_PREFIX_LENGTH - 1 && isset($carry[$comma + 10])) {
                $path = substr($carry, self::URI_PREFIX_LENGTH, $comma - self::URI_PREFIX_LENGTH);
                $date = substr($carry, $comma + 1, 10);

                if (! isset($counts[$path])) {
                    $counts[$path] = [$date => 1];
                } elseif (! isset($counts[$path][$date])) {
                    $counts[$path][$date] = 1;
                } else {
                    $counts[$path][$date]++;
                }
            }
        }

        return $counts;
    }

    private function findSplitOffset(string $inputPath, int $target): int
    {
        if ($target <= 0) {
            return 0;
        }

        $handle = fopen($inputPath, 'rb');
        if ($handle === false) {
            throw new Exception("Failed to open input file: {$inputPath}");
        }

        try {
            if (fseek($handle, $target) !== 0) {
                return 0;
            }

            if (fgets($handle) === false) {
                $position = ftell($handle);

                return $position === false ? 0 : $position;
            }

            $position = ftell($handle);

            return $position === false ? 0 : $position;
        } finally {
            fclose($handle);
        }
    }

    /**
     * @param array<string, array<string, int>> $target
     * @param array<string, array<string, int>> $source
     */
    private function mergeCounts(array &$target, array $source): void
    {
        foreach ($source as $path => $dailyCounts) {
            if (! isset($target[$path])) {
                $target[$path] = $dailyCounts;

                continue;
            }

            foreach ($dailyCounts as $date => $count) {
                if (! isset($target[$path][$date])) {
                    $target[$path][$date] = $count;

                    continue;
                }

                $target[$path][$date] += $count;
            }
        }
    }
}
