<?php

namespace App;

final class Parser
{
    public const int CONCURRENCY = 10;
    public const int CHUNK_SIZE = 1024 * 1024 * 16;
    public const string POTENTIAL_FIRST_DAY = '2021-01-01';

    public function parse(string $inputPath, string $outputPath): void
    {
        \gc_disable();

        $offsets = $this->splitFilesIntoByteOffsetsChunks($inputPath);
        $chunks = \array_chunk($offsets, \ceil(\count($offsets) / self::CONCURRENCY));

        $childrenSockets = [];
        foreach ($chunks as $chunk) {
            $socketPair = \stream_socket_pair(\AF_UNIX, \SOCK_STREAM, 0);
            if ($socketPair === false) {
                throw new \RuntimeException('Failed to create socket');
            }

            [$parentHandle, $childHandle] = $socketPair;
            \stream_set_blocking($parentHandle, false);
            \stream_set_blocking($childHandle, true);

            $pid = \pcntl_fork();
            if ($pid === -1) {
                throw new \RuntimeException('Failed to fork');
            }

            if ($pid === 0) {
                \fclose($parentHandle);
                $this->parseDataFromOffsetsInWorker($inputPath, $chunk, $childHandle);
                exit(0);
            }

            \fclose($childHandle);
            $childrenSockets[] = $parentHandle;
        }

        $days = [];
        $period = new \DatePeriod(
            new \DateTime(self::POTENTIAL_FIRST_DAY), new \DateInterval('P1D'), new \DateTime()
        );
        foreach ($period as $date) {
            $days[] = $date->format('Y-m-d');
        }

        $results = $this->receiveDataFromWorkers($childrenSockets, $days);

        $done = [];
        foreach ($results as $basePath => $dailyCounts) {
            $keyedDailyCounts = \array_filter(\array_combine($days, $dailyCounts));
            $done['/blog/' . $basePath] = $keyedDailyCounts;
        }

        \file_put_contents($outputPath, \json_encode($done, \JSON_PRETTY_PRINT));
    }

    private function splitFilesIntoByteOffsetsChunks(string $path): array
    {
        $size = \filesize($path);
        $file = \fopen($path, "r");

        $chunks = [];
        $start = 0;

        while ($start < $size) {
            $target = \min($start + self::CHUNK_SIZE, $size);

            if ($target >= $size) {
                $chunks[] = [$start, $size - $start];
                break;
            }

            \fseek($file, $target);
            \fgets($file);
            $end = \ftell($file);

            $chunks[] = [$start, $end - $start];
            $start = $end;
        }

        \fclose($file);
        return $chunks;
    }

    /** @param resource[] $sockets */
    private function receiveDataFromWorkers(array $sockets, array $days): array
    {
        $daysCount = \count($days);

        $readBuffers = \array_fill(0, \count($sockets), '');
        $messageLengths = \array_fill(0, \count($sockets), null); // null = length unknwon (ie wait frame)
        $urlOrders = \array_fill(0, \count($sockets), []);

        $results = [];

        $write = $except = null;
        while (\count($sockets) > 0) {
            $read = $sockets;
            \stream_select($read, $write, $except, null);

            foreach ($read as $socket) {
                $workerIndex = \array_search($socket, $sockets);

                $chunk = \fread($socket, 1024 * 1024 * 4);
                if ($chunk === false || $chunk === '') {
                    unset($sockets[$workerIndex]);
                    \fclose($socket);
                    continue;
                }

                $messageLength = &$messageLengths[$workerIndex];
                $buffer = &$readBuffers[$workerIndex];
                $buffer .= $chunk;
                $bufferLength = \strlen($buffer);

                $position = 0;

                while (true) {
                    if ($messageLength === null) {
                        if ($bufferLength - $position < 8) {
                            break; // incomplete header
                        }
                        $messageLength = \unpack('q', $buffer, $position)[1];
                        $position += 8;
                    }

                    if ($bufferLength - $position < $messageLength) {
                        break; // incomplete message
                    }

                    $urlLength = \unpack('v', $buffer, $position)[1];
                    $url = \substr($buffer, $position + 2, $urlLength);

                    $counts = \unpack(
                        'v*',
                        \substr($buffer, $position + 2 + $urlLength, $messageLength - $urlLength - 2)
                    );

                    $position += $messageLength;
                    $messageLength = null;

                    $urlResults = &$results[$url];
                    if (!isset($urlOrders[$workerIndex][$url])) {
                        if ($urlResults === null) {
                            $urlResults = \array_fill(0, $daysCount, 0);
                        }
                        $urlOrders[$workerIndex][] = $url;
                    }

                    $i = 0;
                    foreach ($counts as $count) {
                        $urlResults[$i++] += $count;
                    }
                }

                $buffer = \substr($buffer, $position);
            }
        }

        $i = 0;
        $urlOrderPosition = [];

        foreach ($urlOrders as $urls) {
            foreach ($urls as $url) {
                if (!isset($urlOrderPosition[$url])) {
                    $urlOrderPosition[$url] = $i++;
                }
            }
        }

        \uksort($results, fn ($a, $b) => $urlOrderPosition[$a] <=> $urlOrderPosition[$b]);

        return $results;
    }

    /** @param resource $parent */
    private function parseDataFromOffsetsInWorker(string $inputPath, array $offsets, $parent): void
    {
        $file = \fopen($inputPath, 'r');
        \stream_set_read_buffer($file, 0);
        \fseek($file, $offsets[0][0]);

        $daysToId = [];
        $period = new \DatePeriod(
            new \DateTime(self::POTENTIAL_FIRST_DAY), new \DateInterval('P1D'), new \DateTime()
        );
        foreach ($period as $i => $date) {
            $daysToId[\substr($date->format('y-m-d'), 1)] = \pack('v', $i);
        }

        $data = [];
        $shortUrlToFull = [];

        foreach ($offsets as $offset) {
            $bufferLength = $offset[1]; // length
            $buffer = \fread($file, $bufferLength);

            $position = 29;
            while ($position < $bufferLength) {
                $commaPosition = \strpos($buffer, ",", $position);

                $urlDays = &$data[\substr($buffer, $position, $commaPosition - $position)];
                if ($urlDays === null) {
                    $urlDays = '';
                    $shortUrlToFull[\substr($buffer, $position, $commaPosition - $position)] = \substr($buffer, $position - 4, $commaPosition - $position + 4);
                }

                $urlDays .= $daysToId[\substr($buffer, $commaPosition + 4, 7)];
                $position = $commaPosition + 56;
            }
        }

        $daysCount = \count($daysToId);
        $outputBuffer = '';

        foreach ($data as $shortUrl => $days) {
            $url = $shortUrlToFull[$shortUrl];

            $counts = \array_fill(0, $daysCount, 0);
            foreach (\array_count_values(\unpack('v*', $days)) as $day => $count) {
                $counts[$day] = $count;
            }

            $message = \pack('v', \strlen($url)) . $url . \pack('v*', ...$counts);
            $outputBuffer .= \pack('q', \strlen($message)) . $message;

            $writtenBytes = \fwrite($parent, $outputBuffer);
            $outputBuffer = \substr($outputBuffer, $writtenBytes);
        }

        $write = [$parent];
        $read = $except = [];
        while (\strlen($outputBuffer) > 0) {
            \stream_select($read, $write, $except, null);
            $writtenBytes = \fwrite($parent, $outputBuffer);
            $outputBuffer = \substr($outputBuffer, $writtenBytes);
        }

        \fclose($file);
        \fclose($parent);
    }
}