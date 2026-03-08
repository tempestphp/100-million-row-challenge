<?php

namespace App;

use App\Commands\Visit;
use RuntimeException;
use function array_keys;
use function array_unique;
use function array_values;
use function count;
use function fclose;
use function fgets;
use function file_put_contents;
use function filesize;
use function fopen;
use function fseek;
use function ftell;
use function fwrite;
use function igbinary_serialize;
use function igbinary_unserialize;
use function intdiv;
use function is_array;
use function is_int;
use function is_resource;
use function is_string;
use function json_encode;
use function ksort;
use function pcntl_fork;
use function pcntl_waitpid;
use function pcntl_wexitstatus;
use function pcntl_wifexited;
use function sort;
use function sprintf;
use function stream_socket_pair;
use function stream_set_read_buffer;
use function strlen;
use function substr;

final class Parser
{
    private const int CHUNK_SIZE = 256 * 1024 * 1024;
    private const int TIMESTAMP_LENGTH = 25;
    private const int HOST_PREFIX_LENGTH = 19;
    private const int DEFAULT_WORKERS = 12;
    private const int SOCKET_HEADER_LENGTH = 20;

    /**
     * @var array<string, int>
     */
    private array $pathIdByPath = [];

    /**
     * @var array<int, string>
     */
    private array $pathById = [];

    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);

        if (! is_int($fileSize)) {
            throw new RuntimeException("Unable to read input file size: {$inputPath}");
        }

        [$this->pathIdByPath, $this->pathById] = $this->buildPathDictionaries();

        $visitsByPathId = $this->parseParallel($inputPath, $fileSize);
        $visits = $this->materializePathVisits($visitsByPathId);

        foreach ($visits as &$pathVisits) {
            ksort($pathVisits);

            $normalizedDates = [];

            foreach ($pathVisits as $dateInt => $count) {
                $dateValue = (int) $dateInt;
                $year = intdiv($dateValue, 10000);
                $month = intdiv($dateValue % 10000, 100);
                $day = $dateValue % 100;

                $normalizedDates[sprintf('%04d-%02d-%02d', $year, $month, $day)] = $count;
            }

            $pathVisits = $normalizedDates;
        }

        unset($pathVisits);

        $json = json_encode($visits, JSON_PRETTY_PRINT);

        if (! is_string($json)) {
            throw new RuntimeException('Failed to encode output JSON');
        }

        if (file_put_contents($outputPath, $json) === false) {
            throw new RuntimeException("Unable to write output file: {$outputPath}");
        }
    }

    /**
     * @return array{0: array<string, int>, 1: array<int, string>}
     */
    private function buildPathDictionaries(): array
    {
        $pathIdByPath = [];
        $pathById = [];
        $nextPathId = 0;

        foreach (Visit::all() as $visit) {
            $path = substr($visit->uri, self::HOST_PREFIX_LENGTH);

            if ($path === '' || isset($pathIdByPath[$path])) {
                continue;
            }

            $pathIdByPath[$path] = $nextPathId;
            $pathById[$nextPathId] = $path;
            $nextPathId++;
        }

        return [$pathIdByPath, $pathById];
    }

    /**
     * @param array<int, array<int, int>> $visitsByPathId
     * @return array<string, array<int, int>>
     */
    private function materializePathVisits(array $visitsByPathId): array
    {
        $visits = [];

        foreach ($visitsByPathId as $pathId => $pathVisits) {
            if (! isset($this->pathById[$pathId])) {
                continue;
            }

            $visits[$this->pathById[$pathId]] = $pathVisits;
        }

        return $visits;
    }

    /**
     * @return array<int, array<int, int>>
     */
    private function parseParallel(string $inputPath, int $fileSize): array
    {
        $segments = $this->buildSegments($inputPath, $fileSize);

        if ($segments === []) {
            return [];
        }

        $workerSockets = [];
        $pids = [];

        foreach ($segments as $worker => $segment) {
            [$start, $end] = $segment;

            $socketPair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);

            if (! is_array($socketPair) || count($socketPair) !== 2) {
                foreach ($workerSockets as $socket) {
                    fclose($socket);
                }

                foreach (array_keys($pids) as $existingPid) {
                    pcntl_waitpid($existingPid, $status);
                }

                throw new RuntimeException('Unable to create socket pair for worker process');
            }

            [$parentSocket, $childSocket] = $socketPair;
            $pid = pcntl_fork();

            if ($pid === -1) {
                fclose($parentSocket);
                fclose($childSocket);

                foreach ($workerSockets as $socket) {
                    fclose($socket);
                }

                foreach (array_keys($pids) as $existingPid) {
                    pcntl_waitpid($existingPid, $status);
                }

                throw new RuntimeException('Unable to fork worker process');
            }

            if ($pid === 0) {
                fclose($parentSocket);

                $workerResult = $this->parseSegment($inputPath, $start, $end);

                if (! is_array($workerResult)) {
                    fclose($childSocket);
                    exit(1);
                }

                $payload = igbinary_serialize($workerResult);
                $lengthHeader = sprintf('%020d', strlen($payload));

                if (! $this->writeToSocket($childSocket, $lengthHeader . $payload)) {
                    fclose($childSocket);
                    exit(1);
                }

                fclose($childSocket);
                exit(0);
            }

            fclose($childSocket);
            $workerSockets[$worker] = $parentSocket;
            $pids[$pid] = $worker;
        }

        $merged = [];
        ksort($workerSockets);

        foreach ($workerSockets as $worker => $socket) {
            $header = $this->readFromSocket($socket, self::SOCKET_HEADER_LENGTH);

            if (! is_string($header)) {
                fclose($socket);

                foreach ($workerSockets as $index => $otherSocket) {
                    if ($index !== $worker) {
                        fclose($otherSocket);
                    }
                }

                foreach (array_keys($pids) as $pid) {
                    pcntl_waitpid($pid, $status);
                }

                throw new RuntimeException('Unable to read payload header from worker process');
            }

            $payloadLength = (int) $header;

            if ($payloadLength < 0) {
                fclose($socket);

                foreach (array_keys($pids) as $pid) {
                    pcntl_waitpid($pid, $status);
                }

                throw new RuntimeException('Invalid payload length from worker process');
            }

            $payload = $this->readFromSocket($socket, $payloadLength);
            fclose($socket);

            if (! is_string($payload)) {
                foreach (array_keys($pids) as $pid) {
                    pcntl_waitpid($pid, $status);
                }

                throw new RuntimeException('Unable to read payload body from worker process');
            }

            $workerData = igbinary_unserialize($payload);

            if (! is_array($workerData)) {
                foreach (array_keys($pids) as $pid) {
                    pcntl_waitpid($pid, $status);
                }

                throw new RuntimeException('Invalid worker payload');
            }

            foreach ($workerData as $pathId => $pathVisits) {
                $pathId = (int) $pathId;

                if (! isset($merged[$pathId])) {
                    $merged[$pathId] = $pathVisits;
                    continue;
                }

                $mergedPathVisits = &$merged[$pathId];

                foreach ($pathVisits as $dateInt => $count) {
                    if (! isset($mergedPathVisits[$dateInt])) {
                        $mergedPathVisits[$dateInt] = $count;
                        continue;
                    }

                    $mergedPathVisits[$dateInt] += $count;
                }

                unset($mergedPathVisits);
            }
        }

        $failed = false;

        foreach (array_keys($pids) as $pid) {
            pcntl_waitpid($pid, $status);

            if (! pcntl_wifexited($status) || pcntl_wexitstatus($status) !== 0) {
                $failed = true;
            }
        }

        if ($failed) {
            throw new RuntimeException('One or more worker processes failed');
        }

        return $merged;
    }

    /**
     * @return array<int, array<int, int>>|null
     */
    private function parseSegment(string $inputPath, int $start, int $end): ?array
    {
        $segmentSize = $end - $start;

        if ($segmentSize <= 0) {
            return [];
        }

        $handle = fopen($inputPath, 'rb');

        if (! is_resource($handle)) {
            return null;
        }

        stream_set_read_buffer($handle, self::CHUNK_SIZE);

        if (fseek($handle, $start) !== 0) {
            fclose($handle);
            return null;
        }

        $visits = $this->parseSegmentWithFgets($handle, $segmentSize);
        fclose($handle);

        return $visits;
    }

    /**
     * @param resource $handle
     * @return array<int, array<int, int>>
     */
    private function parseSegmentWithFgets(mixed $handle, int $segmentSize): array
    {
        $visits = [];
        $remainingBytes = $segmentSize;

        while ($remainingBytes > 0) {
            $line = fgets($handle);

            if ($line === false) {
                break;
            }

            $lineByteLength = strlen($line);
            $remainingBytes -= $lineByteLength;

            $lineLength = $lineByteLength - 1;
            $timestampStart = $lineLength - self::TIMESTAMP_LENGTH;
            $commaPos = $timestampStart - 1;
            $pathLength = $commaPos - self::HOST_PREFIX_LENGTH;

            $path = substr($line, self::HOST_PREFIX_LENGTH, $pathLength);
            $dateInt = (int) (
                substr($line, $timestampStart, 4)
                . substr($line, $timestampStart + 5, 2)
                . substr($line, $timestampStart + 8, 2)
            );

            $pathId = $this->pathIdByPath[$path];

            if (! isset($visits[$pathId])) {
                $visits[$pathId] = [$dateInt => 1];
                continue;
            }

            $pathBucket = &$visits[$pathId];

            if (isset($pathBucket[$dateInt])) {
                $pathBucket[$dateInt]++;
                continue;
            }

            $pathBucket[$dateInt] = 1;
        }

        return $visits;
    }

    /**
     * @return list<array{0: int, 1: int}>
     */
    private function buildSegments(string $inputPath, int $fileSize): array
    {
        if ($fileSize <= 0) {
            return [];
        }

        $handle = fopen($inputPath, 'rb');

        if (! is_resource($handle)) {
            return [];
        }

        $offsets = [0];

        for ($worker = 1; $worker < self::DEFAULT_WORKERS; $worker++) {
            $target = intdiv($fileSize * $worker, self::DEFAULT_WORKERS);

            if (fseek($handle, $target) !== 0) {
                continue;
            }

            if ($target > 0) {
                fgets($handle);
            }

            $offset = ftell($handle);

            if (is_int($offset) && $offset > 0 && $offset < $fileSize) {
                $offsets[] = $offset;
            }
        }

        fclose($handle);

        $offsets[] = $fileSize;

        sort($offsets);
        $offsets = array_values(array_unique($offsets, SORT_NUMERIC));

        $segments = [];
        $offsetCount = count($offsets);

        for ($i = 0; $i < $offsetCount - 1; $i++) {
            $start = $offsets[$i];
            $end = $offsets[$i + 1];

            if ($end > $start) {
                $segments[] = [$start, $end];
            }
        }

        return $segments;
    }

    /**
     * @param resource $socket
     */
    private function writeToSocket(mixed $socket, string $payload): bool
    {
        $payloadLength = strlen($payload);
        $written = 0;

        while ($written < $payloadLength) {
            $bytes = fwrite($socket, substr($payload, $written));

            if ($bytes === false || $bytes === 0) {
                return false;
            }

            $written += $bytes;
        }

        return true;
    }

    /**
     * @param resource $socket
     */
    private function readFromSocket(mixed $socket, int $length): ?string
    {
        if ($length === 0) {
            return '';
        }

        $buffer = '';

        while (strlen($buffer) < $length) {
            $chunk = fread($socket, $length - strlen($buffer));

            if ($chunk === false || $chunk === '') {
                return null;
            }

            $buffer .= $chunk;
        }

        return $buffer;
    }
}
