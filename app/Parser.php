<?php

namespace App;

use RuntimeException;
use function array_keys;
use function array_unique;
use function array_values;
use function call_user_func;
use function count;
use function fclose;
use function fgets;
use function file_put_contents;
use function filesize;
use function fopen;
use function fseek;
use function ftell;
use function fread;
use function fwrite;
use function getenv;
use function hrtime;
use function intdiv;
use function is_array;
use function is_int;
use function is_resource;
use function is_string;
use function json_encode;
use function ksort;
use function max;
use function min;
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
    private const int DEFAULT_CHUNK_SIZE = 128 * 1024 * 1024;
    private const int TIMESTAMP_LENGTH = 25;
    private const int HOST_PREFIX_LENGTH = 19;
    private const int MIN_LINE_LENGTH = self::HOST_PREFIX_LENGTH + 1 + self::TIMESTAMP_LENGTH;

    private const int FORK_MIN_FILE_SIZE = 512 * 1024 * 1024;
    private const int DEFAULT_WORKERS = 12;
    private const int MAX_WORKERS = 32;
    private const int MIN_BYTES_PER_WORKER = 128 * 1024 * 1024;

    private const string SERIALIZER = 'igbinary';
    private const int SOCKET_HEADER_LENGTH = 20;

    private int $chunkSize = self::DEFAULT_CHUNK_SIZE;
    private string $serializer = self::SERIALIZER;
    private bool $profileEnabled = false;

    /**
     * @var array<string, int>
     */
    private array $profileDurations = [];

    public function parse(string $inputPath, string $outputPath): void
    {
        $this->profileEnabled = getenv('PARSER_PROFILE') === '1';
        $this->profileDurations = [];

        $fileSize = filesize($inputPath);

        if (! is_int($fileSize)) {
            throw new RuntimeException("Unable to read input file size: {$inputPath}");
        }

        $this->chunkSize = $this->determineChunkSize();

        $totalStart = $this->profileStart();

        $parseStart = $this->profileStart();
        $visits = $this->parseParallel($inputPath, $fileSize);
        $this->profileStop('parse_phase', $parseStart);

        $sortStart = $this->profileStart();
        foreach ($visits as &$pathVisits) {
            ksort($pathVisits);
        }
        unset($pathVisits);
        $this->profileStop('sort_phase', $sortStart);

        $encodeStart = $this->profileStart();
        $json = json_encode($visits, JSON_PRETTY_PRINT);
        $this->profileStop('json_encode_phase', $encodeStart);

        if (! is_string($json)) {
            throw new RuntimeException('Failed to encode output JSON');
        }

        $writeStart = $this->profileStart();
        if (file_put_contents($outputPath, $json) === false) {
            throw new RuntimeException("Unable to write output file: {$outputPath}");
        }
        $this->profileStop('write_phase', $writeStart);

        $this->profileStop('total', $totalStart);
        $this->printProfile($fileSize, count($visits));
    }

    /**
     * @return array<string, array<string, int>>
     */
    private function parseParallel(string $inputPath, int $fileSize): array
    {
        $workers = $this->determineWorkerCount($fileSize);

        $segmentBuildStart = $this->profileStart();
        $segments = $this->buildSegments($inputPath, $fileSize, $workers);
        $this->profileStop('parallel_segment_build', $segmentBuildStart);

        if ($segments === []) {
            return [];
        }

        if (! $this->shouldFork($fileSize, count($segments))) {
            return $this->parseSegmentOrFail($inputPath, 0, $fileSize);
        }

        $parallelStart = $this->profileStart();

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

                $visits = $this->parseSegment($inputPath, $start, $end);

                if (! is_array($visits)) {
                    fclose($childSocket);
                    exit(1);
                }

                $payload = $this->encodePayload($visits);
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

            $workerData = $this->decodePayload($payload);

            if (! is_array($workerData)) {
                foreach (array_keys($pids) as $pid) {
                    pcntl_waitpid($pid, $status);
                }

                throw new RuntimeException('Invalid worker payload');
            }

            foreach ($workerData as $path => $pathVisits) {
                if (! isset($merged[$path])) {
                    $merged[$path] = $pathVisits;
                    continue;
                }

                $mergedPathVisits = &$merged[$path];

                foreach ($pathVisits as $date => $count) {
                    if (! isset($mergedPathVisits[$date])) {
                        $mergedPathVisits[$date] = $count;
                        continue;
                    }

                    $mergedPathVisits[$date] += $count;
                }

                unset($mergedPathVisits);
            }
        }

        $waitStart = $this->profileStart();
        $failed = false;

        foreach (array_keys($pids) as $pid) {
            pcntl_waitpid($pid, $status);

            if (! pcntl_wifexited($status) || pcntl_wexitstatus($status) !== 0) {
                $failed = true;
            }
        }

        $this->profileStop('parallel_wait', $waitStart);

        if ($failed) {
            throw new RuntimeException('One or more worker processes failed');
        }

        $this->profileStop('parallel_total', $parallelStart);

        return $merged;
    }

    private function shouldFork(int $fileSize, int $segmentCount): bool
    {
        if ($fileSize < self::FORK_MIN_FILE_SIZE) {
            return false;
        }

        if ($segmentCount < 2) {
            return false;
        }

        return true;
    }

    /**
     * @return array<string, array<string, int>>
     */
    private function parseSegmentOrFail(string $inputPath, int $start, int $end): array
    {
        $visits = $this->parseSegment($inputPath, $start, $end);

        if (! is_array($visits)) {
            throw new RuntimeException('Unable to parse input segment');
        }

        return $visits;
    }

    /**
     * @return array<string, array<string, int>>|null
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

        stream_set_read_buffer($handle, $this->chunkSize);

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
     * @return array<string, array<string, int>>
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

            if ($lineByteLength === 0) {
                continue;
            }

            $remainingBytes -= $lineByteLength;

            $lineLength = $lineByteLength - 1;

            if ($lineLength < self::MIN_LINE_LENGTH) {
                continue;
            }

            if ($line[$lineLength - 1] === "\r") {
                $lineLength--;
            }

            $timestampStart = $lineLength - self::TIMESTAMP_LENGTH;
            $commaPos = $timestampStart - 1;
            $pathLength = $commaPos - self::HOST_PREFIX_LENGTH;

            if ($pathLength <= 0) {
                continue;
            }

            $path = substr($line, self::HOST_PREFIX_LENGTH, $pathLength);
            $date = substr($line, $timestampStart, 10);

            if (isset($visits[$path][$date])) {
                $visits[$path][$date]++;
                continue;
            }

            $visits[$path][$date] = 1;
        }

        return $visits;
    }

    /**
     * @return list<array{0: int, 1: int}>
     */
    private function buildSegments(string $inputPath, int $fileSize, int $workers): array
    {
        if ($fileSize <= 0) {
            return [];
        }

        $handle = fopen($inputPath, 'rb');

        if (! is_resource($handle)) {
            return [];
        }

        $offsets = [0];

        for ($worker = 1; $worker < $workers; $worker++) {
            $target = intdiv($fileSize * $worker, $workers);

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

    private function determineWorkerCount(int $fileSize): int
    {
        $configured = getenv('PARSER_WORKERS');
        $workers = self::DEFAULT_WORKERS;

        if (is_string($configured) && $configured !== '') {
            $configuredWorkers = (int) $configured;

            if ($configuredWorkers > 0) {
                $workers = $configuredWorkers;
            }
        }

        $maxBySize = max(1, intdiv($fileSize, self::MIN_BYTES_PER_WORKER));

        return max(1, min(self::MAX_WORKERS, $workers, $maxBySize));
    }

    private function determineChunkSize(): int
    {
        $configured = getenv('PARSER_CHUNK_MB');

        if (! is_string($configured) || $configured === '') {
            return self::DEFAULT_CHUNK_SIZE;
        }

        $megabytes = (int) $configured;

        if ($megabytes < 1 || $megabytes > 512) {
            return self::DEFAULT_CHUNK_SIZE;
        }

        return $megabytes * 1024 * 1024;
    }

    /**
     * @param array<string, array<string, int>> $data
     */
    private function encodePayload(array $data): string
    {
        return (string) call_user_func('igbinary_serialize', $data);
    }

    /**
     * @return array<string, array<string, int>>|null
     */
    private function decodePayload(string $payload): ?array
    {
        $decoded = call_user_func('igbinary_unserialize', $payload);

        return is_array($decoded) ? $decoded : null;
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

    private function profileStart(): int
    {
        if (! $this->profileEnabled) {
            return 0;
        }

        return hrtime(true);
    }

    private function profileStop(string $phase, int $start): void
    {
        if (! $this->profileEnabled || $start === 0) {
            return;
        }

        $this->profileDurations[$phase] = ($this->profileDurations[$phase] ?? 0) + (hrtime(true) - $start);
    }

    private function printProfile(int $fileSize, int $paths): void
    {
        if (! $this->profileEnabled) {
            return;
        }

        fwrite(STDERR, sprintf(
            "profile file_size=%d paths=%d chunk_mb=%d serializer=%s\n",
            $fileSize,
            $paths,
            intdiv($this->chunkSize, 1024 * 1024),
            $this->serializer,
        ));

        foreach ($this->profileDurations as $phase => $duration) {
            fwrite(STDERR, sprintf("profile %s=%.6f\n", $phase, $duration / 1_000_000_000));
        }
    }

}
