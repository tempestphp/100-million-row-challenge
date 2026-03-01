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
use function str_ends_with;
use function substr;

final class Parser
{
    private const int DEFAULT_CHUNK_SIZE = 128 * 1024 * 1024;
    private const int TIMESTAMP_LENGTH = 25;
    private const int HOST_PREFIX_LENGTH = 19;
    private const int MIN_LINE_LENGTH = self::HOST_PREFIX_LENGTH + 1 + self::TIMESTAMP_LENGTH;

    private const int DEFAULT_WORKERS = 12;
    private const int MAX_WORKERS = 32;
    private const int MIN_BYTES_PER_WORKER = 128 * 1024 * 1024;

    private const string SERIALIZER = 'igbinary';
    private const int SOCKET_HEADER_LENGTH = 20;
    private const int PROFILE_SAMPLE_MASK = 127;

    private int $chunkSize = self::DEFAULT_CHUNK_SIZE;
    private string $serializer = self::SERIALIZER;
    private bool $profileEnabled = false;

    /**
     * @var array<string, int>
     */
    private array $profileDurations = [];

    /**
     * @var array<string, int>
     */
    private array $profileCounters = [];

    /**
     * @var array<string, int>
     */
    private array $workerTimeTotalsNs = [];

    /**
     * @var array<string, int>
     */
    private array $workerTimeMaxNs = [];

    private int $workerProfileCount = 0;

    public function parse(string $inputPath, string $outputPath): void
    {
        $this->profileEnabled = getenv('PARSER_PROFILE') === '1';
        $this->profileDurations = [];
        $this->profileCounters = [];
        $this->workerTimeTotalsNs = [];
        $this->workerTimeMaxNs = [];
        $this->workerProfileCount = 0;

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

        $parallelStart = $this->profileStart();
        $spawnStart = $this->profileStart();

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

                $workerProfile = null;

                if ($this->profileEnabled) {
                    $workerProfile = [];
                    $workerProfile['worker_total_start_ns'] = hrtime(true);
                }

                $visits = $this->parseSegment($inputPath, $start, $end, $workerProfile);

                if (! is_array($visits)) {
                    fclose($childSocket);
                    exit(1);
                }

                if (is_array($workerProfile)) {
                    $workerProfile['worker_total_ns'] = hrtime(true) - $workerProfile['worker_total_start_ns'];
                    unset($workerProfile['worker_total_start_ns']);
                }

                $payloadData = $workerProfile === null
                    ? $visits
                    : [
                        'visits' => $visits,
                        'worker_profile' => $workerProfile,
                    ];

                $payload = $this->encodePayload($payloadData);
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

        $this->profileStop('parallel_spawn', $spawnStart);
        $this->profileCounter('parallel_workers', count($workerSockets));

        $merged = [];
        $receiveStart = $this->profileStart();

        ksort($workerSockets);

        foreach ($workerSockets as $worker => $socket) {
            $socketReadStart = $this->profileStart();
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
            $this->profileStop('parallel_socket_read', $socketReadStart);

            if (! is_string($payload)) {
                foreach (array_keys($pids) as $pid) {
                    pcntl_waitpid($pid, $status);
                }

                throw new RuntimeException('Unable to read payload body from worker process');
            }

            $this->profileCounter('parallel_payload_count', 1);
            $this->profileCounter('parallel_payload_bytes', strlen($payload));

            $decodeStart = $this->profileStart();
            $decodedPayload = $this->decodePayload($payload);
            $this->profileStop('parallel_decode', $decodeStart);

            if (! is_array($decodedPayload)) {
                foreach (array_keys($pids) as $pid) {
                    pcntl_waitpid($pid, $status);
                }

                throw new RuntimeException('Invalid worker payload');
            }

            $workerData = $decodedPayload;

            if (
                isset($decodedPayload['visits'], $decodedPayload['worker_profile'])
                && is_array($decodedPayload['visits'])
                && is_array($decodedPayload['worker_profile'])
            ) {
                $workerData = $decodedPayload['visits'];
                $this->ingestWorkerProfile($decodedPayload['worker_profile']);
            }

            $mergeStart = $this->profileStart();
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

            $this->profileStop('parallel_merge', $mergeStart);
        }

        $this->profileStop('parallel_receive', $receiveStart);

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

    /**
     * @return array<string, array<string, int>>|null
     */
    private function parseSegment(string $inputPath, int $start, int $end, ?array &$workerProfile = null): ?array
    {
        $segmentSize = $end - $start;

        if ($segmentSize <= 0) {
            return [];
        }

        $openSeekStart = $workerProfile !== null ? hrtime(true) : 0;

        $handle = fopen($inputPath, 'rb');

        if (! is_resource($handle)) {
            return null;
        }

        stream_set_read_buffer($handle, $this->chunkSize);

        if (fseek($handle, $start) !== 0) {
            fclose($handle);
            return null;
        }

        if ($workerProfile !== null && $openSeekStart !== 0) {
            $workerProfile['worker_open_seek_ns'] = hrtime(true) - $openSeekStart;
        }

        $visits = $this->parseSegmentWithFgets($handle, $segmentSize, $workerProfile);

        fclose($handle);

        return $visits;
    }

    /**
     * @param resource $handle
     * @return array<string, array<string, int>>
     */
    private function parseSegmentWithFgets(mixed $handle, int $segmentSize, ?array &$workerProfile = null): array
    {
        $visits = [];
        $remainingBytes = $segmentSize;
        $profilingWorker = $workerProfile !== null;

        $sampledFgetsNs = 0;
        $sampledParseNs = 0;
        $lineIndex = 0;

        $parseLoopStart = $profilingWorker ? hrtime(true) : 0;

        while ($remainingBytes > 0) {
            if ($profilingWorker) {
                $lineIndex++;
            }

            $sampleLine = $profilingWorker && (($lineIndex & self::PROFILE_SAMPLE_MASK) === 0);
            $fgetsStart = $sampleLine ? hrtime(true) : 0;

            $line = fgets($handle);

            if ($sampleLine && $fgetsStart !== 0) {
                $sampledFgetsNs += hrtime(true) - $fgetsStart;
            }

            if ($line === false) {
                break;
            }

            $parseLineStart = $sampleLine ? hrtime(true) : 0;

            $lineByteLength = strlen($line);

            if ($lineByteLength === 0) {
                if ($sampleLine && $parseLineStart !== 0) {
                    $sampledParseNs += hrtime(true) - $parseLineStart;
                }

                continue;
            }

            $remainingBytes -= $lineByteLength;

            $lineLength = $lineByteLength - 1;

            if ($lineLength < self::MIN_LINE_LENGTH) {
                if ($sampleLine && $parseLineStart !== 0) {
                    $sampledParseNs += hrtime(true) - $parseLineStart;
                }

                continue;
            }

            if ($line[$lineLength - 1] === "\r") {
                $lineLength--;
            }

            $timestampStart = $lineLength - self::TIMESTAMP_LENGTH;
            $commaPos = $timestampStart - 1;
            $pathLength = $commaPos - self::HOST_PREFIX_LENGTH;

            if ($pathLength <= 0) {
                if ($sampleLine && $parseLineStart !== 0) {
                    $sampledParseNs += hrtime(true) - $parseLineStart;
                }

                continue;
            }

            $path = substr($line, self::HOST_PREFIX_LENGTH, $pathLength);
            $date = substr($line, $timestampStart, 10);

            if (isset($visits[$path][$date])) {
                $visits[$path][$date]++;

                if ($sampleLine && $parseLineStart !== 0) {
                    $sampledParseNs += hrtime(true) - $parseLineStart;
                }

                continue;
            }

            $visits[$path][$date] = 1;

            if ($sampleLine && $parseLineStart !== 0) {
                $sampledParseNs += hrtime(true) - $parseLineStart;
            }
        }

        if ($profilingWorker && $parseLoopStart !== 0) {
            $parseLoopNs = hrtime(true) - $parseLoopStart;
            $workerProfile['worker_parse_loop_ns'] = $parseLoopNs;

            $sampledTotalNs = $sampledFgetsNs + $sampledParseNs;

            if ($sampledTotalNs > 0) {
                $fgetsShareNs = (int) (($parseLoopNs * $sampledFgetsNs) / $sampledTotalNs);
                $workerProfile['worker_fgets_share_ns'] = $fgetsShareNs;
                $workerProfile['worker_line_parse_share_ns'] = $parseLoopNs - $fgetsShareNs;
            }
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
     * @param array<mixed> $data
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

    private function profileCounter(string $name, int $value): void
    {
        if (! $this->profileEnabled) {
            return;
        }

        $this->profileCounters[$name] = ($this->profileCounters[$name] ?? 0) + $value;
    }

    /**
     * @param array<string, int> $workerProfile
     */
    private function ingestWorkerProfile(array $workerProfile): void
    {
        if (! $this->profileEnabled) {
            return;
        }

        $this->workerProfileCount++;

        foreach ($workerProfile as $name => $value) {
            if (! is_int($value) || ! str_ends_with($name, '_ns')) {
                continue;
            }

            $this->workerTimeTotalsNs[$name] = ($this->workerTimeTotalsNs[$name] ?? 0) + $value;
            $currentMax = $this->workerTimeMaxNs[$name] ?? 0;

            if ($value > $currentMax) {
                $this->workerTimeMaxNs[$name] = $value;
            }
        }
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

        foreach ($this->profileCounters as $name => $value) {
            fwrite(STDERR, sprintf("profile %s=%d\n", $name, $value));
        }

        if ($this->workerProfileCount === 0) {
            return;
        }

        fwrite(STDERR, sprintf("profile worker_count=%d\n", $this->workerProfileCount));

        ksort($this->workerTimeTotalsNs);

        foreach ($this->workerTimeTotalsNs as $metric => $totalNs) {
            $avgSeconds = ($totalNs / $this->workerProfileCount) / 1_000_000_000;
            $maxSeconds = ($this->workerTimeMaxNs[$metric] ?? 0) / 1_000_000_000;
            $metricName = substr($metric, 0, strlen($metric) - 3);

            fwrite(STDERR, sprintf("profile %s_avg_sec=%.6f\n", $metricName, $avgSeconds));
            fwrite(STDERR, sprintf("profile %s_max_sec=%.6f\n", $metricName, $maxSeconds));
        }
    }

}
