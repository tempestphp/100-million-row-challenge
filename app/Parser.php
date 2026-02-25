<?php

namespace App;

use Exception;

final class Parser
{
    private const HOST_PREFIX_LEN = 19; // "https://stitcher.io"
    private const READ_CHUNK = 64 * 1024 * 1024; // 64MB

    public function parse(string $inputPath, string $outputPath, int $workers = 2): void
    {
        if (!\function_exists('pcntl_fork') || $workers <= 1) {
            $this->parseSingle($inputPath, $outputPath);
            return;
        }

        $size = filesize($inputPath);
        if ($size === false || $size === 0) {
            $this->writeResult([], [], $outputPath);
            return;
        }

        $readPipes = [];
        $writePipes = [];
        for ($i = 0; $i < $workers; $i++) {
            $pair = @stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            if ($pair === false) {
                throw new Exception('stream_socket_pair failed');
            }
            $readPipes[$i] = $pair[0];
            $writePipes[$i] = $pair[1];
        }

        $chunkSize = (int) ($size / $workers);
        $pids = [];
        for ($i = 0; $i < $workers; $i++) {
            $startByte = $i === 0 ? 0 : $chunkSize * $i;
            $endByte = $i === $workers - 1 ? $size : $chunkSize * ($i + 1);
            $pid = pcntl_fork();
            if ($pid === -1) {
                foreach ($readPipes as $r) {
                    fclose($r);
                }
                foreach ($writePipes as $w) {
                    fclose($w);
                }
                throw new Exception('pcntl_fork failed');
            }
            if ($pid === 0) {
                foreach ($readPipes as $r) {
                    @fclose($r);
                }
                $this->processChunk($startByte, $endByte, $inputPath, $writePipes[$i]);
                fclose($writePipes[$i]);
                exit(0);
            }
            fclose($writePipes[$i]);
            $pids[$i] = $pid;
        }

        $workerData = [];
        for ($i = 0; $i < $workers; $i++) {
            $raw = (string) stream_get_contents($readPipes[$i]);
            fclose($readPipes[$i]);
            $workerData[] = self::unpackPayload($raw);
        }
        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $pathOrder = $this->buildPathOrder($workerData);
        $merged = $this->mergeAggregates($pathOrder, $workerData);
        $this->writeResult($pathOrder, $merged, $outputPath);
    }

    private static function packPayload(array $payload): string
    {
        if (\extension_loaded('igbinary')) {
            return igbinary_serialize($payload);
        }
        if (\extension_loaded('msgpack')) {
            return msgpack_pack($payload);
        }
        return serialize($payload);
    }

    private static function unpackPayload(string $raw): array
    {
        $empty = ['order' => [], 'agg' => []];
        if ($raw === '') {
            return $empty;
        }
        if (\extension_loaded('igbinary')) {
            $v = igbinary_unserialize($raw);
            return \is_array($v) ? $v : $empty;
        }
        if (\extension_loaded('msgpack')) {
            $v = msgpack_unpack($raw);
            return \is_array($v) ? $v : $empty;
        }
        $v = unserialize($raw);
        return \is_array($v) ? $v : $empty;
    }

    private function buildPathOrder(array $workerData): array
    {
        $orders = array_column($workerData, 'order');
        if (count($orders) === 1) {
            $all = $orders[0];
        } else {
            $all = $this->mergeSortedByOffset($orders);
        }
        $seen = [];
        $pathOrder = [];
        foreach ($all as [$path]) {
            if (!isset($seen[$path])) {
                $seen[$path] = true;
                $pathOrder[] = $path;
            }
        }
        return $pathOrder;
    }

    /** @param array<int, array{0: string, 1: int}> $orders */
    private function mergeSortedByOffset(array $orders): array
    {
        $idx = array_fill(0, count($orders), 0);
        $result = [];
        $n = count($orders);
        while (true) {
            $minOffset = PHP_INT_MAX;
            $minK = -1;
            for ($k = 0; $k < $n; $k++) {
                $i = $idx[$k];
                if ($i < count($orders[$k]) && $orders[$k][$i][1] < $minOffset) {
                    $minOffset = $orders[$k][$i][1];
                    $minK = $k;
                }
            }
            if ($minK === -1) {
                break;
            }
            $result[] = $orders[$minK][$idx[$minK]];
            $idx[$minK]++;
        }
        return $result;
    }

    private function processChunk(int $startByte, int $endByte, string $inputPath, $writeStream): void
    {
        $handle = fopen($inputPath, 'r');
        if ($handle === false) {
            fwrite($writeStream, self::packPayload(['order' => [], 'agg' => []]));
            return;
        }
        stream_set_read_buffer($handle, self::READ_CHUNK);
        if ($startByte > 0) {
            fseek($handle, $startByte, SEEK_SET);
            while (true) {
                $block = fread($handle, 8192);
                if ($block === false || $block === '') {
                    break;
                }
                $nl = strpos($block, "\n");
                if ($nl !== false) {
                    fseek($handle, $startByte + $nl + 1, SEEK_SET);
                    $startByte = $startByte + $nl + 1;
                    break;
                }
                $startByte += strlen($block);
            }
            if ($startByte >= $endByte) {
                fclose($handle);
                fwrite($writeStream, self::packPayload(['order' => [], 'agg' => []]));
                return;
            }
        }
        $aggregate = [];
        $order = [];
        $seen = [];
        $lastPath = null;
        $lastRef = null;
        $filePos = $startByte;
        $remainder = '';
        while (true) {
            $chunk = fread($handle, self::READ_CHUNK);
            if ($chunk === false) {
                break;
            }
            $buf = $remainder . $chunk;
            $remainder = '';
            $len = strlen($buf);
            $chunkStart = $filePos;
            $start = 0;
            while ($start < $len) {
                $nl = strpos($buf, "\n", $start);
                if ($nl === false) {
                    $remainder = substr($buf, $start);
                    break;
                }
                $lineStart = $start;
                $lineLen = $nl - $start;
                $lineStartInFile = $chunkStart + $lineStart;
                $start = $nl + 1;
                if ($lineStartInFile >= $endByte) {
                    $remainder = substr($buf, $lineStart);
                    break 2;
                }
                if ($lineLen >= self::HOST_PREFIX_LEN + 12) {
                    $pathStart = $lineStart + self::HOST_PREFIX_LEN;
                    $commaPos = strpos($buf, ',', $pathStart);
                    if ($commaPos !== false && $commaPos < $lineStart + $lineLen) {
                        $path = substr($buf, $pathStart, $commaPos - $pathStart);
                        $d = substr($buf, $commaPos + 1, 10);
                        $dateInt = ((int) substr($d, 0, 4)) * 10000 + ((int) substr($d, 5, 2)) * 100 + (int) substr($d, 8, 2);
                        if ($path !== $lastPath) {
                            $lastPath = $path;
                            if (!isset($aggregate[$path])) {
                                $aggregate[$path] = [];
                            }
                            $lastRef = &$aggregate[$path];
                            if (!isset($seen[$path])) {
                                $seen[$path] = true;
                                $order[] = [$path, $lineStartInFile];
                            }
                        }
                        if (!isset($lastRef[$dateInt])) {
                            $lastRef[$dateInt] = 0;
                        }
                        $lastRef[$dateInt]++;
                    }
                }
            }
            $filePos += $len;
            if ($chunk === '') {
                break;
            }
        }
        unset($lastRef);
        fclose($handle);
        $aggOut = [];
        foreach ($aggregate as $path => $dates) {
            ksort($dates);
            $aggOut[$path] = [];
            foreach ($dates as $dk => $c) {
                $aggOut[$path][] = [$dk, $c];
            }
        }
        fwrite($writeStream, self::packPayload(['order' => $order, 'agg' => $aggOut]));
    }

    /**
     * @param array<int, array{0: int, 1: int}> $lists sorted by [0] (dateKey)
     * @return array<int, int> dateKey => count
     */
    private static function mergeSortedDateCounts(array $lists): array
    {
        $lists = array_values(array_filter($lists, fn ($l) => $l !== []));
        if ($lists === []) {
            return [];
        }
        $idx = array_fill(0, count($lists), 0);
        $result = [];
        $n = count($lists);
        while (true) {
            $minKey = null;
            for ($k = 0; $k < $n; $k++) {
                $i = $idx[$k];
                if ($i < count($lists[$k])) {
                    $key = $lists[$k][$i][0];
                    if ($minKey === null || $key < $minKey) {
                        $minKey = $key;
                    }
                }
            }
            if ($minKey === null) {
                break;
            }
            $sum = 0;
            for ($k = 0; $k < $n; $k++) {
                $i = $idx[$k];
                if ($i < count($lists[$k]) && $lists[$k][$i][0] === $minKey) {
                    $sum += $lists[$k][$i][1];
                    $idx[$k]++;
                }
            }
            $result[$minKey] = $sum;
        }
        return $result;
    }

    private function mergeAggregates(array $pathOrder, array $workerData): array
    {
        $merged = [];
        foreach ($pathOrder as $path) {
            $lists = [];
            foreach ($workerData as $w) {
                if (isset($w['agg'][$path])) {
                    $lists[] = $w['agg'][$path];
                }
            }
            $merged[$path] = self::mergeSortedDateCounts($lists);
        }
        return $merged;
    }

    private const WRITE_BUF_SIZE = 1024 * 1024; // 1MB flush

    private function writeResult(array $pathOrder, array $merged, string $outputPath): void
    {
        $out = fopen($outputPath, 'w');
        if ($out === false) {
            throw new Exception("Could not write output: {$outputPath}");
        }
        stream_set_write_buffer($out, 2 * 1024 * 1024);
        $pathsToWrite = array_filter($pathOrder, fn ($p) => isset($merged[$p]));
        $numPaths = count($pathsToWrite);
        $idx = 0;
        $buf = "{\n";
        foreach ($pathsToWrite as $path) {
            $dates = $merged[$path];
            $escapedPath = str_replace(['\\', '"', '/'], ['\\\\', '\\"', '\\/'], $path);
            $block = '    "' . $escapedPath . '": {' . "\n";
            $lines = [];
            foreach ($dates as $dateKey => $count) {
                $dateStr = \is_int($dateKey)
                    ? sprintf('%04d-%02d-%02d', (int) ($dateKey / 10000), (int) (($dateKey % 10000) / 100), $dateKey % 100)
                    : $dateKey;
                $lines[] = '        "' . $dateStr . '": ' . $count;
            }
            $block .= implode(",\n", $lines) . "\n";
            $idx++;
            $block .= '    }' . ($idx < $numPaths ? ",\n" : "\n");
            $buf .= $block;
            if (strlen($buf) >= self::WRITE_BUF_SIZE) {
                fwrite($out, $buf);
                $buf = '';
            }
        }
        fwrite($out, $buf . '}');
        fclose($out);
    }

    private function parseSingle(string $inputPath, string $outputPath): void
    {
        $handle = fopen($inputPath, 'r');
        if ($handle === false) {
            throw new Exception("Could not open input: {$inputPath}");
        }
        stream_set_read_buffer($handle, self::READ_CHUNK);
        $aggregate = [];
        $remainder = '';
        while (true) {
            $chunk = fread($handle, self::READ_CHUNK);
            if ($chunk === false) {
                break;
            }
            $buf = $remainder . $chunk;
            $remainder = '';
            $len = strlen($buf);
            $start = 0;
            while ($start < $len) {
                $nl = strpos($buf, "\n", $start);
                if ($nl === false) {
                    $remainder = substr($buf, $start);
                    break;
                }
                $lineStart = $start;
                $lineLen = $nl - $start;
                $start = $nl + 1;
                if ($lineLen < self::HOST_PREFIX_LEN + 12) {
                    continue;
                }
                $commaPos = strpos($buf, ',', $lineStart + self::HOST_PREFIX_LEN);
                if ($commaPos === false || $commaPos >= $lineStart + $lineLen) {
                    continue;
                }
                $path = substr($buf, $lineStart + self::HOST_PREFIX_LEN, $commaPos - $lineStart - self::HOST_PREFIX_LEN);
                $date = substr($buf, $commaPos + 1, 10);
                $aggregate[$path][$date] = ($aggregate[$path][$date] ?? 0) + 1;
            }
            if ($chunk === '') {
                break;
            }
        }
        if ($remainder !== '' && strlen($remainder) >= self::HOST_PREFIX_LEN + 12) {
            $commaPos = strpos($remainder, ',', self::HOST_PREFIX_LEN);
            if ($commaPos !== false) {
                $path = substr($remainder, self::HOST_PREFIX_LEN, $commaPos - self::HOST_PREFIX_LEN);
                $date = substr($remainder, $commaPos + 1, 10);
                $aggregate[$path][$date] = ($aggregate[$path][$date] ?? 0) + 1;
            }
        }
        fclose($handle);
        foreach ($aggregate as $path => $dates) {
            ksort($aggregate[$path]);
        }
        $this->writeResult(array_keys($aggregate), $aggregate, $outputPath);
    }
}
