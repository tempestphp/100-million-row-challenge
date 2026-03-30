<?php

namespace App;

use RuntimeException;

final class Parser
{
    private const int PREFIX_LEN = 19; // https://stitcher.io
    private const int MULTI_CORE_THRESHOLD = 250 * 1024 * 1024; // 250MB
    private const int BUFFER_SIZE = 8 * 1024 * 1024;

    public function parse(string $inputPath, string $outputPath): void
    {
        $startTime = microtime(true);

        if (!is_readable($inputPath))
            throw new RuntimeException("Input file not readable: {$inputPath}");

        $fileSize = filesize($inputPath);
        echo "File size: {$fileSize} bytes\n";

        if ($fileSize < self::MULTI_CORE_THRESHOLD || !function_exists('pcntl_fork')) {
            echo "Mode: single-core\n";
            $data = $this->processChunk($inputPath, 0, $fileSize);
        } else {
            echo "Mode: multi-core\n";
            $data = $this->multiCore($inputPath, $fileSize);
        }

        $this->writeJson($data, $outputPath);

        echo "Total time: " . number_format(microtime(true) - $startTime, 3) . " sec\n";
        echo "Peak memory: " . round(memory_get_peak_usage(true) / 1024 / 1024, 2) . " MB\n";
    }

    private function multiCore(string $input, int $fileSize): array
    {
        $cores = max(1, (int)trim(shell_exec('nproc 2>/dev/null') ?: '1'));
        $chunkSize = intdiv($fileSize, $cores);
        $children = $sockets = [];

        for ($i = 0; $i < $cores; $i++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
            if (!$pair) throw new RuntimeException('Failed to create socket pair');
            [$parentSock, $childSock] = $pair;

            $start = $i * $chunkSize;
            $end = ($i === $cores - 1) ? $fileSize : $start + $chunkSize;
            $pid = pcntl_fork();

            if ($pid === 0) {
                fclose($parentSock);
                fwrite($childSock, serialize($this->processChunk($input, $start, $end)));
                exit(0);
            }
            fclose($childSock);
            $children[] = $pid;
            $sockets[] = $parentSock;
        }

        $final = [];
        foreach ($sockets as $sock) {
            $buffer = '';
            while (!feof($sock)) $buffer .= fread($sock, 65536);
            fclose($sock);
            foreach (unserialize($buffer) ?: [] as $path => $dates) {
                foreach ($dates as $date => $count) {
                    $final[$path][$date] = ($final[$path][$date] ?? 0) + $count;
                }
            }
        }

        foreach ($children as $pid) pcntl_waitpid($pid, $status);

        return $final;
    }

    private function processChunk(string $file, int $start, int $end): array
    {
        $handle = fopen($file, 'r');

        if ($start > 0) {
            fseek($handle, $start);
            fgets($handle);
        }

        $data = [];
        while (ftell($handle) < $end && !feof($handle)) {
            $buffer = fread($handle, self::BUFFER_SIZE);

            if ($buffer === '' || $buffer === false) break;

            if (ftell($handle) >= $end && ($extra = fgets($handle)) !== false) $buffer .= $extra;

            $offset = 0;
            $len = strlen($buffer);
            while ($offset < $len) {
                $newline = strpos($buffer, "\n", $offset);
                if ($newline === false) break;
                $comma = strpos($buffer, ',', $offset);
                if ($comma !== false && $comma < $newline) {
                    $path = substr($buffer, $offset + self::PREFIX_LEN, $comma - ($offset + self::PREFIX_LEN));
                    $date = substr($buffer, $comma + 1, 10);
                    $data[$path][$date] = ($data[$path][$date] ?? 0) + 1;
                }
                $offset = $newline + 1;
            }
        }

        fclose($handle);

        return $data;
    }

    private function writeJson(array $data, string $output): void
    {
        foreach ($data as &$dates) ksort($dates);

        file_put_contents($output, json_encode($data, JSON_PRETTY_PRINT));
    }
}
