<?php

declare(strict_types=1);

namespace App;

use Exception;

use function array_count_values;
use function explode;
use function fgetc;
use function fgets;
use function file_get_contents;
use function file_put_contents;
use function filesize;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function gc_disable;
use function igbinary_serialize;
use function igbinary_unserialize;
use function json_encode;
use function ksort;
use function pcntl_fork;
use function pcntl_waitpid;
use function strlen;
use function strrpos;
use function substr;
use function substr_replace;
use function sys_get_temp_dir;
use function unlink;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $fileSize = filesize($inputPath);
        $workers = 4;
        $chunkSize = (int)($fileSize / $workers);
        $pids = [];

        for ($i = 0; $i < $workers; $i++) {
            $start = $i * $chunkSize;
            $end = ($i === $workers - 1) ? $fileSize : $start + $chunkSize;

            $pid = pcntl_fork();

            if ($pid === -1) {
                throw new Exception("Fork failed");
            } elseif ($pid === 0) {
                $this->processChunk($inputPath, $start, $end, $i);
                exit(0);
            } else {
                $pids[] = $pid;
            }
        }

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $this->mergeAndOutput($workers, $outputPath);
    }

    private function processChunk(string $file, int $start, int $end, int $workerId): void
    {
        $fp = fopen($file, 'r');
        if (!$fp) exit(1);

        fseek($fp, $start);

        if ($start > 0) {
            fgets($fp);
        }

        $workerData = [];
        $currentBytesRead = ftell($fp);
        $leftover = '';

        while ($currentBytesRead < $end) {
            $readSize = $end - $currentBytesRead;
            if ($readSize > 4194304) $readSize = 4194304;

            $chunk = fread($fp, $readSize);
            $currentBytesRead += strlen($chunk);

            if ($currentBytesRead >= $end) {
                while (($char = fgetc($fp)) !== false) {
                    $chunk .= $char;
                    if ($char === "\n") break;
                }
            }

            $chunk = $leftover . $chunk;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) {
                $leftover = $chunk;
                continue;
            }

            $processChunk = substr($chunk, 0, $lastNl);
            $leftover = substr($chunk, $lastNl + 1);
            unset($chunk);

            $lines = explode("\n", $processChunk);
            unset($processChunk);

            $lines = substr_replace($lines, '', -15);

            $counts = array_count_values($lines);
            unset($lines);

            foreach ($counts as $key => $count) {
                $workerData[$key] = ($workerData[$key] ?? 0) + $count;
            }
        }

        fclose($fp);

        $tmpFile = sys_get_temp_dir() . "/w_{$workerId}.dat";
        $serialized = igbinary_serialize($workerData);

        file_put_contents($tmpFile, $serialized);
    }

    private function mergeAndOutput(int $workers, string $outputPath): void
    {
        $finalData = [];
        $aggregatedData = [];

        for ($i = 0; $i < $workers; $i++) {
            $file = sys_get_temp_dir() . "/w_{$i}.dat";

            $payload = file_get_contents($file);
            $data = igbinary_unserialize($payload);

            unlink($file);

            foreach ($data as $key => $count) {
                $aggregatedData[$key] = ($aggregatedData[$key] ?? 0) + $count;
            }
            unset($data, $payload);
        }

        foreach ($aggregatedData as $rawKey => $count) {
            $date = substr($rawKey, -10);
            $path = substr($rawKey, 19, -11);
            $finalData[$path][$date] = ($finalData[$path][$date] ?? 0) + $count;
        }

        unset($aggregatedData);

        foreach ($finalData as &$dates) {
            ksort($dates);
        }
        unset($dates);

        file_put_contents($outputPath, json_encode($finalData, JSON_PRETTY_PRINT));
    }
}
