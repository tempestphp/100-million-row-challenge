<?php

namespace App;

use Exception;

/**
 * Optimized Parser for the 100 Million Row Challenge.
 */
final class Parser
{
    private const int CPU_COUNT = 8;
    private const int PREFIX_LEN = 19;
    private const int BUFFER_SIZE = 16 * 1024 * 1024;

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();
        ini_set('memory_limit', '-1');

        $fileSize = filesize($inputPath);
        if ($fileSize === 0) {
            file_put_contents($outputPath, "{}\n");
            return;
        }

        $chunkSize = (int) ceil($fileSize / self::CPU_COUNT);
        $pids = [];
        $tempFiles = [];

        for ($i = 0; $i < self::CPU_COUNT; $i++) {
            $start = $i * $chunkSize;
            $end = ($i === self::CPU_COUNT - 1) ? $fileSize : ($i + 1) * $chunkSize;
            
            $tempFile = sys_get_temp_dir() . "/results_part_$i.bin";
            $tempFiles[] = $tempFile;

            $pid = pcntl_fork();
            if ($pid === -1) {
                throw new Exception("Could not fork process $i");
            }

            if ($pid === 0) {
                try {
                    $this->processChunk($inputPath, $start, $end, $tempFile);
                    exit(0);
                } catch (\Throwable $e) {
                    exit(1);
                }
            } else {
                $pids[] = $pid;
            }
        }

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $mergedAggregates = [];
        foreach ($tempFiles as $tempFile) {
            if (!file_exists($tempFile)) continue;
            
            $serializedData = file_get_contents($tempFile);
            $partResults = function_exists('igbinary_unserialize') 
                ? igbinary_unserialize($serializedData) 
                : unserialize($serializedData);
            unlink($tempFile);

            if (!is_array($partResults)) continue;

            foreach ($partResults as $key => $count) {
                if (isset($mergedAggregates[$key])) {
                    $mergedAggregates[$key] += $count;
                } else {
                    $mergedAggregates[$key] = $count;
                }
            }
            unset($partResults);
        }

        $finalResults = [];
        foreach ($mergedAggregates as $key => $count) {
            $commaPos = strrpos($key, ',');
            $url = substr($key, 0, $commaPos);
            $date = substr($key, $commaPos + 1);
            $finalResults[$url][$date] = $count;
        }
        unset($mergedAggregates);

        foreach ($finalResults as &$dates) {
            ksort($dates, SORT_STRING);
        }

        file_put_contents($outputPath, json_encode($finalResults, JSON_PRETTY_PRINT));
    }

    private function processChunk(string $inputPath, int $start, int $end, string $tempFile): void
    {
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        
        if ($start > 0) {
            fseek($handle, $start - 1);
            while (($char = fgetc($handle)) !== false && $char !== "\n");
        } else {
            fseek($handle, 0);
        }

        $currentFilePos = ftell($handle);
        $results = [];
        $remaining = '';
        
        while ($currentFilePos < $end) {
            $chunk = fread($handle, self::BUFFER_SIZE);
            if ($chunk === false || $chunk === '') break;
            
            $data = $remaining . $chunk;
            $dataStartInFile = $currentFilePos - strlen($remaining);
            $currentFilePos += strlen($chunk);

            $pos = 0;
            while (($newlinePos = strpos($data, "\n", $pos)) !== false) {
                if ($dataStartInFile + $pos >= $end) {
                    $pos = strlen($data);
                    break;
                }

                $key = substr($data, $pos + self::PREFIX_LEN, $newlinePos - $pos - (self::PREFIX_LEN + 15));
                
                if (isset($results[$key])) {
                    $results[$key]++;
                } else {
                    $results[$key] = 1;
                }
                
                $pos = $newlinePos + 1;
            }
            
            $remaining = substr($data, $pos);
        }

        if ($remaining !== '') {
            $newlinePos = strpos($remaining, "\n");
            $line = ($newlinePos === false) ? $remaining : substr($remaining, 0, $newlinePos);
            if ($line !== '') {
                $key = substr($line, self::PREFIX_LEN, -15);
                if (isset($results[$key])) {
                    $results[$key]++;
                } else {
                    $results[$key] = 1;
                }
            }
        }

        fclose($handle);
        
        $serialized = function_exists('igbinary_serialize') 
            ? igbinary_serialize($results) 
            : serialize($results);
        file_put_contents($tempFile, $serialized);
    }
}
