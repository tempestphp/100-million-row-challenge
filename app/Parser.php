<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        // Absolute performance tuning
        \ini_set('memory_limit', '-1');
        \ini_set('pcre.backtrack_limit', '10000000');
        \ini_set('igbinary.compact_strings', '1');
        \gc_disable();

        $fileSize = (int)\filesize($inputPath);
        if ($fileSize === 0) {
            \file_put_contents($outputPath, '{}');
            return;
        }

        // Hardware-aware core detection
        $nproc = (int) (\shell_exec('nproc 2>/dev/null') ?: \shell_exec('sysctl -n hw.ncpu 2>/dev/null') ?: 8);
        $numCores = (\function_exists('pcntl_fork') && $fileSize > 5 * 1024 * 1024) ? $nproc : 1;

        // Use /dev/shm on Linux for zero-disk IPC, fallback to temp on macOS
        $shmPath = \is_dir('/dev/shm') ? '/dev/shm' : \sys_get_temp_dir();

        $processSegment = static function (string $inputPath, int $start, int $end) {
            \gc_disable();
            $handle = \fopen($inputPath, 'rb');
            if ($start > 0) {
                \fseek($handle, $start - 1);
                \fgets($handle);
            }
            $actualStart = \ftell($handle);
            \fclose($handle);
            
            $length = $end - $actualStart;
            if ($length <= 0) return [];
            
            $data = \file_get_contents($inputPath, false, null, $actualStart, $length);
            
            // Regex with 'S' (Study) modifier for even faster repetitive execution
            \preg_match_all('/\.io\K[^,]+,[\d-]{10}/S', $data, $matches);
            unset($data);
            
            if (empty($matches[0])) return [];
            
            $counts = \array_count_values($matches[0]);
            unset($matches);
            
            $nested = [];
            foreach ($counts as $key => $count) {
                // key: "/path,YYYY-MM-DD"
                $nested[\substr($key, 0, -11)][\substr($key, -10)] = $count;
            }
            return $nested;
        };

        $outputData = [];
        if ($numCores === 1) {
            $outputData = $processSegment($inputPath, 0, $fileSize);
        } else {
            $chunkSize = (int)($fileSize / $numCores);
            $pids = [];
            
            for ($i = 0; $i < $numCores; $i++) {
                $tmpFile = $shmPath . '/php_p_' . $i . '_' . \getmypid();
                $pid = \pcntl_fork();
                
                if ($pid === 0) {
                    $start = $i * $chunkSize;
                    $end = ($i === $numCores - 1) ? $fileSize : ($i + 1) * $chunkSize;
                    $childCounts = $processSegment($inputPath, $start, $end);
                    \file_put_contents($tmpFile, \igbinary_serialize($childCounts));
                    exit(0);
                }
                
                $pids[$pid] = $tmpFile;
            }

            // High-speed merging as workers finish
            while (\count($pids) > 0) {
                $pid = \pcntl_wait($status);
                if ($pid > 0) {
                    $tmpFile = $pids[$pid];
                    unset($pids[$pid]);
                    
                    if ($childData = \file_get_contents($tmpFile)) {
                        $childCounts = \igbinary_unserialize($childData);
                        unset($childData);
                        
                        foreach ($childCounts as $path => $dates) {
                            if (!isset($outputData[$path])) {
                                $outputData[$path] = $dates;
                            } else {
                                // Optimization: Access target array by reference
                                $target = &$outputData[$path];
                                foreach ($dates as $date => $count) {
                                    $target[$date] = ($target[$date] ?? 0) + $count;
                                }
                            }
                        }
                        unset($childCounts);
                    }
                }
            }
        }
        
        foreach ($outputData as &$dates) { \ksort($dates, SORT_STRING); }
        \file_put_contents($outputPath, \json_encode($outputData, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT));
    }
}
