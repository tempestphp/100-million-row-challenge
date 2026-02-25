<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $fileSize >> 1);
        fgets($handle);
        $splitPoint = ftell($handle);
        fclose($handle);

        $useIgbinary = function_exists('igbinary_serialize');
        $shmDir = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
        $workerFile = $shmDir . '/p100m_0.dat';
        $pid = pcntl_fork();

        if ($pid === 0) {
            $data = self::parseRange($inputPath, $splitPoint, $fileSize);
            file_put_contents($workerFile, $useIgbinary ? igbinary_serialize($data) : serialize($data));
            exit(0);
        }

        $merged = self::parseRange($inputPath, 0, $splitPoint);

        pcntl_waitpid($pid, $status);

        $raw = file_get_contents($workerFile);
        $childData = $useIgbinary ? igbinary_unserialize($raw) : unserialize($raw);
        unlink($workerFile);

        foreach ($childData as $key => $count) {
            if (isset($merged[$key])) {
                $merged[$key] += $count;
            } else {
                $merged[$key] = $count;
            }
        }

        $result = [];

        foreach ($merged as $key => $count) {
            $result[substr($key, 0, -11)][substr($key, -10)] = $count;
        }

        foreach ($result as &$dates) {
            ksort($dates);
        }
        unset($dates);

        file_put_contents($outputPath, json_encode($result, JSON_PRETTY_PRINT));
    }

    private static function parseRange(string $inputPath, int $start, int $end): array
    {
        $data = [];
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);
        $remaining = $end - $start;
        $tail = '';

        while ($remaining > 0) {
            $chunk = fread($handle, $remaining > 1048576 ? 1048576 : $remaining);
            $remaining -= strlen($chunk);
            $pos = 0;

            if ($tail !== '') {
                $firstNl = strpos($chunk, "\n");

                if ($firstNl === false) {
                    $tail .= $chunk;
                    continue;
                }

                $line = $tail . substr($chunk, 0, $firstNl);
                $tail = '';
                $lineLen = strlen($line);

                if ($lineLen > 35) {
                    $key = substr($line, 19, $lineLen - 34);
                    if (isset($data[$key])) {
                        $data[$key]++;
                    } else {
                        $data[$key] = 1;
                    }
                }

                $pos = $firstNl + 1;
            }

            $lastNl = strrpos($chunk, "\n");

            if ($lastNl === false || $lastNl < $pos) {
                $tail = ($pos === 0) ? $chunk : substr($chunk, $pos);
                continue;
            }

            if ($lastNl < strlen($chunk) - 1) {
                $tail = substr($chunk, $lastNl + 1);
            }

            while ($pos < $lastNl) {
                $nlPos = strpos($chunk, "\n", $pos);
                $key = substr($chunk, $pos + 19, $nlPos - $pos - 34);
                if (isset($data[$key])) {
                    $data[$key]++;
                } else {
                    $data[$key] = 1;
                }
                $pos = $nlPos + 1;
            }
        }

        if ($tail !== '') {
            $len = strlen($tail);

            if ($len > 35) {
                $key = substr($tail, 19, $len - 34);
                if (isset($data[$key])) {
                    $data[$key]++;
                } else {
                    $data[$key] = 1;
                }
            }
        }

        fclose($handle);

        return $data;
    }
}
