<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);

        $handle = fopen($inputPath, 'rb');
        $midPoint = intdiv($fileSize, 2);
        fseek($handle, $midPoint);
        fgets($handle);
        $splitPoint = ftell($handle);
        fclose($handle);

        $tmpFile = tempnam(sys_get_temp_dir(), 'p_');
        $pid = pcntl_fork();

        if ($pid === 0) {
            $result = $this->parseChunk($inputPath, $splitPoint, $fileSize);
            $serialized = \function_exists('igbinary_serialize') ? igbinary_serialize($result) : serialize($result);
            file_put_contents($tmpFile, $serialized);
            exit(0);
        }

        $data = $this->parseChunk($inputPath, 0, $splitPoint);

        pcntl_waitpid($pid, $status);

        $raw = file_get_contents($tmpFile);
        $childData = \function_exists('igbinary_unserialize') ? igbinary_unserialize($raw) : unserialize($raw);
        unlink($tmpFile);

        foreach ($childData as $key => $count) {
            $data[$key] = ($data[$key] ?? 0) + $count;
        }
        unset($childData, $raw);

        $nested = [];
        foreach ($data as $key => $count) {
            $nested[substr($key, 0, -11)][substr($key, -10)] = $count;
        }
        unset($data);

        foreach ($nested as &$dates) {
            ksort($dates, SORT_STRING);
        }
        unset($dates);

        $this->writeJson($outputPath, $nested);
    }

    private function parseChunk(string $inputPath, int $start, int $end): array
    {
        $data = [];
        $handle = fopen($inputPath, 'rb');
        fseek($handle, $start);

        $bytesToRead = $end - $start;
        $checkInterval = max(1, min(10000, (int)($bytesToRead / 100)));
        $count = 0;

        while (true) {
            if (++$count >= $checkInterval) {
                $count = 0;
                $pos = ftell($handle);
                if ($pos >= $end) {
                    break;
                }
                $remaining = $end - $pos;
                // Use max line length (~100 bytes) as divisor to never overshoot
                $checkInterval = max(1, (int)($remaining / 100));
            }

            $line = fgets($handle);
            if ($line === false) {
                break;
            }

            $key = substr($line, 19, -16);
            if (isset($data[$key])) {
                $data[$key]++;
            } else {
                $data[$key] = 1;
            }
        }

        fclose($handle);
        return $data;
    }

    private function writeJson(string $outputPath, array $data): void
    {
        $handle = fopen($outputPath, 'wb');
        stream_set_write_buffer($handle, 1024 * 1024);

        $buf = "{\n";
        $first = true;

        foreach ($data as $path => $dates) {
            if (!$first) {
                $buf .= ",\n";
            }
            $first = false;

            $buf .= '    "' . str_replace('/', '\\/', $path) . '": {' . "\n";

            $firstDate = true;
            foreach ($dates as $date => $count) {
                if ($firstDate) {
                    $firstDate = false;
                } else {
                    $buf .= ",\n";
                }
                $buf .= '        "' . $date . '": ' . $count;
            }
            $buf .= "\n    }";
        }

        $buf .= "\n}";
        fwrite($handle, $buf);
        fclose($handle);
    }
}
