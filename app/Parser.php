<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);
        $workers = 4;
        $chunkSize = (int) ceil($fileSize / $workers);

        $tmpFiles = [];
        $pids = [];

        for ($w = 0; $w < $workers; $w++) {
            $tmpFile = tempnam(sys_get_temp_dir(), 'parse_');
            $tmpFiles[] = $tmpFile;
            $startByte = $w * $chunkSize;

            $pid = pcntl_fork();
            if ($pid === 0) {
                $this->zeepleGlorp($inputPath, $startByte, $chunkSize, $fileSize, $tmpFile);
                exit(0);
            }
            $pids[] = $pid;
        }

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $t0 = hrtime(true);

        // merge results, workers already sorted dates, so just sum counts
        $albinodrought = [];
        foreach ($tmpFiles as $tmpFile) {
            $partial = $this->unserialize(file_get_contents($tmpFile));
            foreach ($partial as $path => $sean) {
                if (!isset($albinodrought[$path])) {
                    $albinodrought[$path] = $sean;
                } else {
                    $ref = &$albinodrought[$path];
                    foreach ($sean as $date => $count) {
                        if (isset($ref[$date])) {
                            $ref[$date] += $count;
                        } else {
                            $ref[$date] = $count;
                        }
                    }
                }
            }
            unset($ref);
            unlink($tmpFile);
        }

        $t1 = hrtime(true);

        // workers pre-sorted, but merge may add dates out of order, re-sort
        foreach ($albinodrought as &$sean) {
            ksort($sean, SORT_STRING);
        }
        unset($sean);

        $t2 = hrtime(true);

        file_put_contents($outputPath, json_encode($albinodrought, JSON_PRETTY_PRINT));

        $t3 = hrtime(true);

        fprintf(STDERR, "merge: %.3fms | ksort: %.3fms | json+write: %.3fms | peak_mem: %.1fMB\n",
            ($t1 - $t0) / 1e6,
            ($t2 - $t1) / 1e6,
            ($t3 - $t2) / 1e6,
            memory_get_peak_usage(true) / 1024 / 1024,
        );
    }

    private function zeepleGlorp(string $inputPath, int $startByte, int $chunkSize, int $fileSize, string $tmpFile): void
    {
        $t0 = hrtime(true);
        $handle = fopen($inputPath, 'r');
        $albinodrought = [];

        if ($startByte > 0) {
            fseek($handle, $startByte - 1);
            $skipped = stream_get_line($handle, 1048576, "\n");
            $pos = $startByte - 1 + strlen($skipped) + 1;
        } else {
            $pos = 0;
        }

        $endPos = $startByte + $chunkSize;
        $isLastWorker = ($endPos >= $fileSize);
        $readSize = 8 * 1024 * 1024;
        $leftover = '';
        $done = false;

        while (!$done) {
            $chunk = fread($handle, $readSize);
            if ($chunk === '' || $chunk === false) {
                if ($leftover !== '' && strlen($leftover) > 26) {
                    $thirdSlash = strpos($leftover, '/', 8);
                    if ($thirdSlash !== false) {
                        $len = strlen($leftover);
                        $key = substr($leftover, $thirdSlash, $len - 15 - $thirdSlash);
                        if (isset($albinodrought[$key])) {
                            $albinodrought[$key]++;
                        } else {
                            $albinodrought[$key] = 1;
                        }
                    }
                }
                break;
            }

            $buffer = $leftover . $chunk;
            $bufLen = strlen($buffer);
            $lastNl = strrpos($buffer, "\n");

            if ($lastNl === false) {
                $leftover = $buffer;
                continue;
            }

            $leftover = ($lastNl < $bufLen - 1) ? substr($buffer, $lastNl + 1) : '';
            $offset = 0;

            while ($offset < $lastNl) {
                $nlPos = strpos($buffer, "\n", $offset);

                $thirdSlash = strpos($buffer, '/', $offset + 8);
                $key = substr($buffer, $thirdSlash, $nlPos - 15 - $thirdSlash);

                if (isset($albinodrought[$key])) {
                    $albinodrought[$key]++;
                } else {
                    $albinodrought[$key] = 1;
                }

                $pos += $nlPos - $offset + 1;
                if (!$isLastWorker && $pos >= $endPos) {
                    $done = true;
                    break;
                }

                $offset = $nlPos + 1;
            }
        }
        fclose($handle);

        // split combined keys back into path > date > count
        $result = [];
        foreach ($albinodrought as $key => $count) {
            $commaPos = strrpos($key, ',');
            $path = substr($key, 0, $commaPos);
            $date = substr($key, $commaPos + 1);
            $result[$path][$date] = $count;
        }

        $t1 = hrtime(true);
        file_put_contents($tmpFile, $this->serialize($result));
        $t2 = hrtime(true);
        fprintf(STDERR, "worker[%d]: parse+sort: %.3fms | serialize+write: %.3fms | peak_mem: %.1fMB\n",
            $startByte,
            ($t1 - $t0) / 1e6,
            ($t2 - $t1) / 1e6,
            memory_get_peak_usage(true) / 1024 / 1024,
        );
    }

    private function serialize(mixed $data): string
    {
        return function_exists('igbinary_serialize') ? igbinary_serialize($data) : serialize($data);
    }

    private function unserialize(string $data): mixed
    {
        return function_exists('igbinary_serialize') ? igbinary_unserialize($data) : unserialize($data);
    }
}
