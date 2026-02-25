<?php

namespace App;

use function shell_exec;
use function filesize;
use function is_dir;
use function dirname;
use function getmypid;
use function ceil;
use function pcntl_fork;
use function min;
use function fopen;
use function fseek;
use function fread;
use function fgets;
use function ftell;
use function strlen;
use function strrpos;
use function substr;
use function strpos;
use function fclose;
use function file_put_contents;
use function pcntl_waitpid;
use function unserialize;
use function file_get_contents;
use function unlink;
use function ksort;
use function json_encode;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $threads = (int)shell_exec('nproc 2>/dev/null');
        $filesize = filesize($inputPath);
        $tmpDir = is_dir('/dev/shm') ? '/dev/shm' : dirname($outputPath);
        $uid = getmypid();

        $chunkSize = (int)ceil($filesize / $threads);
        $pids = [];

        for ($i = 0; $i < $threads; $i++) {
            $pid = pcntl_fork();
            if ($pid === -1) exit("Fork failed");

            if ($pid === 0) {
                // --- CHILD PROCESS ---
                $startByte = $i * $chunkSize;
                $endByte = min(($i + 1) * $chunkSize, $filesize);

                $fp = fopen($inputPath, 'rb');
                fseek($fp, $startByte);
                if ($i > 0) {
                    fseek($fp, $startByte - 1);
                    if (fread($fp, 1) !== "\n") fgets($fp); // skip partial line only if mid-line
                }

                $bytesRemaining = $endByte - ftell($fp);
                $results = [];
                $buffer = '';

                while ($bytesRemaining > 0) {
                    $chunk = fread($fp, min(1048576, $bytesRemaining));
                    if ($chunk === false || $chunk === '') break;
                    $bytesRemaining -= strlen($chunk);

                    if ($buffer !== '') {
                        $chunk = $buffer . $chunk;
                        $buffer = '';
                    }

                    $lastNl = strrpos($chunk, "\n");
                    if ($lastNl === false) {
                        $buffer = $chunk;
                        continue;
                    }

                    if ($lastNl < strlen($chunk) - 1) {
                        $buffer = substr($chunk, $lastNl + 1);
                    }

                    // All rows: https://stitcher.io/blog/PATH,yyyy-mm-ddT00:00:00+00:00
                    $pos = 0;
                    while ($pos < $lastNl) {
                        $commaPos = strpos($chunk, ',', $pos + 25);
                        $nlPos = strpos($chunk, "\n", $commaPos);

                        $path = substr($chunk, $pos + 19, $commaPos - $pos - 19);
                        $date = substr($chunk, $commaPos + 1, 10);

                        if (isset($results[$path][$date])) {
                            $results[$path][$date]++;
                        } else {
                            $results[$path][$date] = 1;
                        }

                        $pos = $nlPos + 1;
                    }
                }

                // Handle remaining partial line at chunk boundary
                if ($buffer !== '') {
                    $rest = fgets($fp);
                    if ($rest !== false) $buffer .= $rest;
                    if (strlen($buffer) > 25) {
                        $commaPos = strpos($buffer, ',', 25);
                        if ($commaPos !== false) {
                            $path = substr($buffer, 19, $commaPos - 19);
                            $date = substr($buffer, $commaPos + 1, 10);
                            if (isset($results[$path][$date])) {
                                $results[$path][$date]++;
                            } else {
                                $results[$path][$date] = 1;
                            }
                        }
                    }
                }

                fclose($fp);
                file_put_contents("{$tmpDir}/csv_{$uid}_{$i}.dat", serialize($results));
                exit(0);
            }

            $pids[] = $pid;
        }

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $merged = [];
        for ($i = 0; $i < $threads; $i++) {
            $tempFile = "{$tmpDir}/csv_{$uid}_{$i}.dat";
            /** @var array<string, array<string, int>> $partial */
            $partial = unserialize(file_get_contents($tempFile));
            unlink($tempFile);

            foreach ($partial as $path => $dates) {
                foreach ($dates as $date => $count) {
                    if (isset($merged[$path][$date])) {
                        $merged[$path][$date] += $count;
                    } else {
                        $merged[$path][$date] = $count;
                    }
                }
            }
        }

        foreach ($merged as &$result) {
            ksort($result);
        }

        file_put_contents($outputPath, json_encode($merged, JSON_PRETTY_PRINT));
    }
}
