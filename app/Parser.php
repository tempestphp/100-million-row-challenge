<?php

namespace App;

use Exception;
use function array_key_last;
use function fclose;
use function fgets;
use function file_put_contents;
use function filesize;
use function fopen;
use function fseek;
use function ftell;
use function ini_set;
use function intdiv;
use function ksort;
use function pcntl_exec;
use function pcntl_fork;
use function pcntl_waitpid;
use function set_error_handler;
use function set_exception_handler;
use function str_replace;
use function stream_set_read_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function sys_get_temp_dir;
use function tempnam;
use function unlink;

final class Parser
{
    private const WORKERS = 2;

    public function parse(string $inputPath, string $outputPath): void
    {
        ini_set('memory_limit', '700M');

        $chunks = $this->splitIntoChunks($inputPath);
        $tempFiles = $this->forkWorkers($inputPath, $chunks);

        $result = $this->mergeResults($tempFiles);

        file_put_contents($outputPath, $this->toJson($result));
    }

    private function splitIntoChunks(string $path): array
    {
        $fileSize = filesize($path);
        $chunkSize = intdiv($fileSize, self::WORKERS);
        $handle = fopen($path, 'r');
        $chunks = [];
        $start = 0;

        for ($i = 0; $i < self::WORKERS; $i++) {
            if ($i === self::WORKERS - 1) {
                $chunks[] = [$start, $fileSize];
            } else {
                fseek($handle, $start + $chunkSize);
                fgets($handle);
                $end = ftell($handle);
                $chunks[] = [$start, $end];
                $start = $end;
            }
        }

        fclose($handle);
        return $chunks;
    }

    private function forkWorkers(string $inputPath, array $chunks): array
    {
        $tempFiles = [];
        $pids = [];

        foreach ($chunks as [$start, $end]) {
            $tempFile = tempnam(sys_get_temp_dir(), 'parser_');
            $tempFiles[] = $tempFile;

            $pid = pcntl_fork();

            if ($pid === -1) {
                throw new Exception('pcntl_fork() failed');
            }

            if ($pid === 0) {
                set_error_handler(null);
                set_exception_handler(null);

                $result = $this->parseCsvChunk($inputPath, $start, $end);

                $out = '';
                foreach ($result as $url => $dates) {
                    foreach ($dates as $date => $count) {
                        $out .= "$url\t$date\t$count\n";
                    }
                }
                file_put_contents($tempFile, $out);
                pcntl_exec('/bin/true');
            }

            $pids[] = $pid;
        }

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        return $tempFiles;
    }

    private function mergeResults(array $tempFiles): array
    {
        $merged = [];

        foreach ($tempFiles as $tempFile) {
            $handle = fopen($tempFile, 'r');
            while (($line = fgets($handle, 65536)) !== false) {
                $tab1 = strrpos($line, "\t", -12);
                $tab2 = strrpos($line, "\t");
                $url = substr($line, 0, $tab1);
                $date = substr($line, $tab1 + 1, $tab2 - $tab1 - 1);
                $count = (int) substr($line, $tab2 + 1);
                $merged[$url][$date] = ($merged[$url][$date] ?? 0) + $count;
            }
            fclose($handle);
            unlink($tempFile);
        }

        foreach ($merged as &$dates) {
            ksort($dates);
        }

        return $merged;
    }

    private function parseCsvChunk(string $path, int $start, int $end): array
    {
        $result = [];
        $handle = fopen($path, 'r');
        fseek($handle, $start);

        stream_set_read_buffer($handle, 1 * 1024 * 1024);

        $pos = $start;
        while ($pos < $end && ($line = fgets($handle, 4096)) !== false) {
            $pos += strlen($line);
            $comma = strpos($line, ',');
            $url = substr($line, 19, $comma - 19);  // strip "https://stitcher.io"
            $date = substr($line, $comma + 1, 10);   // YYYY-MM-DD

            $ref = &$result[$url];
            if (isset($ref[$date])) {
                $ref[$date]++;
            } else {
                $ref[$date] = 1;
            }
        }

        fclose($handle);
        return $result;
    }

    private function toJson(array $result): string
    {
        $out = "{\n";
        $lastUrl = array_key_last($result);

        foreach ($result as $url => $dates) {
            $lastDate = array_key_last($dates);
            $escapedUrl = str_replace('/', '\/', $url);
            $out .= "    \"$escapedUrl\": {\n";

            foreach ($dates as $date => $count) {
                $comma = $date === $lastDate ? '' : ',';
                $out .= "        \"$date\": $count$comma\n";
            }

            $comma = $url === $lastUrl ? '' : ',';
            $out .= "    }$comma\n";
        }

        return $out . '}';
    }
}
