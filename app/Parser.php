<?php

namespace App;

final class Parser
{
    private const int STITCHER_PREFIX_LENGTH = 19;
    private const int DATE_LENGTH = 10;
    private const int COMMA_DELIMITER_POSITION = 26;
    private const int MIN_NUM_PROCESSES = 16;
    private const int CHUNK_SIZE = 2_097_152;


    public function parse(string $inputPath, string $outputPath): void
    {
        $numProcesses = max(self::MIN_NUM_PROCESSES,
            (int)shell_exec('nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null') * 2
        );
        $fileSize = filesize($inputPath);
        $chunkSize = ceil($fileSize / $numProcesses);

        $boundaries = [0];
        $handle = fopen($inputPath, 'rb');
        for ($i = 1; $i < $numProcesses; $i++) {
            fseek($handle, $i * $chunkSize);
            fgets($handle);
            $boundaries[] = ftell($handle);
        }
        fclose($handle);
        $boundaries[] = $fileSize;

        $tempFiles = [];
        $pids = [];

        for ($i = 0; $i < $numProcesses; $i++) {
            $tempFiles[$i] = tempnam(sys_get_temp_dir(), "parser_chunk_$i");
            $startByte = $boundaries[$i];
            $endByte = $boundaries[$i + 1] - 1;

            $pid = pcntl_fork();
            if ($pid == -1) {
                die("Could not fork process");
            } elseif ($pid == 0) {
                self::processChunk($inputPath, $startByte, $endByte, $tempFiles[$i]);
                exit(0);
            } else {
                $pids[] = $pid;
            }
        }

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $existingPath = [];
        for ($i = 0; $i < $numProcesses; $i++) {
            $chunkResults = igbinary_unserialize(file_get_contents($tempFiles[$i]));
            unlink($tempFiles[$i]);

            foreach ($chunkResults as $path => $dates) {
                foreach ($dates as $date => $count) {
                    $existingPath[$path][$date] = ($existingPath[$path][$date] ?? 0) + $count;
                }
            }
        }

        foreach ($existingPath as $path => $dates) {
            ksort($existingPath[$path]);
        }

        file_put_contents($outputPath, json_encode($existingPath, JSON_PRETTY_PRINT));
    }

    public static function processChunk(string $inputPath, int $startByte, int $endByte, string $outputFile): void
    {
        $file = fopen($inputPath, 'rb');
        stream_set_read_buffer($file, 0);
        fseek($file, $startByte);

        $toRead = $endByte - $startByte;
        $remainder = '';
        $existingPath = [];

        while ($toRead > 0) {
            $chunk = fread($file, $toRead > self::CHUNK_SIZE ? self::CHUNK_SIZE : $toRead);
            $toRead -= strlen($chunk);

            if ($remainder !== '') {
                $chunk = $remainder . $chunk;
            }

            $newLine = strrpos($chunk, "\n");
            $remainder = substr($chunk, $newLine + 1);

            $pos = 0;
            while ($pos < $newLine) {
                $newLinePos = strpos($chunk, "\n", $pos);
                $line = substr($chunk, $pos, $newLinePos - $pos);

                $content = substr($line, self::STITCHER_PREFIX_LENGTH);
                $path = substr($content, 0, -self::COMMA_DELIMITER_POSITION);

                $date = substr($content, -self::COMMA_DELIMITER_POSITION + 1, self::DATE_LENGTH);
                if (strlen($date) !== self::DATE_LENGTH) {
                    $pos = $newLinePos + 1;
                    continue;
                }

                $existingPath[$path][$date] = ($existingPath[$path][$date] ?? 0) + 1;
                $pos = $newLinePos + 1;
            }
        }

        if ($remainder !== '') {
            $content = substr($remainder, self::STITCHER_PREFIX_LENGTH);
            $path = substr($content, 0, -self::COMMA_DELIMITER_POSITION);

            $date = substr($content, -self::COMMA_DELIMITER_POSITION + 1, self::DATE_LENGTH);
            if (strlen($date) === self::DATE_LENGTH) {
                $existingPath[$path][$date] = ($existingPath[$path][$date] ?? 0) + 1;
            }
        }

        fclose($file);

        file_put_contents($outputFile, igbinary_serialize($existingPath));
    }
}
