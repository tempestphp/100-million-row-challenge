<?php

declare(strict_types=1);

namespace App;

use SplFileObject;
use App\Commands\Visit;

final class Parser
{
    public $stats;
    public const CPU_CORES = 8;

    public function parse(string $inputPath, string $outputPath): void
    {
        $this->read($inputPath);
        $this->write($outputPath);
    }

    /**
     * Read input file stats and splits file into blocks to be processed in parallel.
     *
     * @param string $inputPath The path to the input file.
     * @return void
     */
    private function read(string $inputPath): void
    {
        $file = new SplFileObject($inputPath);
        $stat = $file->fstat();
        $fileSize = $stat['size'];
        $baseBlockSize = (int) ceil($fileSize / self::CPU_CORES);

        $currentOffset = 0;
        $pids = [];

        for ($i = 0; $i < self::CPU_CORES; $i++) {
            $endOffset = $currentOffset + $baseBlockSize;

            if ($endOffset >= $fileSize) {
                $endOffset = $fileSize;
            } else {
                $extra = $this->getExtraBlockSize($file, $endOffset);
                $endOffset += $extra;
            }

            $length = $endOffset - $currentOffset;
            $blockStart = $currentOffset;
            $currentOffset = $endOffset;

            if ($length <= 0) break;

            $tempFile = tempnam(sys_get_temp_dir(), 'worker_' . $i . '_');
            $tempFiles[$i] = $tempFile;

            $pid = pcntl_fork();
            if ($pid == -1) {
                die('could not fork');
            } else if ($pid) {
                $pids[] = $pid;
            } else {
                $childFile = new SplFileObject($inputPath);
                $childFile->fseek($blockStart);
                $this->readBlock($childFile, $endOffset, $tempFile);
                exit(0);
            }
        }

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        foreach ($tempFiles as $i => $tempFile) {
            if (file_exists($tempFile)) {
                $this->stats[$i] = unserialize(file_get_contents($tempFile));
                unlink($tempFile);
            }
        }
    }

    /**
     * Merges the stats from all workers and writes the final result to the output file.
     *
     * @param string $outputPath The path to the output file.
     * @return void
     */
    private function write(string $outputPath): void
    {
        $merged = [];
        foreach ($this->stats as $workerStats) {
            foreach ($workerStats as $key => $count) {
                $merged[$key] = ($merged[$key] ?? 0) + $count;
            }
        }

        $result = [];
        foreach ($merged as $key => $count) {
            $commaPos = strrpos($key, ',');
            if ($commaPos === false) continue;

            $path = substr($key, 0, $commaPos);
            $date = substr($key, $commaPos + 1);

            if (!isset($result[$path])) {
                $result[$path] = [];
            }
            $result[$path][$date] = ($result[$path][$date] ?? 0) + $count;
        }

        foreach ($result as &$dates) {
            ksort($dates);
        }
        unset($dates);

        file_put_contents($outputPath, json_encode($result, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE));
    }

    /**
     * Reads a block of the file, processes its contents, and writes the results to a temporary file.
     *
     * @param SplFileObject $file The file object to read from.
     * @param int $endOffset The offset to stop reading at.
     * @param string $tempFile The path to the temporary file to write results to.
     * @return void
     */
    private function readBlock(SplFileObject $file, int $endOffset, string $tempFile): void
    {
        $lines = [];
        while($line = $file->fgets()) {
            $lines[] = substr($line, 19, -16);

            if($file->ftell() >= $endOffset) {
                break;
            }
        }
        file_put_contents($tempFile, serialize(array_count_values($lines)));
    }

    /**
     * Reads extra bytes to ensure file pointer ends on newline character.
     *
     * @param SplFileObject $file The file object to read from.
     * @param int $offset The offset to start reading from.
     * @return int The number of extra bytes read to reach the next newline.
     */
    private function getExtraBlockSize(SplFileObject $file, int $offset): int
    {
        $file->fseek($offset);

        if ($file->eof()) {
            return 0;
        }

        $chunk = $file->fread(128);
        if ($chunk === false || $chunk === '') {
            return 0;
        }

        $position = strpos($chunk, "\n");
        if ($position === false) {
            return 0;
        }

        return $position + 1;
    }
}