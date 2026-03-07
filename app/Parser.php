<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        // we have 12 GB, assuming 8GB is for the system and other processes, we can use up to 4GB for our parser
        // with 8 workers, that’s 512MB each — should be enough for our needs, but we can adjust if necessary
        ini_set('memory_limit', '512M');

        $this->parseDefault($inputPath, $outputPath);
    }

    public function parseDefault(string $inputPath, string $outputPath): void
    {
        $numWorkers = 8;
        $fileSize = filesize($inputPath);
        $chunkSize = intdiv($fileSize, $numWorkers);

        $tmpFiles = [];
        $pids = [];

        for ($i = 0; $i < $numWorkers; $i++) {
            $tmpFile = tempnam(sys_get_temp_dir(), 'parser_');
            $tmpFiles[] = $tmpFile;

            $start = $i * $chunkSize;
            $end = ($i === $numWorkers - 1) ? $fileSize : ($i + 1) * $chunkSize;

            $pid = pcntl_fork();
            if ($pid === -1) {
                die('Could not fork');
            } elseif ($pid === 0) {
                // Child process
                $result = [];
                $handle = fopen($inputPath, 'r');

                if ($start > 0) {
                    fseek($handle, $start);
                    fgets($handle); // skip partial line
                }

                $bufferSize = 8 * 1024 * 1024; // 8MB chunks — ~110 reads vs 12.5M fgets calls
                $leftover = '';
                $curPos = ftell($handle);

                while ($curPos < $end) {
                    $toRead = min($bufferSize, $end - $curPos); // never read past $end
                    $chunk = fread($handle, $toRead);
                    if ($chunk === false || $chunk === '') break;
                    $curPos += strlen($chunk);

                    $data = $leftover . $chunk;
                    $lastNl = strrpos($data, "\n");
                    if ($lastNl === false) { $leftover = $data; continue; }

                    $leftover = substr($data, $lastNl + 1);
                    foreach (explode("\n", substr($data, 0, $lastNl)) as $line) {
                        if ($line === '') continue;
                        $commaPos = strpos($line, ',');
                        $day = substr($line, $commaPos + 1, 10);
                        $slashPos = strpos($line, '/', 8); // skip 'https://'
                        $path = substr($line, $slashPos, $commaPos - $slashPos);
                        $result[$path][$day] = ($result[$path][$day] ?? 0) + 1;
                    }
                }

                // $leftover holds the start of the line straddling $end — complete it with fgets
                if ($leftover !== '') {
                    $rest = fgets($handle);
                    $line = rtrim($leftover . ($rest !== false ? $rest : ''), "\n");
                    if ($line !== '') {
                        $commaPos = strpos($line, ',');
                        $day = substr($line, $commaPos + 1, 10);
                        $slashPos = strpos($line, '/', 8);
                        $path = substr($line, $slashPos, $commaPos - $slashPos);
                        $result[$path][$day] = ($result[$path][$day] ?? 0) + 1;
                    }
                }

                fclose($handle);

                $serialized = function_exists('igbinary_serialize')
                    ? igbinary_serialize($result)
                    : serialize($result);
                file_put_contents($tmpFile, $serialized);
                exit(0);
            } else {
                $pids[] = $pid;
            }
        }

        // Wait for all children
        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        // Merge results
        $result = [];
        foreach ($tmpFiles as $tmpFile) {
            $raw = file_get_contents($tmpFile);
            $partial = function_exists('igbinary_unserialize')
                ? igbinary_unserialize($raw)
                : unserialize($raw);
            // be mindful of unserialize though

            unlink($tmpFile);
            foreach ($partial as $path => $days) {
                foreach ($days as $day => $count) {
                    $result[$path][$day] = ($result[$path][$day] ?? 0) + $count;
                }
            }
        }

        foreach ($result as &$days) {
            ksort($days);
        }

        file_put_contents($outputPath, json_encode($result, JSON_PRETTY_PRINT));
    }
}
