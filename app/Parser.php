<?php

declare(strict_types=1);

namespace App;

final class Parser
{
    private const int READ_CHUNK_SIZE = 1024*1024;
    private const int WORKER_COUNT = 4;

    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);
        $segmentSize = (int) ($fileSize / self::WORKER_COUNT);
        $boundaries = [0];

        $handle = fopen($inputPath, 'rb');
        for ($i = 1; $i < self::WORKER_COUNT; $i++) {
            fseek($handle, $i * $segmentSize);
            fgets($handle);
            $boundaries[] = (int) ftell($handle);
        }
        $boundaries[] = $fileSize;
        fclose($handle);

        $workerPids = [];
        $tempFiles = [];
        $parentPid = getmypid();

        for ($i = 0; $i < self::WORKER_COUNT; $i++) {
            $binFile = "/dev/shm/d_{$parentPid}_{$i}.bin";
            $mapFile = "/dev/shm/m_{$parentPid}_{$i}.php";
            $tempFiles[] = ['bin' => $binFile, 'map' => $mapFile];

            $pid = pcntl_fork();
            if ($pid === 0) {
                gc_disable();
                $this->processSegment($inputPath, $boundaries[$i], $boundaries[$i + 1], $binFile, $mapFile);
                exit(0);
            }
            $workerPids[] = $pid;
        }

        foreach ($workerPids as $pid) pcntl_waitpid($pid, $status);
        $this->mergeResults($tempFiles, $outputPath);
    }

    private function processSegment(string $path, int $start, int $end, string $binFile, string $mapFile): void
    {
        $readHandle = fopen($path, 'rb');
        $writeHandle = fopen($binFile, 'wb');
        stream_set_write_buffer($writeHandle, 0);
        fseek($readHandle, $start);

        $bytesToRead = $end - $start;
        $leftover = '';

        $urlMap = []; $urlReg = []; $uCount = 0;
        $dateMap = []; $dateReg = []; $dCount = 0;
        $freqMap = [];

        while ($bytesToRead > 0) {
            $chunk = fread($readHandle, min($bytesToRead, self::READ_CHUNK_SIZE));
            if ($chunk === false || $chunk === '') break;
            $bytesToRead -= strlen($chunk);

            $chunk = $leftover . $chunk;
            $lastNewline = strrpos($chunk, "\n");
            if ($lastNewline === false) { $leftover = $chunk; continue; }
            $leftover = substr($chunk, $lastNewline + 1);

            $cursor = 0;
            while ($cursor < $lastNewline) {
                $lineEnd = strpos($chunk, "\n", $cursor);

                $rawUrl = substr($chunk, $cursor + 19, $lineEnd - $cursor - 45);
                if (!isset($urlMap[$rawUrl])) {
                    $uId = $urlMap[$rawUrl] = $uCount++;
                    $urlReg[$uId] = $rawUrl;
                } else {
                    $uId = $urlMap[$rawUrl];
                }

                $rawDate = substr($chunk, $lineEnd - 25, 10);
                if (!isset($dateMap[$rawDate])) {
                    $dId = $dateMap[$rawDate] = $dCount++;
                    $dateReg[$dId] = $rawDate;
                } else {
                    $dId = $dateMap[$rawDate];
                }

                $idx = ($uId << 10) | $dId;
                if (!isset($freqMap[$idx])) {
                    $freqMap[$idx] = 1;
                } else {
                    $freqMap[$idx]++;
                }

                $cursor = $lineEnd + 1;
            }
        }

        foreach ($freqMap as $idx => $count) {
            fwrite($writeHandle, pack('VV', $idx, $count));
        }

        fclose($readHandle);
        fclose($writeHandle);
        file_put_contents($mapFile, "<?php return " . var_export(['u' => $urlReg, 'd' => $dateReg], true) . ";");
    }
    private function mergeResults(array $tempFiles, string $outputPath): void
    {
        $aggregated = [];
        $allDates = [];

        // 1. Collecte et agrégation des données
        foreach ($tempFiles as $files) {
            $maps = require $files['map'];
            $handle = fopen($files['bin'], 'rb');

            // On fusionne les dates connues pour pouvoir les trier plus tard
            foreach ($maps['d'] as $dId => $dateStr) {
                if (!isset($allDates[$dateStr])) {
                    $allDates[$dateStr] = $dateStr;
                }
            }

            while (!feof($handle)) {
                $bin = fread($handle, 8);
                if (strlen($bin) < 8) break;

                $data = unpack('Vidx/Vcount', $bin);
                $idx = $data['idx'];

                $url = $maps['u'][$idx >> 10];
                $date = $maps['d'][$idx & 1023];

                if (!isset($aggregated[$url])) {
                    $aggregated[$url] = [];
                }

                $aggregated[$url][$date] = ($aggregated[$url][$date] ?? 0) + $data['count'];
            }

            fclose($handle);
            unlink($files['bin']);
            unlink($files['map']);
        }

        asort($allDates);
        $orderedDates = array_values($allDates);

        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1048576);

        fwrite($out, '{');

        $firstPath = true;

        foreach ($aggregated as $url => $pathCounts) {
            $pathBuffer = $firstPath ? '' : ',';
            $firstPath = false;

            $escapedPath = str_replace('/', '\\/', $url);
            $pathBuffer .= "\n    \"{$escapedPath}\": {";

            $firstDate = true;

            foreach ($orderedDates as $dateStr) {
                $count = $pathCounts[$dateStr] ?? 0;

                if ($count === 0) {
                    continue;
                }

                if ($firstDate) {
                    $pathBuffer .= "\n";
                    $firstDate = false;
                } else {
                    $pathBuffer .= ",\n";
                }

                $pathBuffer .= "        \"{$dateStr}\": {$count}";
            }

            $pathBuffer .= "\n    }";
            fwrite($out, $pathBuffer);
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
