<?php

declare(strict_types=1);

namespace App;

final class Parser
{
    private const int WORKERS = 6;
    private const int BUFFER_SIZE = 8 * 1024 * 1024;
    private const int URL_PREFIX_LEN = 19;
    private const int DATE_LEN = 10;
    private const int TIMESTAMP_LEN = 25;
    private const int MIN_LINE_SUFFIX = 26;

    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);
        $workerCount = self::WORKERS;

        $splitPoints = [0];
        $handle = fopen($inputPath, 'rb');
        for ($i = 1; $i < $workerCount; $i++) {
            $offset = (int)($fileSize * $i / $workerCount);
            fseek($handle, $offset);
            fgets($handle);
            $splitPoints[] = ftell($handle);
        }
        fclose($handle);
        $splitPoints[] = $fileSize;

        $children = [];
        for ($i = 1; $i < $workerCount; $i++) {
            $pipes = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            $pid = pcntl_fork();

            if ($pid === 0) {
                fclose($pipes[0]);
                [$paths, $dates, $counts] = $this->processChunk($inputPath, $splitPoints[$i], $splitPoints[$i + 1]);
                $packed = $this->packWorkerData($paths, $dates, $counts);
                fwrite($pipes[1], $packed);
                fclose($pipes[1]);
                exit(0);
            }

            fclose($pipes[1]);
            $children[] = ['pid' => $pid, 'pipe' => $pipes[0]];
        }

        [$paths, $dates, $counts] = $this->processChunk($inputPath, $splitPoints[0], $splitPoints[1]);

        $raw = [];
        $allDates = [];
        foreach ($counts as $pathId => $dateCounts) {
            $path = $paths[$pathId];
            foreach ($dateCounts as $dateId => $count) {
                if ($count > 0) {
                    $date = $dates[$dateId];
                    $raw[$path][$date] = $count;
                    $allDates[$date] = true;
                }
            }
        }
        unset($paths, $dates, $counts);

        foreach ($children as $child) {
            $packed = stream_get_contents($child['pipe']);
            fclose($child['pipe']);
            pcntl_waitpid($child['pid'], $status);

            [$childPaths, $childDates, $childCounts] = $this->unpackWorkerData($packed);
            unset($packed);

            foreach ($childCounts as $pathId => $dateCounts) {
                $path = $childPaths[$pathId];
                foreach ($dateCounts as $dateId => $count) {
                    if ($count > 0) {
                        $date = $childDates[$dateId];
                        $raw[$path][$date] = ($raw[$path][$date] ?? 0) + $count;
                        $allDates[$date] = true;
                    }
                }
            }
            unset($childPaths, $childDates, $childCounts);
        }

        $sortedDates = array_keys($allDates);
        sort($sortedDates);
        unset($allDates);

        $data = [];
        foreach ($raw as $path => $dateCounts) {
            foreach ($sortedDates as $date) {
                if (isset($dateCounts[$date])) {
                    $data[$path][$date] = $dateCounts[$date];
                }
            }
        }
        unset($raw);

        file_put_contents($outputPath, json_encode($data, JSON_PRETTY_PRINT));
    }

    private function packWorkerData(array $paths, array $dates, array $counts): string
    {
        $dateCount = count($dates);
        $pathCount = count($paths);

        $packed = pack('VV', $pathCount, $dateCount);

        foreach ($paths as $path) {
            $packed .= pack('V', strlen($path)) . $path;
        }

        foreach ($dates as $date) {
            $packed .= $date;
        }

        foreach ($counts as $row) {
            $packed .= pack('V*', ...$row);
        }

        return $packed;
    }

    private function unpackWorkerData(string $packed): array
    {
        $offset = 0;

        $header = unpack('VpathCount/VdateCount', $packed, $offset);
        $pathCount = $header['pathCount'];
        $dateCount = $header['dateCount'];
        $offset += 8;

        $paths = [];
        for ($i = 0; $i < $pathCount; $i++) {
            $len = unpack('V', $packed, $offset)[1];
            $offset += 4;
            $paths[$i] = substr($packed, $offset, $len);
            $offset += $len;
        }

        $dates = [];
        for ($i = 0; $i < $dateCount; $i++) {
            $dates[$i] = substr($packed, $offset, 10);
            $offset += 10;
        }

        $rowSize = $dateCount * 4;
        $counts = [];
        for ($p = 0; $p < $pathCount; $p++) {
            $counts[$p] = array_values(unpack("V{$dateCount}", substr($packed, $offset, $rowSize)));
            $offset += $rowSize;
        }

        return [$paths, $dates, $counts];
    }

    private function processChunk(string $inputPath, int $start, int $end): array
    {
        $pathIds = [];
        $paths = [];
        $dateIds = [];
        $dates = [];
        $counts = [];

        $prefixLen = self::URL_PREFIX_LEN;
        $dateLen = self::DATE_LEN;
        $timestampLen = self::TIMESTAMP_LEN;
        $minSuffix = self::MIN_LINE_SUFFIX;
        $bufferSize = self::BUFFER_SIZE;

        $leftover = '';

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $readSize = min($bufferSize, $remaining);
            $chunk = fread($handle, $readSize);
            if ($chunk === false || $chunk === '') {
                break;
            }
            $remaining -= strlen($chunk);
            $chunkLen = strlen($chunk);
            $pos = 0;

            if ($leftover !== '') {
                $firstNl = strpos($chunk, "\n");
                if ($firstNl === false) {
                    $leftover .= $chunk;
                    continue;
                }
                $line = $leftover . substr($chunk, 0, $firstNl);
                $lineLen = strlen($line);
                $leftover = '';
                if ($lineLen > $prefixLen + $minSuffix) {
                    $path = substr($line, $prefixLen, $lineLen - $prefixLen - $minSuffix);
                    $date = substr($line, $lineLen - $timestampLen, $dateLen);

                    $pathId = $pathIds[$path] ?? -1;
                    if ($pathId === -1) {
                        $pathId = count($paths);
                        $pathIds[$path] = $pathId;
                        $paths[$pathId] = $path;
                        $counts[$pathId] = array_fill(0, count($dates), 0);
                    }

                    $dateId = $dateIds[$date] ?? -1;
                    if ($dateId === -1) {
                        $dateId = count($dates);
                        $dateIds[$date] = $dateId;
                        $dates[$dateId] = $date;
                        foreach ($counts as &$pathCounts) {
                            $pathCounts[] = 0;
                        }
                        unset($pathCounts);
                    }

                    $counts[$pathId][$dateId]++;
                }
                $pos = $firstNl + 1;
            }

            $lastNewline = strrpos($chunk, "\n");
            if ($lastNewline === false) {
                $leftover = $chunk;
                continue;
            }

            if ($lastNewline < $chunkLen - 1) {
                $leftover = substr($chunk, $lastNewline + 1);
            }

            while ($pos < $lastNewline) {
                $nlPos = strpos($chunk, "\n", $pos);

                $path = substr($chunk, $pos + $prefixLen, $nlPos - $pos - $prefixLen - $minSuffix);
                $date = substr($chunk, $nlPos - $timestampLen, $dateLen);

                $pathId = $pathIds[$path] ?? -1;
                if ($pathId === -1) {
                    $pathId = count($paths);
                    $pathIds[$path] = $pathId;
                    $paths[$pathId] = $path;
                    $counts[$pathId] = array_fill(0, count($dates), 0);
                }

                $dateId = $dateIds[$date] ?? -1;
                if ($dateId === -1) {
                    $dateId = count($dates);
                    $dateIds[$date] = $dateId;
                    $dates[$dateId] = $date;
                    foreach ($counts as &$pathCounts) {
                        $pathCounts[] = 0;
                    }
                    unset($pathCounts);
                }

                $counts[$pathId][$dateId]++;

                $pos = $nlPos + 1;
            }
        }

        if ($leftover !== '') {
            $len = strlen($leftover);
            if ($len > $prefixLen + $minSuffix) {
                $path = substr($leftover, $prefixLen, $len - $prefixLen - $minSuffix);
                $date = substr($leftover, $len - $timestampLen, $dateLen);

                $pathId = $pathIds[$path] ?? -1;
                if ($pathId === -1) {
                    $pathId = count($paths);
                    $paths[$pathId] = $path;
                    $counts[$pathId] = array_fill(0, count($dates), 0);
                }

                $dateId = $dateIds[$date] ?? -1;
                if ($dateId === -1) {
                    $dateId = count($dates);
                    $dates[$dateId] = $date;
                    foreach ($counts as &$pathCounts) {
                        $pathCounts[] = 0;
                    }
                    unset($pathCounts);
                }

                $counts[$pathId][$dateId]++;
            }
        }

        fclose($handle);

        return [$paths, $dates, $counts];
    }
}
