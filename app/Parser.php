<?php

namespace App;

\gc_disable();

final class Parser
{
    public static function parse(string $inputPath, string $outputPath): void
    {
        $numWorkers = 14;
        $chunkSize = 262144;

        $slugToIdx = [];
        $slugCount = 0;
        $fh = \fopen($inputPath, 'rb');
        $sample = \fread($fh, 262144);

        $sampleLen = \strlen($sample);
        $sPos = 0;
        while ($sPos + 29 < $sampleLen) {
            $c = \strpos($sample, ',', $sPos + 29);
            if ($c === false) break;
            $slug = \substr($sample, $sPos + 25, $c - $sPos - 25);
            if (!isset($slugToIdx[$slug])) {
                $slugToIdx[$slug] = $slugCount++;
                if ($slugCount >= 268) break;
            }
            $sPos = $c + 27;
        }
        $slugOrderList = \array_keys($slugToIdx);
        unset($sample);

        $dateToId = [];
        $dateJsonPrefix = [];
        $dateCount = 0;
        $daysInMonth = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        for ($year = 2021; $year <= 2026; $year++) {
            $isLeap = ($year % 4 === 0 && ($year % 100 !== 0 || $year % 400 === 0));
            for ($month = 1; $month <= 12; $month++) {
                $days = $daysInMonth[$month];
                if ($month === 2 && $isLeap) $days = 29;
                for ($day = 1; $day <= $days; $day++) {
                    $yy = $year - 2000;
                    $dateStr8 = ($yy < 10 ? '0' : '') . $yy . '-' . ($month < 10 ? '0' : '') . $month . '-' . ($day < 10 ? '0' : '') . $day;
                    $dateToId[$dateStr8] = \chr($dateCount & 0xFF) . \chr($dateCount >> 8);
                    $dateJsonPrefix[$dateCount] = '        "' . \sprintf('%04d-%02d-%02d', $year, $month, $day) . '": ';
                    $dateCount++;
                }
            }
        }

        $fileSize = \filesize($inputPath);
        $boundaries = [0];
        for ($w = 1; $w < $numWorkers; $w++) {
            \fseek($fh, (int)($fileSize * $w / $numWorkers));
            \fgets($fh);
            $boundaries[] = \ftell($fh);
        }
        $boundaries[] = $fileSize;
        \fclose($fh);

        $pidToWorker = [];
        $flatSize = $slugCount * $dateCount;
        $dataSize = $flatSize << 1;
        $workerSockets = [];

        for ($w = 0; $w < $numWorkers; $w++) {
            \socket_create_pair(AF_UNIX, SOCK_STREAM, 0, $pair);
            \socket_set_option($pair[1], SOL_SOCKET, SO_SNDBUF, $dataSize + 4096);
            \socket_set_option($pair[0], SOL_SOCKET, SO_RCVBUF, $dataSize + 4096);

            $pid = \pcntl_fork();
            if ($pid === 0) {
                \socket_close($pair[0]);
                $buckets = \array_fill_keys($slugOrderList, '');
                $fh = \fopen($inputPath, 'rb');
                \stream_set_read_buffer($fh, 0);
                $start = $boundaries[$w];
                $end = $boundaries[$w + 1];
                \fseek($fh, $start);
                $remaining = $end - $start;

                do {
                    $toRead = $remaining > $chunkSize ? $chunkSize : $remaining;
                    $raw = \fread($fh, $toRead);
                    if ($raw === false || $raw === '') break;
                    $rawLen = \strlen($raw);
                    $remaining -= $rawLen;

                    $lastNl = \strrpos($raw, "\n");
                    if ($lastNl === false) break;

                    $tail = $rawLen - $lastNl - 1;
                    if ($tail > 0) {
                        \fseek($fh, -$tail, SEEK_CUR);
                        $remaining += $tail;
                    }

                    $i = 25;
                    $fence = $lastNl - 750;

                    if ($i < $fence) {
                        do {
                            $c = \strpos($raw, ',', $i);
                            $buckets[\substr($raw, $i, $c - $i)] .= $dateToId[\substr($raw, $c + 3, 8)];
                            $i = $c + 52;

                            $c = \strpos($raw, ',', $i);
                            $buckets[\substr($raw, $i, $c - $i)] .= $dateToId[\substr($raw, $c + 3, 8)];
                            $i = $c + 52;

                            $c = \strpos($raw, ',', $i);
                            $buckets[\substr($raw, $i, $c - $i)] .= $dateToId[\substr($raw, $c + 3, 8)];
                            $i = $c + 52;

                            $c = \strpos($raw, ',', $i);
                            $buckets[\substr($raw, $i, $c - $i)] .= $dateToId[\substr($raw, $c + 3, 8)];
                            $i = $c + 52;

                            $c = \strpos($raw, ',', $i);
                            $buckets[\substr($raw, $i, $c - $i)] .= $dateToId[\substr($raw, $c + 3, 8)];
                            $i = $c + 52;

                            $c = \strpos($raw, ',', $i);
                            $buckets[\substr($raw, $i, $c - $i)] .= $dateToId[\substr($raw, $c + 3, 8)];
                            $i = $c + 52;
                        } while ($i < $fence);
                    }

                    if ($i < $lastNl) {
                        do {
                            $c = \strpos($raw, ',', $i);
                            if ($c === false || $c > $lastNl) break;
                            $buckets[\substr($raw, $i, $c - $i)] .= $dateToId[\substr($raw, $c + 3, 8)];
                            $i = $c + 52;
                        } while ($i < $lastNl);
                    }
                } while ($remaining > 0);

                $counts = \array_fill(0, $flatSize, 0);
                foreach ($buckets as $slug => $packed) {
                    if ($packed === '') continue;
                    $base = $slugToIdx[$slug] * $dateCount;
                    foreach (\array_count_values(\unpack('v*', $packed)) as $dId => $cnt) {
                        $counts[$base + $dId] = $cnt;
                    }
                }

                $packed = \pack('v*', ...$counts);
                \socket_write($pair[1], $packed, $dataSize);
                \socket_close($pair[1]);
                \posix_kill(\posix_getpid(), 9);
            }
            \socket_close($pair[1]);
            $workerSockets[$pid] = $pair[0];
            $pidToWorker[$pid] = $w;
        }

        $merged = null;
        $drained = 0;

        do {
            $pid = \pcntl_waitpid(-1, $status);
            if ($pid <= 0) continue;

            $sock = $workerSockets[$pid];
            $data = '';
            $rem = $dataSize;
            while ($rem > 0) {
                $chunk = \socket_read($sock, $rem);
                if ($chunk === false || $chunk === '') break;
                $rem -= \strlen($chunk);
                $data .= $chunk;
            }
            \socket_close($sock);

            if ($merged === null) {
                $merged = \unpack('v*', $data);
            } else {
                $child = \unpack('v*', $data);
                for ($j = 1; $j <= $flatSize; $j++) {
                    $merged[$j] += $child[$j];
                }
                unset($child);
            }
            unset($data);

            $drained++;
        } while ($drained < $numWorkers);

        $fhOut = \fopen($outputPath, 'wb');
        \stream_set_write_buffer($fhOut, 1048576);
        \fwrite($fhOut, "{\n");
        $needSep = false;

        foreach ($slugOrderList as $slug) {
            $base = $slugToIdx[$slug] * $dateCount + 1;
            $dateParts = [];
            for ($d = 0; $d < $dateCount; $d++) {
                $v = $merged[$base + $d];
                if ($v > 0) {
                    $dateParts[] = $dateJsonPrefix[$d] . $v;
                }
            }
            if ($dateParts === []) continue;

            if ($needSep) \fwrite($fhOut, ",\n");
            \fwrite($fhOut, '    "\/blog\/' . $slug . '": {' . "\n" . \implode(",\n", $dateParts) . "\n    }");
            $needSep = true;
        }

        \fwrite($fhOut, "\n}");
        \fclose($fhOut);
    }
}
