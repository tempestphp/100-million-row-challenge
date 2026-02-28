<?php

namespace App;

\gc_disable();

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $numWorkers = 10;
        $chunkSize  = 131072;

        $slugToIdx = [];
        $slugCount = 0;
        $fh = \fopen($inputPath, 'rb');
        $sample = \fread($fh, 524288);
        \fclose($fh);

        $sampleLen = \strlen($sample);
        $sPos = 0;
        while ($sPos + 29 < $sampleLen) {
            $c = \strpos($sample, ',', $sPos + 29);
            if ($c === false) break;
            $slug = \substr($sample, $sPos + 25, $c - $sPos - 25);
            if (!isset($slugToIdx[$slug])) {
                $slugToIdx[$slug] = $slugCount++;
            }
            $sPos = $c + 27;
        }
        $slugOrderList = \array_keys($slugToIdx);
        unset($sample);

        $dateToId = [];
        $idToDate = [];
        $dateId = 0;
        $daysInMonth = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        for ($year = 2021; $year <= 2026; $year++) {
            $isLeap = ($year % 4 === 0 && ($year % 100 !== 0 || $year % 400 === 0));
            for ($month = 1; $month <= 12; $month++) {
                $days = $daysInMonth[$month];
                if ($month === 2 && $isLeap) $days = 29;
                for ($day = 1; $day <= $days; $day++) {
                    $yy = $year - 2000;
                    $dateStr8 = ($yy < 10 ? '0' : '') . $yy . '-' . ($month < 10 ? '0' : '') . $month . '-' . ($day < 10 ? '0' : '') . $day;
                    $dateToId[$dateStr8] = \chr($dateId & 0xFF) . \chr($dateId >> 8);
                    $idToDate[$dateId] = \sprintf('%04d-%02d-%02d', $year, $month, $day);
                    $dateId++;
                }
            }
        }

        $dateJsonPrefix = [];
        foreach ($idToDate as $dId => $dateStr) {
            $dateJsonPrefix[$dId] = '        "' . $dateStr . '": ';
        }

        $fileSize = \filesize($inputPath);
        $boundaries = [0];
        $fh = \fopen($inputPath, 'rb');
        for ($w = 1; $w < $numWorkers; $w++) {
            \fseek($fh, (int)($fileSize * $w / $numWorkers));
            \fgets($fh);
            $boundaries[] = \ftell($fh);
        }
        $boundaries[] = $fileSize;
        \fclose($fh);

        $tmpDir = \sys_get_temp_dir();
        $pidToWorker = [];

        for ($w = 0; $w < $numWorkers; $w++) {
            $pid = \pcntl_fork();
            if ($pid === 0) {
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

                    $pos = 0;
                    $fence = $lastNl - 600;

                    if ($pos < $fence) {
                        do {
                            $c = \strpos($raw, ',', $pos + 29);
                            $buckets[\substr($raw, $pos + 25, $c - $pos - 25)] .= $dateToId[\substr($raw, $c + 3, 8)];
                            $pos = $c + 27;

                            $c = \strpos($raw, ',', $pos + 29);
                            $buckets[\substr($raw, $pos + 25, $c - $pos - 25)] .= $dateToId[\substr($raw, $c + 3, 8)];
                            $pos = $c + 27;

                            $c = \strpos($raw, ',', $pos + 29);
                            $buckets[\substr($raw, $pos + 25, $c - $pos - 25)] .= $dateToId[\substr($raw, $c + 3, 8)];
                            $pos = $c + 27;

                            $c = \strpos($raw, ',', $pos + 29);
                            $buckets[\substr($raw, $pos + 25, $c - $pos - 25)] .= $dateToId[\substr($raw, $c + 3, 8)];
                            $pos = $c + 27;

                            $c = \strpos($raw, ',', $pos + 29);
                            $buckets[\substr($raw, $pos + 25, $c - $pos - 25)] .= $dateToId[\substr($raw, $c + 3, 8)];
                            $pos = $c + 27;

                            $c = \strpos($raw, ',', $pos + 29);
                            $buckets[\substr($raw, $pos + 25, $c - $pos - 25)] .= $dateToId[\substr($raw, $c + 3, 8)];
                            $pos = $c + 27;
                        } while ($pos < $fence);
                    }

                    if ($pos < $lastNl) {
                        do {
                            $c = \strpos($raw, ',', $pos + 29);
                            if ($c === false || $c > $lastNl) break;
                            $buckets[\substr($raw, $pos + 25, $c - $pos - 25)] .= $dateToId[\substr($raw, $c + 3, 8)];
                            $pos = $c + 27;
                        } while ($pos < $lastNl);
                    }
                } while ($remaining > 0);
                \fclose($fh);

                $out = '';
                foreach ($buckets as $slug => $packed) {
                    if ($packed === '') continue;
                    $out .= \pack('vV', $slugToIdx[$slug], \strlen($packed)) . $packed;
                }
                unset($buckets);

                \file_put_contents($tmpDir . '/parser_w' . $w, $out);
                \posix_kill(\posix_getpid(), 9);
                exit(0);
            }
            $pidToWorker[$pid] = $w;
        }

        $mergedBuckets = \array_fill_keys($slugOrderList, '');
        $drained = 0;

        do {
            $pid = \pcntl_waitpid(-1, $status);
            if ($pid <= 0) {
                continue;
            }

            $w = $pidToWorker[$pid];
            $tmpFile = $tmpDir . '/parser_w' . $w;
            $data = \file_get_contents($tmpFile);
            \unlink($tmpFile);

            $offset = 0;
            $dataLen = \strlen($data);
            while ($offset < $dataLen) {
                $slugIdx = \ord($data[$offset]) | (\ord($data[$offset + 1]) << 8);
                $bucketLen = \ord($data[$offset + 2]) | (\ord($data[$offset + 3]) << 8) | (\ord($data[$offset + 4]) << 16) | (\ord($data[$offset + 5]) << 24);
                $offset += 6;
                $mergedBuckets[$slugOrderList[$slugIdx]] .= \substr($data, $offset, $bucketLen);
                $offset += $bucketLen;
            }
            unset($data);

            $drained++;
        } while ($drained < $numWorkers);

        $numCounters = 8;
        $numSlugs = \count($slugOrderList);
        $slugsPerCounter = (int)\ceil($numSlugs / $numCounters);

        $slugJsonHeaders = [];
        foreach ($slugOrderList as $slug) {
            $slugJsonHeaders[$slug] = '    "\/blog\/' . $slug . '": {' . "\n";
        }

        $countPipes = [];
        for ($c = 0; $c < $numCounters; $c++) {
            \socket_create_pair(AF_UNIX, SOCK_STREAM, 0, $rawPair);
            \socket_set_option($rawPair[0], SOL_SOCKET, SO_RCVBUF, 2097152);
            \socket_set_option($rawPair[1], SOL_SOCKET, SO_SNDBUF, 2097152);
            $countPipes[$c] = [\socket_export_stream($rawPair[0]), \socket_export_stream($rawPair[1])];
        }

        $countPids = [];
        for ($c = 0; $c < $numCounters; $c++) {
            $pid = \pcntl_fork();
            if ($pid === 0) {
                for ($i = 0; $i < $numCounters; $i++) {
                    \fclose($countPipes[$i][0]);
                    if ($i !== $c) \fclose($countPipes[$i][1]);
                }

                $myStart = $c * $slugsPerCounter;
                $myEnd = \min(($c + 1) * $slugsPerCounter, $numSlugs);

                $slugParts = [];

                for ($s = $myStart; $s < $myEnd; $s++) {
                    $slug = $slugOrderList[$s];
                    $packed = $mergedBuckets[$slug];
                    if ($packed === '') continue;

                    $counts = \array_count_values(\unpack('v*', $packed));
                    \ksort($counts);

                    $dateParts = [];
                    foreach ($counts as $dId => $count) {
                        $dateParts[] = $dateJsonPrefix[$dId] . $count;
                    }
                    $slugParts[] = $slugJsonHeaders[$slug] . \implode(",\n", $dateParts) . "\n    }";
                }

                $fragment = \implode(",\n", $slugParts);

                $sock = $countPipes[$c][1];
                $len = \strlen($fragment);
                $written = 0;
                while ($written < $len) {
                    $n = \fwrite($sock, \substr($fragment, $written, 131072));
                    if ($n === false) break;
                    $written += $n;
                }
                \fclose($sock);
                \posix_kill(\posix_getpid(), 9);
                exit(0);
            }
            $countPids[] = $pid;
        }

        for ($c = 0; $c < $numCounters; $c++) {
            \fclose($countPipes[$c][1]);
        }
        unset($mergedBuckets);

        $fhOut = \fopen($outputPath, 'wb');
        \fwrite($fhOut, "{\n");
        $needSep = false;

        for ($c = 0; $c < $numCounters; $c++) {
            $fragment = \stream_get_contents($countPipes[$c][0]);
            \fclose($countPipes[$c][0]);

            if ($fragment !== '') {
                if ($needSep) \fwrite($fhOut, ",\n");
                \fwrite($fhOut, $fragment);
                $needSep = true;
            }
        }

        \fwrite($fhOut, "\n}");
        \fclose($fhOut);

        foreach ($countPids as $pid) {
            \pcntl_waitpid($pid, $status);
        }
    }
}
