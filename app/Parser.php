<?php

namespace App;

use App\Commands\Visit;

use const SEEK_CUR;
use const STREAM_IPPROTO_IP;
use const STREAM_PF_UNIX;
use const STREAM_SOCK_STREAM;

final class Parser
{
    private const WORKERS = 10;
    private const BUF_SIZE = 163_840; // 160 KB — L1-cache-friendly reads
    private const PROBE_SIZE = 2_097_152; // 2 MB warm-up scan

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();
        ini_set('memory_limit', '-1');

        $fileSize = filesize($inputPath);

        if ($fileSize === 0) {
            file_put_contents($outputPath, '{}');
            return;
        }

        // Build date lookup table: 2020–2026
        $dateChars = [];
        $dateLabels = [];
        $dateCount = 0;

        for ($y = 21; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => ($y % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $ms = ($m < 10 ? '0' : '') . $m;
                $ymd = ($y % 10) . '-' . $ms . '-';
                for ($d = 1; $d <= $maxD; $d++) {
                    $key = $ymd . ($d < 10 ? '0' : '') . $d;
                    $dateChars[$key] = chr($dateCount & 0xFF) . chr($dateCount >> 8);
                    $dateLabels[$dateCount] = '202' . $key;
                    $dateCount++;
                }
            }
        }

        // Warm-up scan: discover slugs from first 2 MB (preserves file-order for JSON output)
        $probeSize = min(self::PROBE_SIZE, $fileSize);
        $fh = fopen($inputPath, 'rb');
        $sample = fread($fh, $probeSize);
        fclose($fh);

        $slugIds = [];
        $slugLabels = [];
        $slugCount = 0;

        $lastNlW = strrpos($sample, "\n");
        if ($lastNlW !== false) {
            $p = 25; // skip 'https://stitcher.io/blog/'
            while ($p < $lastNlW) {
                $sep = strpos($sample, ',', $p);
                if ($sep === false) break;
                $slug = substr($sample, $p, $sep - $p);
                if (!isset($slugIds[$slug])) {
                    $slugIds[$slug] = $slugCount;
                    $slugLabels[$slugCount] = $slug;
                    $slugCount++;
                }
                $p = $sep + 52;
            }
        }
        unset($sample);

        // Seed any slugs not seen in the warm-up sample
        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, 25);
            if (!isset($slugIds[$slug])) {
                $slugIds[$slug] = $slugCount;
                $slugLabels[$slugCount] = $slug;
                $slugCount++;
            }
        }

        // Compute line-aligned chunk boundaries
        $fh = fopen($inputPath, 'rb');
        $starts = [0];
        for ($i = 1; $i < self::WORKERS; $i++) {
            fseek($fh, (int) ($i * $fileSize / self::WORKERS));
            fgets($fh);
            $starts[$i] = ftell($fh);
        }
        fclose($fh);

        $ends = [];
        for ($i = 0; $i < self::WORKERS - 1; $i++) {
            $ends[$i] = $starts[$i + 1];
        }
        $ends[self::WORKERS - 1] = $fileSize;

        // Fork WORKERS - 1 children via socket pairs; parent handles last chunk
        $pipes = [];
        $childPids = [];

        for ($i = 0; $i < self::WORKERS - 1; $i++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);

            $pid = pcntl_fork();
            if ($pid === -1) {
                throw new \Exception('pcntl_fork() failed');
            }
            if ($pid === 0) {
                fclose($pair[0]); // child: close read-end
                $result = $this->processChunk(
                    $inputPath, $starts[$i], $ends[$i],
                    $slugIds, $dateChars, $slugCount, $dateCount,
                );
                foreach (array_chunk($result, 8192) as $batch) {
                    fwrite($pair[1], pack('v*', ...$batch));
                }
                fclose($pair[1]);
                exit(0);
            }

            fclose($pair[1]); // parent: close write-end so EOF is detectable
            $pipes[] = $pair[0];
            $childPids[] = $pid;
        }

        // Parent processes last chunk while children run concurrently
        $counts = $this->processChunk(
            $inputPath, $starts[self::WORKERS - 1], $ends[self::WORKERS - 1],
            $slugIds, $dateChars, $slugCount, $dateCount,
        );

        // Drain all child sockets concurrently via stream_select
        $buffers = [];
        $open = [];
        $pipeIndex = [];
        foreach ($pipes as $k => $pipe) {
            stream_set_blocking($pipe, false);
            $buffers[$k] = '';
            $rid = (int) $pipe;
            $open[$rid] = $pipe;
            $pipeIndex[$rid] = $k;
        }

        while ($open) {
            $read = array_values($open);
            $w = null;
            $e = null;
            stream_select($read, $w, $e, 5);

            foreach ($read as $pipe) {
                $rid = (int) $pipe;
                $k = $pipeIndex[$rid];
                $data = fread($pipe, 131072);
                if ($data !== '' && $data !== false) {
                    $buffers[$k] .= $data;
                }
                if (feof($pipe)) {
                    $raw = $buffers[$k];
                    fclose($pipe);
                    unset($open[$rid], $pipeIndex[$rid], $buffers[$k]);

                    if ($raw === '') {
                        continue;
                    }

                    $j = 0;
                    foreach (unpack('v*', $raw) as $v) {
                        $counts[$j++] += $v;
                    }
                }
            }
        }

        foreach ($childPids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        // Write JSON
        $datePrefixes = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "' . $dateLabels[$d] . '": ';
        }

        $escapedPaths = [];
        for ($s = 0; $s < $slugCount; $s++) {
            $escapedPaths[$s] = '"\\/blog\\/' . str_replace('/', '\\/', $slugLabels[$s]) . '"';
        }

        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);

        fwrite($out, '{');
        $firstSlug = true;

        for ($s = 0; $s < $slugCount; $s++) {
            $base = $s * $dateCount;
            $body = '';
            $sep = '';

            for ($d = 0; $d < $dateCount; $d++) {
                $n = $counts[$base + $d];
                if ($n === 0) {
                    continue;
                }
                $body .= $sep . $datePrefixes[$d] . $n;
                $sep = ",\n";
            }

            if ($body === '') {
                continue;
            }

            fwrite($out, ($firstSlug ? '' : ',') . "\n    " . $escapedPaths[$s] . ": {\n" . $body . "\n    }");
            $firstSlug = false;
        }

        fwrite($out, "\n}");
        fclose($out);
    }

    private function processChunk(
        string $inputPath,
        int $start,
        int $end,
        array $slugIds,
        array $dateChars,
        int $slugCount,
        int $dateCount,
    ): array {
        $buckets = array_fill(0, $slugCount, '');
        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);
        fseek($fh, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $chunk = fread($fh, min(self::BUF_SIZE, $remaining));
            $chunkLen = strlen($chunk);
            if ($chunkLen === 0) {
                break;
            }
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) {
                break;
            }

            // fseek tail trick: rewind past partial line so next read starts clean
            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($fh, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $p = 25; // slug starts 25 bytes into each line
            $fence = $lastNl - 720; // 6 × max-line guard for unrolled loop

            // Hot loop, unrolled 6×
            while ($p < $fence) {
                $sep = strpos($chunk, ',', $p);
                $buckets[$slugIds[substr($chunk, $p, $sep - $p)]] .= $dateChars[substr($chunk, $sep + 4, 7)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$slugIds[substr($chunk, $p, $sep - $p)]] .= $dateChars[substr($chunk, $sep + 4, 7)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$slugIds[substr($chunk, $p, $sep - $p)]] .= $dateChars[substr($chunk, $sep + 4, 7)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$slugIds[substr($chunk, $p, $sep - $p)]] .= $dateChars[substr($chunk, $sep + 4, 7)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$slugIds[substr($chunk, $p, $sep - $p)]] .= $dateChars[substr($chunk, $sep + 4, 7)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$slugIds[substr($chunk, $p, $sep - $p)]] .= $dateChars[substr($chunk, $sep + 4, 7)];
                $p = $sep + 52;
            }

            // Tail: handle remaining lines near end of buffer
            while ($p < $lastNl) {
                $sep = strpos($chunk, ',', $p);
                if ($sep === false || $sep >= $lastNl) {
                    break;
                }
                $buckets[$slugIds[substr($chunk, $p, $sep - $p)]] .= $dateChars[substr($chunk, $sep + 4, 7)];
                $p = $sep + 52;
            }
        }

        fclose($fh);

        // Bulk-count via array_count_values (pure C)
        $counts = array_fill(0, $slugCount * $dateCount, 0);
        for ($s = 0; $s < $slugCount; $s++) {
            if ($buckets[$s] === '') continue;
            $base = $s * $dateCount;
            foreach (array_count_values(unpack('v*', $buckets[$s])) as $dateId => $cnt) {
                $counts[$base + $dateId] += $cnt;
            }
        }

        return $counts;
    }
}
