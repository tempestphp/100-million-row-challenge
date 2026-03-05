<?php

namespace App;

use App\Commands\Visit;

use const SEEK_CUR;
use const STREAM_PF_UNIX;
use const STREAM_SOCK_STREAM;
use const STREAM_IPPROTO_IP;

final class Parser
{
    private const WORKERS = 12;
    private const BUF_SIZE = 524288; // 512 KB — fewer fread syscalls
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

        // Build date lookup table: 2021–2026
        $dateIds = [];
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
                $ymd = $y . '-' . $ms . '-';
                for ($d = 1; $d <= $maxD; $d++) {
                    $key = $ymd . ($d < 10 ? '0' : '') . $d;
                    $dateIds[$key] = $dateCount;
                    $dateLabels[$dateCount] = '20' . $key;
                    $dateCount++;
                }
            }
        }

        // Byte-increment lookup: chr(i) → chr(i+1)
        $next = [];
        for ($i = 0; $i < 255; $i++) {
            $next[chr($i)] = chr($i + 1);
        }

        // Warm-up scan: discover slugs from first 2 MB (preserves file-order for JSON output)
        $probeSize = min(self::PROBE_SIZE, $fileSize);
        $fh = fopen($inputPath, 'rb');
        $sample = fread($fh, $probeSize);
        fclose($fh);

        $slugBaseMap = [];
        $slugLabels = [];
        $slugCount = 0;

        $lastNlW = strrpos($sample, "\n");
        if ($lastNlW !== false) {
            $p = 25;
            while ($p < $lastNlW) {
                $sep = strpos($sample, ',', $p);
                if ($sep === false) break;
                $slug = substr($sample, $p, $sep - $p);
                if (!isset($slugBaseMap[$slug])) {
                    $slugBaseMap[$slug] = $slugCount * $dateCount;
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
            if (!isset($slugBaseMap[$slug])) {
                $slugBaseMap[$slug] = $slugCount * $dateCount;
                $slugLabels[$slugCount] = $slug;
                $slugCount++;
            }
        }

        $outputSize = $slugCount * $dateCount;

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

        // Fork WORKERS - 1 children via Unix sockets; parent handles last chunk
        $sockets = [];

        for ($i = 0; $i < self::WORKERS - 1; $i++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_chunk_size($pair[0], $outputSize);
            stream_set_chunk_size($pair[1], $outputSize);
            $pid = pcntl_fork();
            if ($pid === -1) {
                throw new \Exception('pcntl_fork() failed');
            }
            if ($pid === 0) {
                fclose($pair[0]);
                $result = $this->processChunk(
                    $inputPath, $starts[$i], $ends[$i],
                    $slugBaseMap, $dateIds, $next, $outputSize,
                );
                fwrite($pair[1], $result);
                fclose($pair[1]);
                exit(0);
            }
            fclose($pair[1]);
            $sockets[$i] = $pair[0];
        }

        // Parent processes last chunk while children run concurrently
        $parentOutput = $this->processChunk(
            $inputPath, $starts[self::WORKERS - 1], $ends[self::WORKERS - 1],
            $slugBaseMap, $dateIds, $next, $outputSize,
        );
        $counts = array_fill(0, $outputSize, 0);
        $j = 0;
        foreach (unpack('C*', $parentOutput) as $v) {
            $counts[$j++] = $v;
        }
        unset($parentOutput);

        // Drain children as they finish via stream_select
        while ($sockets !== []) {
            $read = $sockets;
            $write = [];
            $except = [];
            stream_select($read, $write, $except, 5);
            foreach ($read as $key => $socket) {
                $data = '';
                while (!feof($socket)) {
                    $data .= fread($socket, $outputSize);
                }
                fclose($socket);
                unset($sockets[$key]);
                $j = 0;
                foreach (unpack('C*', $data) as $v) {
                    $counts[$j++] += $v;
                }
            }
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
        array $slugBaseMap,
        array $dateIds,
        array $next,
        int $outputSize,
    ): string {
        $output = str_repeat(chr(0), $outputSize);
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

            $p = 25; // slug start (past 25-char URL prefix)
            $fence = $lastNl - 720; // 6 × ~120-byte max-line guard

            // Hot loop, unrolled 6×
            while ($p < $fence) {
                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
            }

            // Tail: handle remaining lines near end of buffer
            while ($p < $lastNl) {
                $sep = strpos($chunk, ',', $p);
                if ($sep === false || $sep >= $lastNl) break;
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
            }
        }

        fclose($fh);

        return $output;
    }
}
