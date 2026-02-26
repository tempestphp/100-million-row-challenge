<?php

declare(strict_types=1);

namespace App;

use App\Commands\Visit;

use function array_fill, count, fgets, filesize, fopen, fread, fseek, ftell,
    fwrite, gc_disable, pack, pcntl_fork, pcntl_waitpid, str_replace,
    stream_set_read_buffer, stream_set_write_buffer, strlen, strpos, strrpos,
    substr, unpack, shmop_open, shmop_write, shmop_read, shmop_delete, ftok, chr;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();
        $totalBytes = filesize($inputPath);

        $urlIdx = $ptr = $dayIdx = 0;
        $dayMap = $dayLabels = [];

        for ($yr = 20; $yr <= 26; $yr++) {
            for ($mo = 1; $mo <= 12; $mo++) {
                $maxDay = match ($mo) {
                    2 => ($yr + 2000) % 4 === 0 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $prefix = $yr . "-" . ($mo < 10 ? "0" : "") . $mo . "-";
                
                for ($d = 1; $d <= $maxDay; $d++) {
                    $dayMap[$prefix . (($d < 10 ? "0" : "") . $d)] = $dayIdx;
                    $dayLabels[$dayIdx] = $prefix . (($d < 10 ? "0" : "") . $d);
                    $dayIdx++;
                }
            }
        }

        $stream = fopen($inputPath, "rb");
        stream_set_read_buffer($stream, 0);
        
        $buf = fread($stream, $totalBytes > 2_097_152 ? 2_097_152 : $totalBytes);

        $urlMap = $urlDict = [];
        $finalNl = strrpos($buf, "\n");

        while ($ptr < $finalNl) {
            $lf = strpos($buf, "\n", $ptr + 52);
            if ($lf === false) break;

            $uri = substr($buf, $ptr + 25, $lf - $ptr - 51);
            if (!isset($urlMap[$uri])) {
                $urlMap[$uri] = $urlIdx * $dayIdx;
                $urlDict[$urlIdx] = $uri;
                $urlIdx++;
            }
            $ptr = $lf + 1;
        }
        unset($buf);

        foreach (Visit::all() as $visit) {
            $uri = substr($visit->uri, 25);
            if (!isset($urlMap[$uri])) {
                $urlMap[$uri] = $urlIdx * $dayIdx;
                $urlDict[$urlIdx] = $uri;
                $urlIdx++;
            }
        }

        $offsets = [0];
        for ($i = 1; $i < 4; $i++) {
            fseek($stream, (int) (($totalBytes * $i) >> 2));
            fgets($stream);
            $offsets[] = ftell($stream);
        }
        $offsets[] = $totalBytes;
        fclose($stream);

        $pids = [];
        $ramAlloc = $urlIdx * $dayIdx * 4;

        for ($w = 0; $w < 3; $w++) {
            $token = ftok(__FILE__, chr($w + 1));
            $child = pcntl_fork();

            if ($child === 0) {
                $grid = array_fill(0, $urlIdx * $dayIdx, 0);
                $fd = fopen($inputPath, "rb");
                stream_set_read_buffer($fd, 0);
                fseek($fd, $offsets[$w]);
                
                $left = $offsets[$w + 1] - $offsets[$w];

                while ($left > 0) {
                    $block = fread($fd, $left > 8_388_608 ? 8_388_608 : $left);
                    $len = strlen($block);
                    if ($len === 0) break;
                    $left -= $len;

                    $lastReturn = strrpos($block, "\n");
                    if ($lastReturn === false) {
                        fseek($fd, -$len, 1);
                        break;
                    }

                    if ($len - $lastReturn - 1 > 0) {
                        fseek($fd, -($len - $lastReturn - 1), 1);
                        $left += $len - $lastReturn - 1;
                    }

                    $p = 0;
                    while ($p < $lastReturn) {
                        $n1 = strpos($block, "\n", $p + 52);
                        ++$grid[$urlMap[substr($block, $p + 25, $n1 - $p - 51)] + $dayMap[substr($block, $n1 - 23, 8)]];
                        if ($n1 >= $lastReturn) { $p = $n1 + 1; break; }

                        $n2 = strpos($block, "\n", $n1 + 53);
                        ++$grid[$urlMap[substr($block, $n1 + 26, $n2 - $n1 - 52)] + $dayMap[substr($block, $n2 - 23, 8)]];
                        if ($n2 >= $lastReturn) { $p = $n2 + 1; break; }

                        $n3 = strpos($block, "\n", $n2 + 53);
                        ++$grid[$urlMap[substr($block, $n2 + 26, $n3 - $n2 - 52)] + $dayMap[substr($block, $n3 - 23, 8)]];
                        if ($n3 >= $lastReturn) { $p = $n3 + 1; break; }

                        $n4 = strpos($block, "\n", $n3 + 53);
                        ++$grid[$urlMap[substr($block, $n3 + 26, $n4 - $n3 - 52)] + $dayMap[substr($block, $n4 - 23, 8)]];
                        $p = $n4 + 1;
                    }
                }

                $shm = shmop_open($token, "c", 0644, $ramAlloc);
                shmop_write($shm, pack("V*", ...$grid), 0);
                exit(0);
            }
            $pids[] = [$child, $token];
        }

        $masterGrid = array_fill(0, $urlIdx * $dayIdx, 0);
        $fd = fopen($inputPath, "rb");
        stream_set_read_buffer($fd, 0);
        fseek($fd, $offsets[3]);
        $left = $offsets[4] - $offsets[3];

        while ($left > 0) {
            $block = fread($fd, $left > 8_388_608 ? 8_388_608 : $left);
            $len = strlen($block);
            if ($len === 0) break;
            $left -= $len;

            $lastReturn = strrpos($block, "\n");
            if ($lastReturn === false) {
                fseek($fd, -$len, 1);
                break;
            }

            if ($len - $lastReturn - 1 > 0) {
                fseek($fd, -($len - $lastReturn - 1), 1);
                $left += $len - $lastReturn - 1;
            }

            $p = 0;
            while ($p < $lastReturn) {
                $n1 = strpos($block, "\n", $p + 52);
                ++$masterGrid[$urlMap[substr($block, $p + 25, $n1 - $p - 51)] + $dayMap[substr($block, $n1 - 23, 8)]];
                if ($n1 >= $lastReturn) { $p = $n1 + 1; break; }

                $n2 = strpos($block, "\n", $n1 + 53);
                ++$masterGrid[$urlMap[substr($block, $n1 + 26, $n2 - $n1 - 52)] + $dayMap[substr($block, $n2 - 23, 8)]];
                if ($n2 >= $lastReturn) { $p = $n2 + 1; break; }

                $n3 = strpos($block, "\n", $n2 + 53);
                ++$masterGrid[$urlMap[substr($block, $n2 + 26, $n3 - $n2 - 52)] + $dayMap[substr($block, $n3 - 23, 8)]];
                if ($n3 >= $lastReturn) { $p = $n3 + 1; break; }

                $n4 = strpos($block, "\n", $n3 + 53);
                ++$masterGrid[$urlMap[substr($block, $n3 + 26, $n4 - $n3 - 52)] + $dayMap[substr($block, $n4 - 23, 8)]];
                $p = $n4 + 1;
            }
        }

        foreach ($pids as [$child, $token]) {
            pcntl_waitpid($child, $status);

            $shm = shmop_open($token, "a", 0, 0);
            $payload = shmop_read($shm, 0, $ramAlloc);
            shmop_delete($shm);

            $i = 0;
            foreach (unpack("V*", $payload) as $val) {
                if ($val !== 0) {
                    $masterGrid[$i] += $val;
                }
                ++$i;
            }
        }

        $out = fopen($outputPath, "wb");
        stream_set_write_buffer($out, 1_048_576);
        fwrite($out, "{");

        $init = true;
        for ($u = 0; $u < $urlIdx; $u++) {
            $base = $u * $dayIdx;
            $first = true;
            $metrics = "";

            for ($d = 0; $d < $dayIdx; $d++) {
                $v = $masterGrid[$base + $d];
                if ($v === 0) continue;

                if (!$first) $metrics .= ",\n";
                $first = false;
                $metrics .= '        "20' . $dayLabels[$d] . '": ' . $v;
            }

            if ($first) continue;

            fwrite($out, ($init ? "" : ",") . "\n    \"\\/blog\\/" . str_replace("/", "\\/", $urlDict[$u]) . "\": {\n" . $metrics . "\n    }");
            $init = false;
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}