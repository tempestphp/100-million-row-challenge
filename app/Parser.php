<?php

namespace App;

use App\Commands\Visit;
use function chr;
use function fclose;
use function feof;
use function fgets;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function ord;
use function pcntl_fork;
use function socket_create_pair;
use function socket_export_stream;
use function socket_set_option;
use function str_repeat;
use function stream_set_chunk_size;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function unpack;
use const AF_UNIX;
use const SEEK_CUR;
use const SEEK_END;
use const SOCK_STREAM;
use const SOL_SOCKET;
use const SO_RCVBUF;
use const SO_SNDBUF;

final class Parser
{
    private const int WORKERS = 13;
    private const int DISCOVER_SIZE = 262_144;
    private const int READ_CHUNK = 393_216;

    public function parse($inputPath, $outputPath)
    {
        gc_disable();

        $dateIds = [];
        $dates = [];
        $di = 0;
        for ($y = 21; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => $y === 24 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $mStr = ($m < 10 ? '0' : '') . $m;
                $ymStr = ($y % 10) . "-{$mStr}-";
                for ($d = 1; $d <= $maxD; $d++) {
                    $dStr = ($d < 10 ? '0' : '') . $d;
                    $dateIds[$ymStr . $dStr] = $di;
                    $dates[$di] = "20{$y}-{$mStr}-{$dStr}";
                    $di++;
                }
            }
        }

        $next = [];
        for ($i = 0; $i < 255; $i++) {
            $next[chr($i)] = chr($i + 1);
        }

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $raw = fread($handle, self::DISCOVER_SIZE);

        $paths = [];
        $slugBaseMap = [];
        $slugTotal = 0;
        $pos = 0;
        $lastNl = strrpos($raw, "\n") ?: 0;

        while ($pos < $lastNl) {
            $nl = strpos($raw, "\n", $pos + 52);
            if ($nl === false) break;
            $slug = substr($raw, $pos + 25, $nl - $pos - 51);
            if (!isset($slugBaseMap[$slug])) {
                $paths[$slugTotal] = $slug;
                $slugBaseMap[$slug] = $slugTotal * $di;
                $slugTotal++;
            }
            $pos = $nl + 1;
        }
        unset($raw);

        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, 25);
            if (!isset($slugBaseMap[$slug])) {
                $paths[$slugTotal] = $slug;
                $slugBaseMap[$slug] = $slugTotal * $di;
                $slugTotal++;
            }
        }

        $outputSize = $slugTotal * $di;

        stream_set_read_buffer($handle, 8192);
        fseek($handle, 0, SEEK_END);
        $fileSize = ftell($handle);
        $boundaries = [0];
        for ($i = 1; $i < self::WORKERS; $i++) {
            fseek($handle, (int) ($fileSize * $i / self::WORKERS));
            fgets($handle);
            $boundaries[] = ftell($handle);
        }
        fclose($handle);
        $boundaries[] = $fileSize;

        $sockets = [];

        for ($w = 0; $w < self::WORKERS - 1; $w++) {
            socket_create_pair(AF_UNIX, SOCK_STREAM, 0, $sockPair);
            socket_set_option($sockPair[0], SOL_SOCKET, SO_RCVBUF, $outputSize + 8192);
            socket_set_option($sockPair[1], SOL_SOCKET, SO_SNDBUF, $outputSize + 8192);
            $r = socket_export_stream($sockPair[0]);
            $ww = socket_export_stream($sockPair[1]);
            stream_set_chunk_size($r, $outputSize);
            stream_set_chunk_size($ww, $outputSize);
            if (pcntl_fork() === 0) {
                fwrite($ww, self::parseRange(
                    $inputPath, $boundaries[$w], $boundaries[$w + 1],
                    $slugBaseMap, $dateIds, $next, $outputSize,
                ));
                exit(0);
            }
            fclose($ww);
            $sockets[$w] = $r;
        }

        $parentOutput = self::parseRange(
            $inputPath, $boundaries[self::WORKERS - 1], $boundaries[self::WORKERS],
            $slugBaseMap, $dateIds, $next, $outputSize,
        );

        $counts = array_values(unpack('C*', $parentOutput));

        foreach ($sockets as $socket) {
            $data = '';
            while (!feof($socket)) {
                $chunk = fread($socket, $outputSize);
                if ($chunk === '' || $chunk === false) {
                    break;
                }
                $data .= $chunk;
            }
            fclose($socket);

            if ($data === '') {
                continue;
            }

            $len = strlen($data);
            for ($index = 0; $index < $len; $index++) {
                $counts[$index] += ord($data[$index]);
            }
        }

        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);
        fwrite($out, '{');

        $firstDatePrefixes = [];
        $datePrefixes = [];
        $d = $di;
        while ($d-- > 0) {
            $firstDatePrefixes[$d] = "\n        \"" . $dates[$d] . '": ';
            $datePrefixes[$d] = ",\n        \"" . $dates[$d] . '": ';
        }

        $escapedPaths = [];
        $p = $slugTotal;
        while ($p-- > 0) {
            $escapedPaths[$p] = "\"\/blog\/" . $paths[$p] . '": {';
        }

        $sep = "\n    ";
        $base = 0;

        for ($p = 0; $p < $slugTotal; $p++) {
            $firstDate = -1;
            $idx = $base;
            for ($d = 0; $d < $di; $d++) {
                if ($counts[$idx] !== 0) {
                    $firstDate = $d;
                    break;
                }
                $idx++;
            }

            if ($firstDate === -1) {
                $base += $di;
                continue;
            }

            $buf = $sep . $escapedPaths[$p] . $firstDatePrefixes[$firstDate] . $counts[$idx];
            $sep = ",\n    ";

            for ($d = $firstDate + 1; $d < $di; $d++) {
                $idx++;
                if ($counts[$idx] === 0) continue;
                $buf .= $datePrefixes[$d] . $counts[$idx];
            }

            $buf .= "\n    }";
            fwrite($out, $buf);
            $base += $di;
        }

        fwrite($out, "\n}");
        fclose($out);
    }

    private static function parseRange(
        $inputPath, $start, $end,
        $slugBaseMap, $dateIds, $next, $outputSize,
    ) {
        $output = str_repeat("\0", $outputSize);
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $chunk = fread($handle, $remaining > self::READ_CHUNK ? self::READ_CHUNK : $remaining);
            $chunkLen = strlen($chunk);
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) break;

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $p = 25;
            $fence = $lastNl - 1010;

            while ($p < $fence) {
                $idx = $slugBaseMap[substr($chunk, $p, ($sep = strpos($chunk, ',', $p)) - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
                $idx = $slugBaseMap[substr($chunk, $p, ($sep = strpos($chunk, ',', $p)) - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
                $idx = $slugBaseMap[substr($chunk, $p, ($sep = strpos($chunk, ',', $p)) - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
                $idx = $slugBaseMap[substr($chunk, $p, ($sep = strpos($chunk, ',', $p)) - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
                $idx = $slugBaseMap[substr($chunk, $p, ($sep = strpos($chunk, ',', $p)) - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
                $idx = $slugBaseMap[substr($chunk, $p, ($sep = strpos($chunk, ',', $p)) - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
                $idx = $slugBaseMap[substr($chunk, $p, ($sep = strpos($chunk, ',', $p)) - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
                $idx = $slugBaseMap[substr($chunk, $p, ($sep = strpos($chunk, ',', $p)) - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
                $idx = $slugBaseMap[substr($chunk, $p, ($sep = strpos($chunk, ',', $p)) - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
                $idx = $slugBaseMap[substr($chunk, $p, ($sep = strpos($chunk, ',', $p)) - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
            }

            while ($p < $lastNl) {
                if (($sep = strpos($chunk, ',', $p)) === false || $sep >= $lastNl) break;
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
            }
        }

        return $output;
    }
}
