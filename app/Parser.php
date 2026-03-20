<?php
namespace App;

use function array_count_values;
use function array_fill;
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
use function pack;
use function pcntl_fork;
use function pcntl_wait;
use function sodium_add;
use function stream_select;
use function stream_set_chunk_size;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function stream_socket_pair;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function unpack;
use const SEEK_CUR;
use const SEEK_END;
use const STREAM_IPPROTO_IP;
use const STREAM_PF_UNIX;
use const STREAM_SOCK_STREAM;

final class Parser
{
    private const W = 10;
    private const C = 163_840;

    public function parse(string $in, string $out): void
    {
        gc_disable();

        $dateIds = [];
        $datePrefixes = [];
        $dateCount = 0;
        for ($y = 21; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) { 2 => $y === 24 ? 29 : 28, 4, 6, 9, 11 => 30, default => 31 };
                $ms = ($m < 10 ? '0' : '') . $m;
                $ym = "{$y}-{$ms}-";
                for ($d = 1; $d <= $maxD; $d++) {
                    $key = $ym . ($d < 10 ? '0' : '') . $d;
                    $dateIds[$key] = $dateCount;
                    $datePrefixes[$dateCount] = '        "20' . $key . '": ';
                    $dateCount++;
                }
            }
        }

        $dateIdChars = [];
        foreach ($dateIds as $date => $id) {
            $dateIdChars[$date] = chr($id & 0xFF) . chr($id >> 8);
        }

        $fh = fopen($in, 'rb');
        stream_set_read_buffer($fh, 0);
        $raw = fread($fh, 142_000);
        $rawLen = strlen($raw);

        $pl = []; $pi = []; $pc = 0; $pos = 0;
        while ($pc < 268 && $pos + 52 < $rawLen) {
            $nl = strpos($raw, "\n", $pos + 52);
            if ($nl === false) break;
            $s = substr($raw, $pos + 25, $nl - $pos - 51);
            if (!isset($pi[$s])) { $pi[$s] = $pc; $pl[$pc++] = $s; }
            $pos = $nl + 1;
        }
        unset($raw);

        $grain = 1 << 26;
        $bnd = [0];
        $lo = $grain;
        fseek($fh, 0, SEEK_END);
        $fileSize = ftell($fh);
        while ($lo < $fileSize) {
            fseek($fh, $lo); fgets($fh);
            $bnd[] = ftell($fh);
            $lo += $grain;
        }
        $bnd[] = $fileSize;
        fclose($fh);
        $nChunks = count($bnd) - 1;

        $blobSize = $pc * $dateCount * 2;

        $sockets = [];
        for ($w = 0; $w < self::W; $w++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_chunk_size($pair[0], $blobSize);
            stream_set_chunk_size($pair[1], $blobSize);
            if (pcntl_fork() === 0) {
                gc_disable();
                fclose($pair[0]);
                $counts = static::crunch($in, $bnd, $nChunks, $w, self::W, $pi, $dateIdChars, $pc, $dateCount);
                fwrite($pair[1], pack('v*', ...$counts));
                fclose($pair[1]);
                exit(0);
            }
            fclose($pair[1]);
            $sockets[$w] = $pair[0];
        }

        $bins = [];
        $write = []; $except = [];
        while ($sockets !== []) {
            $read = $sockets;
            stream_select($read, $write, $except, null);
            foreach ($read as $key => $sock) {
                $data = fread($sock, $blobSize);
                if ($data !== '' && $data !== false) $bins[$key] = ($bins[$key] ?? '') . $data;
                if (feof($sock)) { fclose($sock); unset($sockets[$key]); }
            }
        }
        for ($w = 0; $w < self::W; $w++) pcntl_wait($st);

        $merged = $bins[0];
        for ($w = 1; $w < self::W; $w++) sodium_add($merged, $bins[$w]);
        unset($bins);

        $counts = unpack('v*', $merged);
        unset($merged);

        $escapedPaths = [];
        for ($p = 0; $p < $pc; $p++)
            $escapedPaths[$p] = '"\/blog\/' . $pl[$p] . '": {';

        $o = fopen($out, 'wb');
        stream_set_write_buffer($o, 2_097_152);
        fwrite($o, '{');

        $sep = "\n    ";
        $ptrBase = 1;
        for ($p = 0; $p < $pc; $p++) {
            $limit = $ptrBase + $dateCount;
            while ($ptrBase < $limit && $counts[$ptrBase] === 0) $ptrBase++;
            if ($ptrBase === $limit) continue;
            $dOff = $ptrBase - ($limit - $dateCount);
            $json = $sep . $escapedPaths[$p] . "\n" . $datePrefixes[$dOff] . $counts[$ptrBase];
            $sep = ",\n    ";
            for ($ptrBase++; $ptrBase < $limit; $ptrBase++) {
                if ($counts[$ptrBase] !== 0)
                    $json .= ",\n" . $datePrefixes[$ptrBase - ($limit - $dateCount)] . $counts[$ptrBase];
            }
            fwrite($o, $json . "\n    }");
        }

        fwrite($o, "\n}");
        fclose($o);
    }

    private static function crunch(
        string $in, array $bnd, int $nChunks, int $worker, int $workers,
        array $pi, array $dateIdChars, int $pc, int $dateCount
    ): array {
        $buckets = array_fill(0, $pc, '');
        $h = fopen($in, 'rb');
        stream_set_read_buffer($h, 0);
        $cs = self::C;

        for ($ci = $worker; $ci < $nChunks; $ci += $workers) {
            fseek($h, $bnd[$ci]);
            $rem = $bnd[$ci + 1] - $bnd[$ci];

            while ($rem > 0) {
                $chunk = fread($h, $rem > $cs ? $cs : $rem);
                if ($chunk === false || $chunk === '') break;
                $cl = strlen($chunk); $rem -= $cl;
                $ln = strrpos($chunk, "\n");
                if ($ln === false) break;
                $t = $cl - $ln - 1;
                if ($t > 0) { fseek($h, -$t, SEEK_CUR); $rem += $t; }

                $p = 25;
                $f = $ln - 600;

                while ($p < $f) {
                    $sep = strpos($chunk, ',', $p);
                    $buckets[$pi[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                    $p = $sep + 52;
                    $sep = strpos($chunk, ',', $p);
                    $buckets[$pi[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                    $p = $sep + 52;
                    $sep = strpos($chunk, ',', $p);
                    $buckets[$pi[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                    $p = $sep + 52;
                    $sep = strpos($chunk, ',', $p);
                    $buckets[$pi[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                    $p = $sep + 52;
                    $sep = strpos($chunk, ',', $p);
                    $buckets[$pi[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                    $p = $sep + 52;
                }

                while ($p < $ln) {
                    $sep = strpos($chunk, ',', $p);
                    if ($sep === false || $sep >= $ln) break;
                    $buckets[$pi[substr($chunk, $p, $sep - $p)]] .= $dateIdChars[substr($chunk, $sep + 3, 8)];
                    $p = $sep + 52;
                }
            }
        }

        fclose($h);

        $counts = array_fill(0, $pc * $dateCount, 0);
        for ($p = 0; $p < $pc; $p++) {
            if ($buckets[$p] === '') continue;
            $offset = $p * $dateCount;
            foreach (array_count_values(unpack('v*', $buckets[$p])) as $did => $cnt) {
                $counts[$offset + $did] += $cnt;
            }
        }

        return $counts;
    }
}