<?php

namespace App;

use App\Commands\Visit;
use function array_count_values;
use function array_fill;
use function chr;
use function fclose;
use function fgets;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function ini_set;
use function pack;
use function pcntl_fork;
use function pcntl_wait;
use function stream_get_contents;
use function stream_select;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function stream_socket_pair;
use function strlen;
use function strpos;
use function strrpos;
use function str_replace;
use function substr;
use function unpack;
use const SEEK_CUR;
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
        ini_set('memory_limit', '-1');
        ini_set('realpath_cache_size', '4096K');
        ini_set('realpath_cache_ttl', '600');
        error_reporting(0);
        $sz = 7_509_674_827;

        $dc = 0;
        $db = [];
        $dl = [];
        for ($y = 21; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $md = match ($m) { 2 => $y === 24 ? 29 : 28, 4, 6, 9, 11 => 30, default => 31};
                $ms = ($m < 10 ? '0' : '') . $m;
                for ($d = 1; $d <= $md; $d++) {
                    $k = "$y-$ms-" . ($d < 10 ? '0' : '') . $d;
                    $db[$k] = chr($dc & 0xFF) . chr($dc >> 8);
                    $dl[$dc++] = $k;
                }
            }
        }

        $fh = fopen($in, 'rb');
        stream_set_read_buffer($fh, 0);
        $raw = fread($fh, min(10_485_760, $sz));
        $ln = strrpos($raw, "\n") ?: 0;
        $pi = [];
        $pl = [];
        $pc = 0;
        $pos = 0;
        while ($pos < $ln) {
            $nl = strpos($raw, "\n", $pos + 52);
            if ($nl === false)
                break;
            $s = substr($raw, $pos + 25, $nl - $pos - 51);
            if (!isset($pi[$s])) {
                $pi[$s] = $pc;
                $pl[$pc++] = $s;
            }
            $pos = $nl + 1;
        }
        unset($raw);
        foreach (Visit::all() as $v) {
            $s = substr($v->uri, 25);
            if (!isset($pi[$s])) {
                $pi[$s] = $pc;
                $pl[$pc++] = $s;
            }
        }

        $bnd = [0];
        for ($i = 1; $i < self::W; $i++) {
            fseek($fh, (int) ($sz * $i / self::W));
            fgets($fh);
            $bnd[] = ftell($fh);
        }
        fclose($fh);
        $bnd[] = $sz;

        $socks = [];
        for ($w = 0; $w < self::W - 1; $w++) {
            $socks[$w] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        }

        for ($w = 0; $w < self::W - 1; $w++) {
            $pid = pcntl_fork();
            if ($pid === 0) {
                gc_disable();
                for ($j = 0; $j < self::W - 1; $j++)
                    fclose($socks[$j][0]);
                for ($j = 0; $j < self::W - 1; $j++)
                    if ($j !== $w)
                        fclose($socks[$j][1]);
                $wc = static::crunch($in, $bnd[$w], $bnd[$w + 1], $pi, $db, $pc, $dc);
                $packed = pack('v*', ...$wc);
                $len = strlen($packed);
                $written = 0;
                while ($written < $len) {
                    $n = fwrite($socks[$w][1], substr($packed, $written));
                    if ($n === false || $n === 0)
                        break;
                    $written += $n;
                }
                fclose($socks[$w][1]);
                exit(0);
            }
        }

        for ($w = 0; $w < self::W - 1; $w++)
            fclose($socks[$w][1]);

        $counts = static::crunch($in, $bnd[self::W - 1], $bnd[self::W], $pi, $db, $pc, $dc);

        $readers = [];
        for ($w = 0; $w < self::W - 1; $w++)
            $readers[(int) $socks[$w][0]] = $socks[$w][0];
        while ($readers) {
            $read = array_values($readers);
            $w2 = [];
            $ex = [];
            stream_select($read, $w2, $ex, 30);
            foreach ($read as $sock) {
                $data = stream_get_contents($sock);
                fclose($sock);
                unset($readers[(int) $sock]);
                $wc = unpack('v*', $data);
                $j = 0;
                foreach ($wc as $v)
                    $counts[$j++] += $v;
            }
        }
        for ($w = 0; $w < self::W - 1; $w++)
            pcntl_wait($st);

        $dp = [];
        for ($d = 0; $d < $dc; $d++)
            $dp[$d] = '        "20' . $dl[$d] . '": ';
        $pp = [];
        for ($p = 0; $p < $pc; $p++)
            $pp[$p] = '"\/blog\/' . str_replace('/', '\/', $pl[$p]) . '"';

        $o = fopen($out, 'wb');
        stream_set_write_buffer($o, 1_048_576);
        fwrite($o, '{');
        $first = true;
        $buf = '';
        for ($p = 0; $p < $pc; $p++) {
            $base = $p * $dc;
            $body = '';
            $sep = "\n";
            for ($d = 0; $d < $dc; $d++) {
                $n = $counts[$base + $d];
                if (!$n)
                    continue;
                $body .= $sep . $dp[$d] . $n;
                $sep = ",\n";
            }
            if (!$body)
                continue;
            $buf .= ($first ? '' : ',') . "\n    " . $pp[$p] . ": {" . $body . "\n    }";
            $first = false;
            if (strlen($buf) > 131_072) {
                fwrite($o, $buf);
                $buf = '';
            }
        }
        fwrite($o, $buf . "\n}");
        fclose($o);
    }

    private static function crunch(string $in, int $s, int $e, array $pi, array $db, int $pc, int $dc): array
    {
        $bk = array_fill(0, $pc, '');
        $h = fopen($in, 'rb');
        stream_set_read_buffer($h, 0);
        fseek($h, $s);
        $rem = $e - $s;
        $cs = self::C;
        while ($rem > 0) {
            $ch = fread($h, $rem > $cs ? $cs : $rem);
            if ($ch === false || $ch === '')
                break;
            $cl = strlen($ch);
            $rem -= $cl;
            $ln = strrpos($ch, "\n");
            if ($ln === false)
                break;
            $t = $cl - $ln - 1;
            if ($t > 0) {
                fseek($h, -$t, SEEK_CUR);
                $rem += $t;
            }
            $p = 25;
            $f = $ln - 1600;
            while ($p < $f) {
                $c = strpos($ch, ',', $p);
                $bk[$pi[substr($ch, $p, $c - $p)]] .= $db[substr($ch, $c + 3, 8)];
                $p = $c + 52;
                $c = strpos($ch, ',', $p);
                $bk[$pi[substr($ch, $p, $c - $p)]] .= $db[substr($ch, $c + 3, 8)];
                $p = $c + 52;
                $c = strpos($ch, ',', $p);
                $bk[$pi[substr($ch, $p, $c - $p)]] .= $db[substr($ch, $c + 3, 8)];
                $p = $c + 52;
                $c = strpos($ch, ',', $p);
                $bk[$pi[substr($ch, $p, $c - $p)]] .= $db[substr($ch, $c + 3, 8)];
                $p = $c + 52;
                $c = strpos($ch, ',', $p);
                $bk[$pi[substr($ch, $p, $c - $p)]] .= $db[substr($ch, $c + 3, 8)];
                $p = $c + 52;
                $c = strpos($ch, ',', $p);
                $bk[$pi[substr($ch, $p, $c - $p)]] .= $db[substr($ch, $c + 3, 8)];
                $p = $c + 52;
                $c = strpos($ch, ',', $p);
                $bk[$pi[substr($ch, $p, $c - $p)]] .= $db[substr($ch, $c + 3, 8)];
                $p = $c + 52;
                $c = strpos($ch, ',', $p);
                $bk[$pi[substr($ch, $p, $c - $p)]] .= $db[substr($ch, $c + 3, 8)];
                $p = $c + 52;
                $c = strpos($ch, ',', $p);
                $bk[$pi[substr($ch, $p, $c - $p)]] .= $db[substr($ch, $c + 3, 8)];
                $p = $c + 52;
                $c = strpos($ch, ',', $p);
                $bk[$pi[substr($ch, $p, $c - $p)]] .= $db[substr($ch, $c + 3, 8)];
                $p = $c + 52;
                $c = strpos($ch, ',', $p);
                $bk[$pi[substr($ch, $p, $c - $p)]] .= $db[substr($ch, $c + 3, 8)];
                $p = $c + 52;
                $c = strpos($ch, ',', $p);
                $bk[$pi[substr($ch, $p, $c - $p)]] .= $db[substr($ch, $c + 3, 8)];
                $p = $c + 52;
                $c = strpos($ch, ',', $p);
                $bk[$pi[substr($ch, $p, $c - $p)]] .= $db[substr($ch, $c + 3, 8)];
                $p = $c + 52;
                $c = strpos($ch, ',', $p);
                $bk[$pi[substr($ch, $p, $c - $p)]] .= $db[substr($ch, $c + 3, 8)];
                $p = $c + 52;
                $c = strpos($ch, ',', $p);
                $bk[$pi[substr($ch, $p, $c - $p)]] .= $db[substr($ch, $c + 3, 8)];
                $p = $c + 52;
                $c = strpos($ch, ',', $p);
                $bk[$pi[substr($ch, $p, $c - $p)]] .= $db[substr($ch, $c + 3, 8)];
                $p = $c + 52;
            }
            while ($p < $ln) {
                $c = strpos($ch, ',', $p);
                if ($c === false || $c >= $ln)
                    break;
                $bk[$pi[substr($ch, $p, $c - $p)]] .= $db[substr($ch, $c + 3, 8)];
                $p = $c + 52;
            }
        }
        fclose($h);
        $cnt = array_fill(0, $pc * $dc, 0);
        for ($p = 0; $p < $pc; $p++) {
            if ($bk[$p] === '')
                continue;
            $base = $p * $dc;
            foreach (array_count_values(unpack('v*', $bk[$p])) as $id => $n)
                $cnt[$base + $id] += $n;
        }
        return $cnt;
    }
}