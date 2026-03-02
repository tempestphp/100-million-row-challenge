<?php

namespace App;

use App\Commands\Visit;
use function strlen;
use function fseek;
use function pack;
use function substr;
use function fwrite;
use function array_fill;
use function pcntl_wait;
use function fread;
use function gc_disable;
use function strpos;
use function unpack;
use function chr;
use function stream_set_read_buffer;
use function fopen;
use function pcntl_fork;
use function str_replace;
use function file_get_contents;
use function file_put_contents;
use function strrpos;
use function ini_set;
use function fclose;
use function ftell;
use function getmypid;
use function stream_set_write_buffer;
use function array_count_values;
use function filesize;
use function fgets;
use function sys_get_temp_dir;
use function unlink;
use const SEEK_CUR;
use const WNOHANG;

final class Parser
{
    private const W = 12;
    private const C = 131_072;

    public function parse(string $in, string $out): void
    {
        gc_disable();
        ini_set('memory_limit', '-1');
        $sz = filesize($in);

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
        $raw = fread($fh, min(2_097_152, $sz));
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

        $pfx = sys_get_temp_dir() . "/p_" . getmypid() . "_";
        $cmap = [];
        for ($w = 0; $w < self::W - 1; $w++) {
            $pid = pcntl_fork();
            if ($pid === 0) {
                gc_disable();
                $wc = static::crunch($in, $bnd[$w], $bnd[$w + 1], $pi, $db, $pc, $dc);
                file_put_contents($pfx . $w, pack('v*', ...$wc));
                exit(0);
            }
            $cmap[$pid] = $w;
        }

        $counts = static::crunch($in, $bnd[self::W - 1], $bnd[self::W], $pi, $db, $pc, $dc);
        $pend = self::W - 1;
        while ($pend > 0) {
            $pid = pcntl_wait($st, WNOHANG);
            if ($pid <= 0)
                $pid = pcntl_wait($st);
            if (!isset($cmap[$pid]))
                continue;
            $w = $cmap[$pid];
            $wc = unpack('v*', file_get_contents($pfx . $w));
            unlink($pfx . $w);
            $j = 0;
            foreach ($wc as $v)
                $counts[$j++] += $v;
            $pend--;
        }

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
            if (strlen($buf) > 65_536) {
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
            $f = $ln - 800;
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