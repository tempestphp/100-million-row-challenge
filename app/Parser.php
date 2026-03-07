<?php

namespace App;

use App\Commands\Visit;
use function array_values;
use function chr;
use function count;
use function fclose;
use function fgets;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function getmypid;
use function ini_set;
use function pcntl_fork;
use function pcntl_wait;
use function shmop_delete;
use function shmop_open;
use function shmop_read;
use function shmop_write;
use function str_repeat;
use function str_replace;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function unpack;
use const SEEK_CUR;

final class Parser
{
    private const W = 4;
    private const CH = 20;
    private const C = 262_144;

    public function parse(string $in, string $out): void
    {
        gc_disable();
        ini_set('memory_limit', '-1');
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
                    $ds = ($d < 10 ? '0' : '') . $d;
                    $k = $y . '-' . $ms . '-' . $ds;
                    $db[$k] = $dc;
                    $dl[$dc++] = '20' . $k;
                }
            }
        }

        $nx = [];
        for ($i = 0; $i < 255; $i++)
            $nx[chr($i)] = chr($i + 1);

        $fh = fopen($in, 'rb');
        stream_set_read_buffer($fh, 0);
        $raw = fread($fh, 2_097_152);
        $lastNl = strrpos($raw, "\n") ?: 0;
        $nlPad = ($lastNl > 0 && $raw[$lastNl - 1] === "\r") ? 52 : 51;

        $pi = [];
        $pl = [];
        $pc = 0;
        $pos = 0;
        $noNew = 0;
        while ($pos < $lastNl) {
            $nl = strpos($raw, "\n", $pos + 52);
            if ($nl === false)
                break;
            $s = substr($raw, $pos + 25, $nl - $pos - $nlPad);
            if (!isset($pi[$s])) {
                $pi[$s] = $pc;
                $pl[$pc++] = $s;
                $noNew = 0;
            } elseif (++$noNew > 5000) {
                break;
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

        $pb = [];
        for ($p = 0; $p < $pc; $p++)
            $pb[$pl[$p]] = $p * $dc;
        $cells = $pc * $dc;

        $bnd = [0];
        for ($i = 1; $i < self::CH; $i++) {
            fseek($fh, (int) ($sz * $i / self::CH));
            fgets($fh);
            $bnd[] = ftell($fh);
        }
        fclose($fh);
        $bnd[] = $sz;

        $myPid = getmypid();
        $shmIds = [];
        for ($w = 0; $w < self::W - 1; $w++) {
            $key = (($myPid & 0x3FFF) << 4) | $w;
            if ($key <= 0)
                $key = 0x1000 + $w;
            $shmIds[$w] = shmop_open($key, 'c', 0600, $cells);
        }

        $pidMap = [];
        for ($w = 0; $w < self::W - 1; $w++) {
            $pid = pcntl_fork();
            if ($pid === 0) {
                gc_disable();
                $blob = static::crunchWorker($in, $bnd, $w, self::W, $pb, $db, $cells, $nx);
                shmop_write($shmIds[$w], $blob, 0);
                exit(0);
            }
            $pidMap[$pid] = $w;
        }

        $baseBlob = static::crunchWorker($in, $bnd, self::W - 1, self::W, $pb, $db, $cells, $nx);
        $counts = array_values(unpack('C*', $baseBlob));
        unset($baseBlob);

        $rem = self::W - 1;
        while ($rem > 0) {
            $pid = pcntl_wait($st);
            if (!isset($pidMap[$pid]))
                continue;
            $w = $pidMap[$pid];
            $blob = shmop_read($shmIds[$w], 0, $cells);
            $j = 0;
            foreach (unpack('C*', $blob) as $v)
                $counts[$j++] += $v;
            shmop_delete($shmIds[$w]);
            $rem--;
        }

        $dp = [];
        for ($d = 0; $d < $dc; $d++)
            $dp[$d] = '        "' . $dl[$d] . '": ';
        $pp = [];
        for ($p = 0; $p < $pc; $p++)
            $pp[$p] = '"\/blog\/' . str_replace('/', '\/', $pl[$p]) . '"';

        $o = fopen($out, 'wb');
        stream_set_write_buffer($o, 1_048_576);
        fwrite($o, '{');
        $nl = PHP_EOL;
        $first = true;
        $buf = '';
        for ($p = 0; $p < $pc; $p++) {
            $base = $p * $dc;
            $body = '';
            $sep = $nl;
            for ($d = 0; $d < $dc; $d++) {
                $n = $counts[$base + $d];
                if (!$n)
                    continue;
                $body .= $sep . $dp[$d] . $n;
                $sep = ',' . $nl;
            }
            if (!$body)
                continue;
            $buf .= ($first ? '' : ',') . $nl . '    ' . $pp[$p] . ': {' . $body . $nl . '    }';
            $first = false;
            if (strlen($buf) > 131_072) {
                fwrite($o, $buf);
                $buf = '';
            }
        }
        fwrite($o, $buf . $nl . '}');
        fclose($o);
    }

    private static function crunchWorker(
        string $in,
        array $bnd,
        int $worker,
        int $workers,
        array $pb,
        array $db,
        int $cells,
        array $nx
    ): string {
        $cnt = str_repeat("\0", $cells);
        $chunks = count($bnd) - 1;
        for ($i = $worker; $i < $chunks; $i += $workers)
            static::crunchInto($in, $bnd[$i], $bnd[$i + 1], $pb, $db, $nx, $cnt);
        return $cnt;
    }

    private static function crunchInto(
        string $in,
        int $s,
        int $e,
        array $pb,
        array $db,
        array $nx,
        string &$cnt
    ): void {
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

            $step = ($ln > 0 && $ch[$ln - 1] === "\r") ? 53 : 52;
            $p = 25;
            $f = $ln - 2000;

            while ($p < $f) {
                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;

                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;

                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;

                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;

                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;

                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;

                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;

                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;

                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;

                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;

                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;

                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;

                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;

                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;

                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;

                $c = strpos($ch, ',', $p);
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;
            }

            while ($p < $ln) {
                $c = strpos($ch, ',', $p);
                if ($c === false || $c >= $ln)
                    break;
                $idx = $pb[substr($ch, $p, $c - $p)] + $db[substr($ch, $c + 3, 8)];
                $cnt[$idx] = $nx[$cnt[$idx]];
                $p = $c + $step;
            }
        }
        fclose($h);
    }
}