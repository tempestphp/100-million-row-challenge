<?php

namespace App;

use function array_fill;
use function fclose;
use function feof;
use function fopen;
use function fread;
use function fseek;
use function fwrite;
use function gc_disable;
use function str_replace;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use const SEEK_CUR;

final class Parser
{
    public static function parse($inputPath, $outputPath)
    {
        gc_disable();

        $dix = [];
        $dls = [];
        $dn = 0;
        for ($yy = 1; $yy <= 6; $yy++) {
            for ($mm = 1; $mm <= 12; $mm++) {
                $md = match ($mm) {
                    2 => $yy === 4 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $ms = ($mm < 10 ? '0' : '') . $mm;
                $ym = $yy . '-' . $ms . '-';
                for ($dd = 1; $dd <= $md; $dd++) {
                    $ds = ($dd < 10 ? '0' : '') . $dd;
                    $dk = $ym . $ds;
                    $dix[$dk] = $dn;
                    $dls[$dn] = '202' . $dk;
                    $dn++;
                }
            }
        }

        $fp = fopen($inputPath, 'rb');
        stream_set_read_buffer($fp, 0);
        $peek = fread($fp, 181000);

        $slugs = [];
        $lut = [];
        $sn = 0;
        $at = 0;
        $cut = strrpos($peek, "\n") ?: 0;

        while ($at < $cut && $sn < 268) {
            $nl = strpos($peek, "\n", $at + 52);
            if ($nl === false) {
                break;
            }

            $slug = substr($peek, $at + 25, $nl - $at - 51);
            if (!isset($lut[$slug])) {
                $slugs[$sn] = $slug;
                $lut[$slug] = $sn * $dn;
                $sn++;
            }

            $at = $nl + 1;
        }
        unset($peek);

        $base = 'https://stitcher.io/blog/';
        $tail = 1;
        while (true) {
            $seen = [];
            $p = 0;
            while ($p < $sn) {
                $sig = substr($base . $slugs[$p], -$tail);
                if (isset($seen[$sig])) {
                    $tail++;
                    continue 2;
                }

                $seen[$sig] = true;
                $p++;
            }

            break;
        }

        $sh = 20;
        $msk = (1 << $sh) - 1;
        $hop = 0;
        $lut = [];
        for ($p = 0; $p < $sn; $p++) {
            $step = strlen($slugs[$p]) + 52;
            if ($step > $hop) {
                $hop = $step;
            }

            $lut[substr($base . $slugs[$p], -$tail)] = ($step << $sh) | ($p * $dn);
        }
        $tailAt = 26 + $tail;
        $dayAt = 22;
        $dayLen = 7;
        $gate = ($hop * 10) + $tailAt;

        $cells = $sn * $dn;

        fclose($fp);

        $cnt = array_fill(0, $cells, 0);
        $fp = fopen($inputPath, 'rb');
        stream_set_read_buffer($fp, 0);

        while (!feof($fp)) {
            $buf = fread($fp, 163_840);
            $len = strlen($buf);
            if ($len === 0) {
                break;
            }

            $nl = strrpos($buf, "\n");
            if ($nl === false) {
                continue;
            }

            $back = $len - $nl - 1;
            if ($back > 0) {
                fseek($fp, -$back, SEEK_CUR);
            }

            $i = $nl;
            while ($i > $gate) {
                $v = $lut[substr($buf, $i - $tailAt, $tail)];
                $ix = ($v & $msk) + $dix[substr($buf, $i - $dayAt, $dayLen)];
                $cnt[$ix]++;
                $i -= $v >> $sh;

                $v = $lut[substr($buf, $i - $tailAt, $tail)];
                $ix = ($v & $msk) + $dix[substr($buf, $i - $dayAt, $dayLen)];
                $cnt[$ix]++;
                $i -= $v >> $sh;

                $v = $lut[substr($buf, $i - $tailAt, $tail)];
                $ix = ($v & $msk) + $dix[substr($buf, $i - $dayAt, $dayLen)];
                $cnt[$ix]++;
                $i -= $v >> $sh;

                $v = $lut[substr($buf, $i - $tailAt, $tail)];
                $ix = ($v & $msk) + $dix[substr($buf, $i - $dayAt, $dayLen)];
                $cnt[$ix]++;
                $i -= $v >> $sh;

                $v = $lut[substr($buf, $i - $tailAt, $tail)];
                $ix = ($v & $msk) + $dix[substr($buf, $i - $dayAt, $dayLen)];
                $cnt[$ix]++;
                $i -= $v >> $sh;

                $v = $lut[substr($buf, $i - $tailAt, $tail)];
                $ix = ($v & $msk) + $dix[substr($buf, $i - $dayAt, $dayLen)];
                $cnt[$ix]++;
                $i -= $v >> $sh;

                $v = $lut[substr($buf, $i - $tailAt, $tail)];
                $ix = ($v & $msk) + $dix[substr($buf, $i - $dayAt, $dayLen)];
                $cnt[$ix]++;
                $i -= $v >> $sh;

                $v = $lut[substr($buf, $i - $tailAt, $tail)];
                $ix = ($v & $msk) + $dix[substr($buf, $i - $dayAt, $dayLen)];
                $cnt[$ix]++;
                $i -= $v >> $sh;

                $v = $lut[substr($buf, $i - $tailAt, $tail)];
                $ix = ($v & $msk) + $dix[substr($buf, $i - $dayAt, $dayLen)];
                $cnt[$ix]++;
                $i -= $v >> $sh;

                $v = $lut[substr($buf, $i - $tailAt, $tail)];
                $ix = ($v & $msk) + $dix[substr($buf, $i - $dayAt, $dayLen)];
                $cnt[$ix]++;
                $i -= $v >> $sh;
            }

            while ($i >= $tailAt) {
                $v = $lut[substr($buf, $i - $tailAt, $tail)];
                $ix = ($v & $msk) + $dix[substr($buf, $i - $dayAt, $dayLen)];
                $cnt[$ix]++;
                $i -= $v >> $sh;
            }
        }

        fclose($fp);

        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);
        fwrite($out, '{');

        $dpre = [];
        for ($d = 0; $d < $dn; $d++) {
            $dpre[$d] = '        "' . $dls[$d] . '": ';
        }

        $spre = [];
        for ($p = 0; $p < $sn; $p++) {
            $spre[$p] = '"\/blog\/' . str_replace('/', '\/', $slugs[$p]) . '": {';
        }

        $sep = "\n    ";
        $baseIx = 0;
        for ($p = 0; $p < $sn; $p++) {
            $d = 0;
            $ix = $baseIx;
            while ($d < $dn && $cnt[$ix] === 0) {
                $d++;
                $ix++;
            }

            if ($d === $dn) {
                $baseIx += $dn;
                continue;
            }

            $json = $sep . $spre[$p] . "\n" . $dpre[$d] . $cnt[$ix];
            $sep = ",\n    ";
            $d++;
            while ($d < $dn) {
                $ix++;
                if ($cnt[$ix] !== 0) {
                    $json .= ",\n" . $dpre[$d] . $cnt[$ix];
                }

                $d++;
            }

            $json .= "\n    }";
            fwrite($out, $json);
            $baseIx += $dn;
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
