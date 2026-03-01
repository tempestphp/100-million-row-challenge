<?php

namespace App;

use App\Commands\Visit;
use function fopen;
use function fclose;
use function fread;
use function fseek;
use function fgets;
use function ftell;
use function fwrite;
use function filesize;
use function strlen;
use function substr;
use function strpos;
use function strrpos;
use function array_fill;
use function array_count_values;
use function chr;
use function unpack;
use function pack;
use function file_put_contents;
use function file_get_contents;
use function str_replace;
use function implode;
use function pcntl_fork;
use function pcntl_wait;
use function getmypid;
use function sys_get_temp_dir;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function unlink;
use function count;
use const WNOHANG;

final class Parser
{
    private const WORKERS = 8;
    private const READ_CHUNK = 8388608;

    public function parse(string $inputPath, string $outputPath): void
    {
        self::run($inputPath, $outputPath, filesize($inputPath));
    }

    private static function run(string $inputPath, string $outputPath, int $fileSize): void
    {

        $dateIds = [];
        $dates = [];
        $dateCount = 0;
        for ($y = 18; $y <= 30; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $md = match ($m) { 2 => $y % 4 === 0 ? 29 : 28, 4, 6, 9, 11 => 30, default => 31 };
                $mStr = ($m < 10 ? '0' : '') . $m;
                $ymStr = "$y-$mStr-";
                for ($d = 1; $d <= $md; $d++) {
                    $key = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $dateIds[$key] = $dateCount;
                    $dates[$dateCount++] = $key;
                }
            }
        }

        $dateIdChars = [];
        foreach ($dateIds as $date => $id) {
            $dateIdChars[$date] = chr($id & 0xFF) . chr($id >> 8);
        }

        $f = fopen($inputPath, 'rb');
        stream_set_read_buffer($f, 0);
        $raw = fread($f, 1048576);
        fclose($f);
        
        $pathIds = [];
        $paths = [];
        $pathCount = 0;
        $pos = 0;
        $lastNl = strrpos($raw, "\n");
        while ($pos < $lastNl) {
            $nlPos = strpos($raw, "\n", $pos + 52);
            if ($nlPos === false) break;
            $slug = substr($raw, $pos + 25, $nlPos - $pos - 51);
            if (isset($pathIds[$slug])) {
                $pos = $nlPos + 1;
                continue;
            }
            $pathIds[$slug] = $pathCount;
            $paths[$pathCount++] = $slug;
            $pos = $nlPos + 1;
        }
        unset($raw);

        foreach (Visit::all() as $v) {
            $slug = substr($v->uri, 25);
            if (isset($pathIds[$slug])) continue;
            $pathIds[$slug] = $pathCount;
            $paths[$pathCount++] = $slug;
        }

        $w = self::WORKERS;
        $b = [0];
        $fh = fopen($inputPath, 'rb');
        for ($i = 1; $i < $w; $i++) {
            fseek($fh, (int)($fileSize * $i / $w));
            fgets($fh);
            $b[] = ftell($fh);
        }
        fclose($fh);
        $b[] = $fileSize;

        $tmp = sys_get_temp_dir();
        $me = getmypid();
        $ch = [];
        
        $useIgbinary = function_exists('igbinary_serialize');

        for ($i = 0; $i < $w - 1; $i++) {
            $tf = "$tmp/p{$me}w$i";
            $pid = pcntl_fork();
            if ($pid === 0) {
                $cnt = self::worker($inputPath, $b[$i], $b[$i + 1], $pathIds, $dateIdChars, $pathCount, $dateCount);
                if ($useIgbinary) {
                    file_put_contents($tf, igbinary_serialize($cnt));
                } else {
                    file_put_contents($tf, pack('v*', ...$cnt));
                }
                exit(0);
            }
            $ch[$pid] = $tf;
        }

        $cnt = self::worker($inputPath, $b[$w - 1], $b[$w], $pathIds, $dateIdChars, $pathCount, $dateCount);

        $pending = count($ch);
        while ($pending > 0) {
            $pid = pcntl_wait($status, WNOHANG);
            if ($pid <= 0) {
                $pid = pcntl_wait($status);
            }
            $data = file_get_contents($ch[$pid]);
            unlink($ch[$pid]);
            
            if ($useIgbinary) {
                $wc = igbinary_unserialize($data);
                for ($j = 0; $j < count($wc); $j++) {
                    $cnt[$j] += $wc[$j];
                }
            } else {
                $wc = unpack('v*', $data);
                $j = 0;
                foreach ($wc as $v) $cnt[$j++] += $v;
            }
            $pending--;
        }

        self::writeJson($outputPath, $cnt, $paths, $dates, $dateCount);
    }

    private static function worker(string $f, int $s, int $e, array $pi, array $dc, int $pc, int $dateCount): array
    {
        $bk = array_fill(0, $pc, '');
        $h = fopen($f, 'rb');
        stream_set_read_buffer($h, 0);
        fseek($h, $s);
        $r = $e - $s;

        while ($r > 0) {
            $toRead = $r > self::READ_CHUNK ? self::READ_CHUNK : $r;
            $d = fread($h, $toRead);
            $l = strlen($d);
            $r -= $l;
            
            $ln = strrpos($d, "\n");
            if ($ln === false) break;
            
            $tail = $l - $ln - 1;
            if ($tail > 0) {
                fseek($h, -$tail, SEEK_CUR);
                $r += $tail;
            }

            $p = 25;
            $fc = $ln - 600;

            
            while ($p < $fc) {
                $x = strpos($d, ',', $p); $bk[$pi[substr($d, $p, $x - $p)]] .= $dc[substr($d, $x + 3, 8)]; $p = $x + 52;
                $x = strpos($d, ',', $p); $bk[$pi[substr($d, $p, $x - $p)]] .= $dc[substr($d, $x + 3, 8)]; $p = $x + 52;
                $x = strpos($d, ',', $p); $bk[$pi[substr($d, $p, $x - $p)]] .= $dc[substr($d, $x + 3, 8)]; $p = $x + 52;
                $x = strpos($d, ',', $p); $bk[$pi[substr($d, $p, $x - $p)]] .= $dc[substr($d, $x + 3, 8)]; $p = $x + 52;
                $x = strpos($d, ',', $p); $bk[$pi[substr($d, $p, $x - $p)]] .= $dc[substr($d, $x + 3, 8)]; $p = $x + 52;
            }

            while ($p < $ln) {
                $x = strpos($d, ',', $p);
                if ($x === false) break;
                $bk[$pi[substr($d, $p, $x - $p)]] .= $dc[substr($d, $x + 3, 8)];
                $p = $x + 52;
            }
        }
        fclose($h);

        $cnt = array_fill(0, $pc * $dateCount, 0);
        for ($i = 0; $i < $pc; $i++) {
            if ($bk[$i] === '') continue;
            $off = $i * $dateCount;
            foreach (array_count_values(unpack('v*', $bk[$i])) as $did => $c) $cnt[$off + $did] += $c;
        }
        return $cnt;
    }

    private static function writeJson(string $outputPath, array $cnt, array $paths, array $dates, int $dateCount): void
    {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1048576);
        fwrite($out, '{');

        $datePrefixes = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "20' . $dates[$d] . '": ';
        }

        $pc = count($paths);
        $escapedPaths = [];
        for ($p = 0; $p < $pc; $p++) {
            $escapedPaths[$p] = '"\\/blog\\/' . str_replace('/', '\\/', $paths[$p]) . '"';
        }

        $first = true;
        for ($p = 0; $p < $pc; $p++) {
            $base = $p * $dateCount;
            $de = [];
            for ($d = 0; $d < $dateCount; $d++) {
                $c = $cnt[$base + $d];
                if ($c === 0) continue;
                $de[] = $datePrefixes[$d] . $c;
            }
            if (!$de) continue;
            
            $buf = $first ? "\n    " : ",\n    ";
            $first = false;
            $buf .= $escapedPaths[$p] . ": {\n" . implode(",\n", $de) . "\n    }";
            fwrite($out, $buf);
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
