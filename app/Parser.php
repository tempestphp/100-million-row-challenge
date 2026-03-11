<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();
        ini_set('memory_limit', '-1');

        $dateIds    = [];
        $dateLabels = [];
        $nd         = 0;

        for ($y = 1; $y <= 6; $y++) {
            $fy = 2020 + $y;
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2           => ($fy % 4 === 0 ? 29 : 28),
                    4, 6, 9, 11 => 30,
                    default     => 31,
                };
                $ms = $m < 10 ? "0$m" : (string) $m;
                for ($d = 1; $d <= $maxD; $d++) {
                    $ds = $d < 10 ? "0$d" : (string) $d;
                    $dateIds["$y-$ms-$ds"] = $nd;
                    $dateLabels[$nd]       = "$fy-$ms-$ds";
                    $nd++;
                }
            }
        }

        $fh       = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);
        $fileSize = filesize($inputPath);
        $probe    = fread($fh, min($fileSize, 2 * 1024 * 1024));

        $d1      = strpos($probe, '/', 8);
        $slugOff = strpos($probe, '/', $d1 + 1) + 1;
        $jump    = 27 + $slugOff;

        $slugIndex  = [];
        $slugLabels = [];
        $ns         = 0;
        $pp         = $slugOff;
        $probeLen   = strlen($probe);

        while ($pp < $probeLen && ($ci = strpos($probe, ',', $pp)) !== false) {
            $slug = substr($probe, $pp, $ci - $pp);
            if (!isset($slugIndex[$slug])) {
                $slugIndex[$slug]  = $ns;
                $slugLabels[$ns++] = $slug;
            }
            $pp = $ci + $jump;
        }
        unset($probe);
        $counts = array_fill(0, $ns * $nd, 0);

        rewind($fh);
        $rem = $fileSize;
        $cs  = 8 * 1024 * 1024;

        while ($rem > 0) {
            $chunk = fread($fh, min($cs, $rem));
            $clen  = strlen($chunk);
            if ($clen === 0) break;
            $rem -= $clen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) continue;

            $over = $clen - $lastNl - 1;
            if ($over > 0) {
                fseek($fh, -$over, SEEK_CUR);
                $rem += $over;
            }

            $bp   = $slugOff;
            $safe = $lastNl - 900;

            while ($bp < $safe) {
                $ci = strpos($chunk, ',', $bp);
                $counts[$slugIndex[substr($chunk, $bp, $ci - $bp)] * $nd + $dateIds[substr($chunk, $ci + 4, 7)]]++;
                $bp = $ci + $jump;

                $ci = strpos($chunk, ',', $bp);
                $counts[$slugIndex[substr($chunk, $bp, $ci - $bp)] * $nd + $dateIds[substr($chunk, $ci + 4, 7)]]++;
                $bp = $ci + $jump;

                $ci = strpos($chunk, ',', $bp);
                $counts[$slugIndex[substr($chunk, $bp, $ci - $bp)] * $nd + $dateIds[substr($chunk, $ci + 4, 7)]]++;
                $bp = $ci + $jump;

                $ci = strpos($chunk, ',', $bp);
                $counts[$slugIndex[substr($chunk, $bp, $ci - $bp)] * $nd + $dateIds[substr($chunk, $ci + 4, 7)]]++;
                $bp = $ci + $jump;

                $ci = strpos($chunk, ',', $bp);
                $counts[$slugIndex[substr($chunk, $bp, $ci - $bp)] * $nd + $dateIds[substr($chunk, $ci + 4, 7)]]++;
                $bp = $ci + $jump;

                $ci = strpos($chunk, ',', $bp);
                $counts[$slugIndex[substr($chunk, $bp, $ci - $bp)] * $nd + $dateIds[substr($chunk, $ci + 4, 7)]]++;
                $bp = $ci + $jump;

                $ci = strpos($chunk, ',', $bp);
                $counts[$slugIndex[substr($chunk, $bp, $ci - $bp)] * $nd + $dateIds[substr($chunk, $ci + 4, 7)]]++;
                $bp = $ci + $jump;

                $ci = strpos($chunk, ',', $bp);
                $counts[$slugIndex[substr($chunk, $bp, $ci - $bp)] * $nd + $dateIds[substr($chunk, $ci + 4, 7)]]++;
                $bp = $ci + $jump;
            }

            while ($bp <= $lastNl) {
                $ci = strpos($chunk, ',', $bp);
                if ($ci === false || $ci > $lastNl) break;
                $counts[$slugIndex[substr($chunk, $bp, $ci - $bp)] * $nd + $dateIds[substr($chunk, $ci + 4, 7)]]++;
                $bp = $ci + $jump;
            }
        }

        fclose($fh);

        $fo = fopen($outputPath, 'wb');
        stream_set_write_buffer($fo, 1024 * 1024);

        fwrite($fo, '{');
        $first = true;

        for ($sid = 0; $sid < $ns; $sid++) {
            $base  = $sid * $nd;
            $body  = '';
            $comma = '';
            for ($did = 0; $did < $nd; $did++) {
                $c = $counts[$base + $did];
                if ($c === 0) continue;
                $body  .= $comma . "\n        \"" . $dateLabels[$did] . "\": " . $c;
                $comma  = ',';
            }
            if ($body === '') continue;

            fwrite($fo, ($first ? '' : ',') . "\n    \"\\/blog\\/" . $slugLabels[$sid] . "\": {" . $body . "\n    }");
            $first = false;
        }

        fwrite($fo, "\n}");
        fclose($fo);
    }
}
