<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $datelut = [];
        for ($y = 1; $y <= 6; $y++)
            for ($mm = 1; $mm <= 12; $mm++)
                for ($dd = 1; $dd <= 31; $dd++)
                    $datelut[] = sprintf("%d-%02d-%02d", $y, $mm, $dd);
        $datelutlut = array_flip($datelut);
        $map = [];
        $file = fopen($inputPath, 'r');
        $running = true;
        while ($running) {
            $block = fread($file, 0x10000).fgets($file);
            $blen = strlen($block);
            if ($blen == 0) break; // eof
            $idx = 25;
            while ($idx < $blen) {
                $comma = strpos($block, ',', $idx);
                $path = substr($block, $idx, $comma - $idx);
                $date = $datelutlut[substr($block, $comma + 4, 7)];
                if (!isset($map[$path])) {
                    $map[$path] = array_fill(0, 2232, 0);
                    if (count($map) >= 268) {
                        $running = false;
                    }
                }
                $map[$path][$date]++;
                $idx = $comma + 52;
            }
        }
        for (;;) {
            $block = fread($file, 0x10000).fgets($file);
            $blen = strlen($block);
            if ($blen == 0) break; // eof
            $idx = 25;
            while ($idx < $blen) {
                $comma = strpos($block, ',', $idx);
                $map[substr($block, $idx, $comma - $idx)][$datelutlut[substr($block, $comma + 4, 7)]]++;
                $idx = $comma + 52;
            }
        }
        $out = fopen($outputPath, 'wb');
        fwrite($out, "{\n");
        foreach ($map as $slug => $dates) {
            $datefmt = [];
            foreach($dates as $dateid=>$count) if ($count > 0) $datefmt[] = $datelut[$dateid].'": '.$count;
            $joined = implode(",\n        \"202", $datefmt);
            fwrite($out, "    \"\\/blog\\/$slug\": {\n        \"202$joined\n    },\n");
        }
        fseek($out, -2, SEEK_CUR); // unwind last comma+newline
        fwrite($out, "\n}");
        fclose($out);
    }
}
