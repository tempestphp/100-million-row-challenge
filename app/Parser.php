<?php

namespace App;

final class Parser
{
    private const int CHUNK_SIZE = 512 * 1024;
    private const int PREFIX_LEN = 25;

    public function parse(string $inputPath, string $outputPath): void
    {
        \gc_disable();

        $dateMap = [];
        $dateStrings = [];
        $dIdx = 0;

        $paddings = [];
        for ($i = 1; $i <= 31; $i++) {
            $paddings[$i] = ($i < 10 ? '0' : '') . $i;
        }
        for ($y = 2020; $y <= 2026; $y++) {
            $yStr = $y . '-';
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => (($y % 4 === 0 && ($y % 100 !== 0 || $y % 400 === 0)) ? 29 : 28),
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $ymStr = $yStr . $paddings[$m] . '-';
                for ($d = 1; $d <= $maxD; $d++) {
                    $key = $ymStr . $paddings[$d];
                    $dateMap[$key] = $dIdx;
                    $dateStrings[$dIdx] = $key;
                    $dIdx++;
                }
            }
        }
        $totalDates = $dIdx;

        $pathIds = [];
        $paths = [];
        $counts = [];
        $pathCount = 0;

        $h = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($h, 0);

        while (!\feof($h)) {
            $chunk = \fread($h, self::CHUNK_SIZE);
            if ($chunk === '' || $chunk === false) break;

            $lastNl = \strrpos($chunk, "\n");
            if ($lastNl === false) continue;

            $extra = \strlen($chunk) - $lastNl - 1;
            if ($extra > 0) {
                \fseek($h, -$extra, \SEEK_CUR);
            }

            $p = 0;
            while ($p < $lastNl) {
                $comma = \strpos($chunk, ',', $p + self::PREFIX_LEN);
                if ($comma === false || $comma > $lastNl) break;

                $slug = \substr($chunk, $p + self::PREFIX_LEN, $comma - ($p + self::PREFIX_LEN));
                $dateKey = \substr($chunk, $comma + 1, 10);

                $p = \strpos($chunk, "\n", $comma) + 1;

                if (!isset($pathIds[$slug])) {
                    $pathIds[$slug] = $pathCount;
                    $paths[$pathCount] = $slug;
                    for ($i = 0; $i < $totalDates; $i++) {
                        $counts[$pathCount * $totalDates + $i] = 0;
                    }
                    $pathCount++;
                }

                if (isset($dateMap[$dateKey])) {
                    $counts[($pathIds[$slug] * $totalDates) + $dateMap[$dateKey]]++;
                }
            }
        }
        \fclose($h);

        $this->writeFinalJson($outputPath, $paths, $dateStrings, $counts, $totalDates);
    }

    private function writeFinalJson($outPath, $paths, $dateStrings, $counts, $totalDates): void
    {
        $fp = \fopen($outPath, 'wb');
        \fwrite($fp, "{\n");

        $isFirstPath = true;
        foreach ($paths as $pIdx => $slug) {
            $base = $pIdx * $totalDates;
            $entries = [];

            for ($d = 0; $d < $totalDates; $d++) {
                $val = $counts[$base + $d];
                if ($val > 0) {
                    $entries[] = "        \"{$dateStrings[$d]}\": {$val}";
                }
            }

            if (!empty($entries)) {
                if (!$isFirstPath) \fwrite($fp, ",\n");
                $escaped = \str_replace('/', '\/', $slug);
                \fwrite($fp, "    \"\/blog\/{$escaped}\": {\n" . \implode(",\n", $entries) . "\n    }");
                $isFirstPath = false;
            }
        }
        \fwrite($fp, "\n}");
        \fclose($fp);
    }
}