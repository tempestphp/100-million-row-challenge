<?php

namespace App;

use App\Commands\Visit;

use function array_fill;
use function array_flip;
use function fclose;
use function feof;
use function fopen;
use function fread;
use function fseek;
use function fwrite;
use function gc_disable;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;

use const SEEK_CUR;

final class Parser
{
    private const string URI_PREFIX = 'https://stitcher.io/blog/';

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $dateIds = [];
        $days = 0;
        for ($y = 1; $y <= 6; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $ms = $m < 10 ? "0$m" : "$m";
                $daysInMonth = match ($m) {
                    4, 6, 9, 11 => 30,
                    2 => $y === 4 ? 29 : 28,
                    default => 31,
                };

                for ($d = 1; $d <= $daysInMonth; $d++) {
                    $ds = $d < 10 ? "0$d" : "$d";
                    $dateIds["$y-$ms-$ds"] = $days++;
                }
            }
        }

        $uris = array_column(Visit::all(), 'uri');

        $slugKeyLen = 1;
        while (true) {
            $keys = [];
            foreach ($uris as $uri) {
                $key = substr($uri, -$slugKeyLen);
                if (isset($keys[$key])) {
                    $slugKeyLen++; continue 2;
                }
                $keys[$key] = true;
            }
            break;
        }

        $slugMap = [];
        foreach ($uris as $i => $uri) {
            $slug = substr($uri, 25);
            $slugId = $i * $days;
            $slugMap[substr($uri, -$slugKeyLen)] = (strlen($slug) << 20) | $slugId;
        }
        $slugCount = $i + 1;

        $slugSorted = [];
        $slugSortedCount = 0;
        $fp = fopen($inputPath, 'r');
        stream_set_read_buffer($fp, 0);

        $sample = fread($fp, 181_000);
        $lastNewline = strrpos($sample, "\n");
        $pos = 25;
        while ($pos < $lastNewline && $slugSortedCount !== $slugCount) {
            $comma = strpos($sample, ',', $pos);
            $slug = substr($sample, $pos, $comma - $pos);
            $pos = $comma + 52;

            if (!isset($slugSorted[$slug])) {
                $slugSorted[$slug] = $slugSortedCount++;
            }
        }
        $slugSorted = array_flip($slugSorted);

        fseek($fp, 0);
        $counts = array_fill(0, $slugCount * $days, 0);
        $slugKeyOffset = $slugKeyLen + 26;
        while (!feof($fp)) {
            $chunk = fread($fp, 1_048_576);

            $lastNewline = strrpos($chunk, "\n");

            $pos = $lastNewline;
            while ($pos > 25) {
                $s = $slugMap[substr($chunk, $pos - $slugKeyOffset, $slugKeyLen)];
                $counts[($s & 1048575) + $dateIds[substr($chunk, $pos - 22, 7)]]++;
                $pos -= 52 + ($s >> 20);

                if ($pos <= 25) break;
                $s = $slugMap[substr($chunk, $pos - $slugKeyOffset, $slugKeyLen)];
                $counts[($s & 1048575) + $dateIds[substr($chunk, $pos - 22, 7)]]++;
                $pos -= 52 + ($s >> 20);

                if ($pos <= 25) break;
                $s = $slugMap[substr($chunk, $pos - $slugKeyOffset, $slugKeyLen)];
                $counts[($s & 1048575) + $dateIds[substr($chunk, $pos - 22, 7)]]++;
                $pos -= 52 + ($s >> 20);

                if ($pos <= 25) break;
                $s = $slugMap[substr($chunk, $pos - $slugKeyOffset, $slugKeyLen)];
                $counts[($s & 1048575) + $dateIds[substr($chunk, $pos - 22, 7)]]++;
                $pos -= 52 + ($s >> 20);

                if ($pos <= 25) break;
                $s = $slugMap[substr($chunk, $pos - $slugKeyOffset, $slugKeyLen)];
                $counts[($s & 1048575) + $dateIds[substr($chunk, $pos - 22, 7)]]++;
                $pos -= 52 + ($s >> 20);

                if ($pos <= 25) break;
                $s = $slugMap[substr($chunk, $pos - $slugKeyOffset, $slugKeyLen)];
                $counts[($s & 1048575) + $dateIds[substr($chunk, $pos - 22, 7)]]++;
                $pos -= 52 + ($s >> 20);

                if ($pos <= 25) break;
                $s = $slugMap[substr($chunk, $pos - $slugKeyOffset, $slugKeyLen)];
                $counts[($s & 1048575) + $dateIds[substr($chunk, $pos - 22, 7)]]++;
                $pos -= 52 + ($s >> 20);

                if ($pos <= 25) break;
                $s = $slugMap[substr($chunk, $pos - $slugKeyOffset, $slugKeyLen)];
                $counts[($s & 1048575) + $dateIds[substr($chunk, $pos - 22, 7)]]++;
                $pos -= 52 + ($s >> 20);
            }

            $leftover = strlen($chunk) - $lastNewline - 1;
            if ($leftover > 0) {
                fseek($fp, -$leftover, SEEK_CUR);
            }
        }
        fclose($fp);

        $fp = fopen($outputPath, 'w');
        stream_set_write_buffer($fp, 1_048_576);
        fwrite($fp, '{');
        $firstSlug = true;
        foreach ($slugSorted as $slug) {
            $buffer = $firstSlug ? '' : ',';
            $buffer .= "\n    \"\/blog\/$slug\": {";
            $slugKey = substr(self::URI_PREFIX . $slug, -$slugKeyLen);
            $s = $slugMap[$slugKey];

            $d = reset($dateIds);
            $date = key($dateIds);
            $count = $counts[($s & 1048575) + $d];
            $buffer .= "\n        \"202$date\": $count";

            while (($date = key($dateIds)) !== null) {
                $d = current($dateIds);
                $count = $counts[($s & 1048575) + $d];
                if ($count !== 0) {
                    $buffer .= ",\n        \"202$date\": $count";
                }

                next($dateIds);
            }
            $buffer .= "\n    }";
            fwrite($fp, $buffer);

            $firstSlug = false;
        }
        fwrite($fp, "\n}");
        fclose($fp);
    }
}