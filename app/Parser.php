<?php

namespace App;

use App\Commands\Visit;

use function strpos;
use function strrpos;
use function substr;
use function strlen;
use function fopen;
use function fclose;
use function fread;
use function fseek;
use function ftell;
use function fgets;
use function fwrite;
use function filesize;
use function gc_disable;
use function ini_set;
use function pcntl_fork;
use function pcntl_wait;
use function pack;
use function unpack;
use function chr;
use function array_fill;
use function array_count_values;
use function implode;
use function str_replace;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function sys_get_temp_dir;
use function file_get_contents;
use function file_put_contents;
use function getmypid;
use function unlink;
use function min;
use const SEEK_CUR;
use const WNOHANG;

final class Parser
{
    private const int WORKERS = 10;
    private const int CHUNK_SIZE = 131_072;

    public static function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();
        ini_set('memory_limit', '-1');

        $fileSize = filesize($inputPath);

        // Build ID mappings
        [$slugMap, $slugOrder, $slugCount, $dateBin, $dateStr, $numDates] =
            self::buildMappings($inputPath, $fileSize);

        // Split file into worker chunks at newline boundaries
        $offsets = [0];
        $fh = fopen($inputPath, 'rb');
        for ($i = 1; $i < self::WORKERS; $i++) {
            fseek($fh, (int)($fileSize * $i / self::WORKERS));
            fgets($fh);
            $offsets[] = ftell($fh);
        }
        fclose($fh);
        $offsets[] = $fileSize;

        // Fork children for chunks 0..N-2, parent takes last chunk
        $tmpDir = sys_get_temp_dir();
        $pid = getmypid();
        $childMap = [];

        for ($w = 0; $w < self::WORKERS - 1; $w++) {
            $childPid = pcntl_fork();
            if ($childPid === 0) {
                ini_set('memory_limit', '-1');
                $bins = self::processChunk(
                    $inputPath, $offsets[$w], $offsets[$w + 1],
                    $slugMap, $dateBin, $slugCount, $numDates,
                );
                $idxBin = '';
                $cntBin = '';
                $nEntries = 0;
                for ($sid = 0; $sid < $slugCount; $sid++) {
                    if ($bins[$sid] === '') continue;
                    $base = $sid * $numDates;
                    foreach (array_count_values(unpack('v*', $bins[$sid])) as $did => $n) {
                        $idxBin .= pack('V', $base + $did);
                        $cntBin .= pack('v', $n);
                        $nEntries++;
                    }
                }
                file_put_contents("{$tmpDir}/rc_{$pid}_{$w}", pack('V', $nEntries) . $idxBin . $cntBin);
                \posix_kill(\posix_getpid(), 9);
            }
            $childMap[$childPid] = $w;
        }

        $bins = self::processChunk(
            $inputPath, $offsets[self::WORKERS - 1], $offsets[self::WORKERS],
            $slugMap, $dateBin, $slugCount, $numDates,
        );
        $result = array_fill(0, $slugCount * $numDates, 0);
        for ($sid = 0; $sid < $slugCount; $sid++) {
            if ($bins[$sid] === '') continue;
            $base = $sid * $numDates;
            foreach (array_count_values(unpack('v*', $bins[$sid])) as $did => $n) {
                $result[$base + $did] = $n;
            }
        }
        unset($bins);

        // Merge child results as they finish (WNOHANG)
        $pending = self::WORKERS - 1;
        while ($pending > 0) {
            $reaped = pcntl_wait($status, WNOHANG);
            if ($reaped <= 0) {
                $reaped = pcntl_wait($status);
            }
            $w = $childMap[$reaped];
            $file = "{$tmpDir}/rc_{$pid}_{$w}";
            $raw = file_get_contents($file);
            unlink($file);
            $n = unpack('V', $raw)[1];
            $idxArr = unpack("V{$n}", $raw, 4);
            $cntArr = unpack("v{$n}", $raw, 4 + $n * 4);
            for ($i = 1; $i <= $n; $i++) {
                $result[$idxArr[$i]] += $cntArr[$i];
            }
            $pending--;
        }

        self::writeOutput($outputPath, $result, $slugOrder, $slugCount, $dateStr, $numDates);
    }

    private static function buildMappings(string $inputPath, int $fileSize): array
    {
        // Paths: sample file for first-seen order, then Visit::all() for completeness
        $slugMap = [];
        $slugOrder = [];
        $slugCount = 0;

        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);
        $sample = fread($fh, min($fileSize, 2_097_152));
        fclose($fh);

        $lastNl = strrpos($sample, "\n");
        $pos = 0;
        while ($pos < $lastNl) {
            $nl = strpos($sample, "\n", $pos + 52);
            if ($nl === false) break;
            $slug = substr($sample, $pos + 25, $nl - $pos - 51);
            if (!isset($slugMap[$slug])) {
                $slugMap[$slug] = $slugCount;
                $slugOrder[$slugCount] = $slug;
                $slugCount++;
            }
            $pos = $nl + 1;
        }
        unset($sample);

        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, 25);
            if (!isset($slugMap[$slug])) {
                $slugMap[$slug] = $slugCount;
                $slugOrder[$slugCount] = $slug;
                $slugCount++;
            }
        }

        // Dates: enumerate calendar 2021-2026 as 2-byte binary tokens
        $dateBin = [];
        $dateStr = [];
        $numDates = 0;
        for ($y = 21; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => ($y % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $ms = $m < 10 ? "0{$m}" : (string)$m;
                for ($d = 1; $d <= $maxD; $d++) {
                    $ds = $d < 10 ? "0{$d}" : (string)$d;
                    $dateBin["{$y}-{$ms}-{$ds}"] = chr($numDates & 0xFF) . chr($numDates >> 8);
                    $dateStr[$numDates] = "20{$y}-{$ms}-{$ds}";
                    $numDates++;
                }
            }
        }

        return [$slugMap, $slugOrder, $slugCount, $dateBin, $dateStr, $numDates];
    }

    private static function processChunk(
        string $path, int $start, int $end,
        array $slugMap, array $dateBin,
        int $slugCount, int $numDates,
    ): array {
        $bins = array_fill(0, $slugCount, '');
        $fh = fopen($path, 'rb');
        stream_set_read_buffer($fh, 0);
        fseek($fh, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $chunk = fread($fh, $remaining > self::CHUNK_SIZE ? self::CHUNK_SIZE : $remaining);
            $cLen = strlen($chunk);
            if ($cLen === 0) break;
            $remaining -= $cLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) break;

            $tail = $cLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($fh, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $p = 25;
            $fence = $lastNl - 900;

            if ($p < $fence) {
                do {
                    $c = strpos($chunk, ',', $p);
                    $bins[$slugMap[substr($chunk, $p, $c - $p)]] .= $dateBin[substr($chunk, $c + 3, 8)];
                    $p = $c + 52;

                    $c = strpos($chunk, ',', $p);
                    $bins[$slugMap[substr($chunk, $p, $c - $p)]] .= $dateBin[substr($chunk, $c + 3, 8)];
                    $p = $c + 52;

                    $c = strpos($chunk, ',', $p);
                    $bins[$slugMap[substr($chunk, $p, $c - $p)]] .= $dateBin[substr($chunk, $c + 3, 8)];
                    $p = $c + 52;

                    $c = strpos($chunk, ',', $p);
                    $bins[$slugMap[substr($chunk, $p, $c - $p)]] .= $dateBin[substr($chunk, $c + 3, 8)];
                    $p = $c + 52;

                    $c = strpos($chunk, ',', $p);
                    $bins[$slugMap[substr($chunk, $p, $c - $p)]] .= $dateBin[substr($chunk, $c + 3, 8)];
                    $p = $c + 52;

                    $c = strpos($chunk, ',', $p);
                    $bins[$slugMap[substr($chunk, $p, $c - $p)]] .= $dateBin[substr($chunk, $c + 3, 8)];
                    $p = $c + 52;

                    $c = strpos($chunk, ',', $p);
                    $bins[$slugMap[substr($chunk, $p, $c - $p)]] .= $dateBin[substr($chunk, $c + 3, 8)];
                    $p = $c + 52;

                    $c = strpos($chunk, ',', $p);
                    $bins[$slugMap[substr($chunk, $p, $c - $p)]] .= $dateBin[substr($chunk, $c + 3, 8)];
                    $p = $c + 52;
                } while ($p < $fence);
            }

            while ($p < $lastNl) {
                $c = strpos($chunk, ',', $p);
                if ($c === false || $c >= $lastNl) break;
                $bins[$slugMap[substr($chunk, $p, $c - $p)]] .= $dateBin[substr($chunk, $c + 3, 8)];
                $p = $c + 52;
            }
        }

        fclose($fh);

        return $bins;
    }

    private static function writeOutput(
        string $outputPath, array $result,
        array $slugOrder, int $slugCount,
        array $dateStr, int $numDates,
    ): void {
        $jsonDate = [];
        for ($d = 0; $d < $numDates; $d++) {
            $jsonDate[$d] = '        "' . $dateStr[$d] . '": ';
        }

        $jsonSlug = [];
        for ($p = 0; $p < $slugCount; $p++) {
            $jsonSlug[$p] = '"\\/blog\\/' . str_replace('/', '\\/', $slugOrder[$p]) . '"';
        }

        $fp = fopen($outputPath, 'wb');
        stream_set_write_buffer($fp, 1_048_576);

        fwrite($fp, '{');
        $first = true;

        for ($p = 0; $p < $slugCount; $p++) {
            $base = $p * $numDates;
            $dateEntries = [];

            for ($d = 0; $d < $numDates; $d++) {
                $n = $result[$base + $d];
                if ($n === 0) continue;
                $dateEntries[] = $jsonDate[$d] . $n;
            }

            if ($dateEntries === []) continue;

            fwrite($fp, ($first ? '' : ',') . "\n    " . $jsonSlug[$p] . ": {\n" . implode(",\n", $dateEntries) . "\n    }");
            $first = false;
        }

        fwrite($fp, "\n}");
        fclose($fp);
    }
}
