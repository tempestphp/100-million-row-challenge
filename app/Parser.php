<?php

namespace App;

use App\Commands\Visit;

final class Parser
{
    private const int WORKERS = 8;
    private const int READ_CHUNK = 4_194_304;
    private const int WRITE_BUFFER = 1_048_576;
    private const int DISCOVER_SIZE = 1_048_576;

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $fileSize = filesize($inputPath);

        // Short dates: "yy-mm-dd" (8 chars) — prepend "20" in output
        $dateIds = [];
        $dates = [];
        $dateCount = 0;

        for ($y = 20; $y <= 26; $y++) {
            $yStr = ($y < 10 ? '0' : '') . $y;
            for ($m = 1; $m <= 12; $m++) {
                $mStr = ($m < 10 ? '0' : '') . $m;
                $ymStr = $yStr . '-' . $mStr . '-';
                $dim = match ($m) {
                    2 => (($y + 2000) % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                for ($d = 1; $d <= $dim; $d++) {
                    $key = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $dateIds[$key] = $dateCount;
                    $dates[$dateCount] = $key;
                    $dateCount++;
                }
            }
        }

        // Discover slugs from first chunk + Visit::all() fallback
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $chunk = fread($handle, min($fileSize, self::DISCOVER_SIZE));
        fclose($handle);

        $lastNl = strrpos($chunk, "\n");
        $pathIds = [];
        $paths = [];
        $pathCount = 0;
        $pos = 0;

        while ($pos < $lastNl) {
            $nlPos = strpos($chunk, "\n", $pos + 52);
            if ($nlPos === false) break;

            $slug = substr($chunk, $pos + 25, $nlPos - $pos - 51);
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug] = $pathCount * $dateCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }

            $pos = $nlPos + 1;
        }
        unset($chunk);

        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, 25);
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug] = $pathCount * $dateCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }
        }

        // Chunk boundaries
        $nw = ($fileSize >= 10_000_000 && function_exists('pcntl_fork'))
            ? self::WORKERS : 1;

        $bounds = [0];
        if ($nw > 1) {
            $fh = fopen($inputPath, 'rb');
            for ($i = 1; $i < $nw; $i++) {
                fseek($fh, (int)($fileSize * $i / $nw));
                fgets($fh);
                $bounds[] = ftell($fh);
            }
            fclose($fh);
        }
        $bounds[] = $fileSize;

        if ($nw === 1) {
            $merged = self::scan(
                $inputPath, 0, $fileSize,
                $pathIds, $dateIds, $pathCount, $dateCount,
            );
        } else {
            $tmpDir = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
            $myPid = getmypid();
            $children = [];

            for ($i = 0; $i < $nw - 1; $i++) {
                $tf = $tmpDir . '/p_' . $myPid . '_' . $i;
                $cpid = pcntl_fork();

                if ($cpid === 0) {
                    $d = self::scan(
                        $inputPath, $bounds[$i], $bounds[$i + 1],
                        $pathIds, $dateIds, $pathCount, $dateCount,
                    );
                    file_put_contents($tf, pack('V*', ...$d));
                    exit(0);
                }

                $children[] = [$cpid, $tf];
            }

            $merged = self::scan(
                $inputPath, $bounds[$nw - 1], $bounds[$nw],
                $pathIds, $dateIds, $pathCount, $dateCount,
            );

            foreach ($children as [$cpid, $tf]) {
                pcntl_waitpid($cpid, $st);
                $wc = unpack('V*', file_get_contents($tf));
                unlink($tf);
                $j = 0;
                foreach ($wc as $v) {
                    $merged[$j++] += $v;
                }
            }
        }

        // JSON output
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, self::WRITE_BUFFER);
        fwrite($out, '{');
        $firstPath = true;
        $pc = count($paths);

        for ($p = 0; $p < $pc; $p++) {
            $base = $p * $dateCount;
            $firstDate = true;
            $dateBuf = '';

            for ($di = 0; $di < $dateCount; $di++) {
                $c = $merged[$base + $di];
                if ($c === 0) {
                    continue;
                }

                if (!$firstDate) {
                    $dateBuf .= ",\n";
                }
                $firstDate = false;
                $dateBuf .= '        "20' . $dates[$di] . '": ' . $c;
            }

            if ($firstDate) {
                continue;
            }

            $buf = $firstPath ? '' : ',';
            $firstPath = false;
            $buf .= "\n    \"\/blog\/" . str_replace('/', '\\/', $paths[$p]) . "\": {\n" . $dateBuf . "\n    }";
            fwrite($out, $buf);
        }

        fwrite($out, "\n}");
        fclose($out);
    }

    private static function scan(
        string $inputPath,
        int $start,
        int $end,
        array $pathIds,
        array $dateIds,
        int $pathCount,
        int $dateCount,
    ): array {
        $counts = array_fill(0, $pathCount * $dateCount, 0);

        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);
        fseek($fh, $start);

        $rem = $end - $start;

        while ($rem > 0) {
            $chunk = fread($fh, $rem > self::READ_CHUNK ? self::READ_CHUNK : $rem);
            $cLen = strlen($chunk);
            if ($cLen === 0) break;
            $rem -= $cLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) {
                fseek($fh, -$cLen, SEEK_CUR);
                $rem += $cLen;
                break;
            }

            $tail = $cLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($fh, -$tail, SEEK_CUR);
                $rem += $tail;
            }

            $pos = 0;

            while ($pos < $lastNl) {
                $nl = strpos($chunk, "\n", $pos + 52);

                // Pre-multiplied pathId + short 8-char date — no null checks
                $counts[$pathIds[substr($chunk, $pos + 25, $nl - $pos - 51)] + $dateIds[substr($chunk, $nl - 23, 8)]]++;

                $pos = $nl + 1;
            }
        }

        fclose($fh);

        return $counts;
    }
}
