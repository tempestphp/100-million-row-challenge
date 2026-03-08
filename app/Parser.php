<?php

namespace App;

use App\Commands\Visit;

final class Parser
{
    private const int PREFIX_LEN = 19;     // "https://stitcher.io"
    private const int TS_LEN = 25;         // "2026-01-24T01:16:58+00:00"
    private const int DAY_LEN = 10;        // "YYYY-MM-DD"
    private const int FIXED_TAIL = self::PREFIX_LEN + 1 + self::TS_LEN; // prefix + "," + timestamp

    private const int DEFAULT_WORKERS = 8;
    // Used for internal caps/tuning during development. Kept only to avoid renumbering.
    private const int MAX_WORKERS = 16;
    private const int PARALLEL_MIN_BYTES = 128 * 1024 * 1024;

    private const int DEFAULT_READ_CHUNK = 524_288; // 512 KiB
    private const int DEFAULT_WRITE_BUF = 1_048_576; // 1 MiB
    private const int DEFAULT_READ_BUF = 65_536; // 64 KiB
    private const string DEFAULT_OUT_MODE = 'hybrid'; // scan|sort|hybrid

    /** @var array{escapedById:list<string>, idByPath: array<string,int>}|null */
    private static ?array $paths = null;

    public function parse(string $inputPath, string $outputPath): void
    {
        if (function_exists('gc_disable')) {
            gc_disable();
        }

        $size = @filesize($inputPath);
        $size = is_int($size) && $size > 0 ? $size : 0;

        $workers = self::DEFAULT_WORKERS;
        if ($size !== 0 && $size < self::PARALLEL_MIN_BYTES) {
            $workers = 1;
        }

        $readChunk = self::DEFAULT_READ_CHUNK;
        $writeBuf = self::DEFAULT_WRITE_BUF;
        $readBuf = self::DEFAULT_READ_BUF;
        $outMode = self::DEFAULT_OUT_MODE;

        $paths = self::paths();
        $pathCount = count($paths['escapedById']);

        if ($pathCount === 0) {
            file_put_contents($outputPath, "{}");
            return;
        }

        if ($workers > 1 && function_exists('pcntl_fork') && $size > 0) {
            [$order, $days, $matrix] = $this->parseParallel(
                $inputPath,
                $size,
                $workers,
                $paths['idByPath'],
                $pathCount,
                $readChunk,
                $readBuf,
            );
        } else {
            [$order, $days, $matrix] = $this->parseSlice(
                $inputPath,
                0,
                $size ?: PHP_INT_MAX,
                $paths['idByPath'],
                $pathCount,
                $readChunk,
                $readBuf,
            );
        }

        $this->emitJson($outputPath, $paths['escapedById'], $order, $days, $matrix, $writeBuf, $outMode);
    }

    private function parseParallel(
        string $inputPath,
        int $fileSize,
        int $workers,
        array $idByPath,
        int $pathCount,
        int $readChunk,
        int $readBuf,
    ): array
    {
        $cuts = $this->splitOffsets($inputPath, $fileSize, $workers);

        $tmpDir = sys_get_temp_dir();
        $pid = getmypid();
        $tmp = [];
        $kids = [];
        $ig = function_exists('igbinary_serialize') && function_exists('igbinary_unserialize');

        for ($i = 0; $i < $workers - 1; $i++) {
            $tmp[$i] = "{$tmpDir}/p{$pid}_" . ($i + 1);
            $kid = pcntl_fork();

            if ($kid < 0) {
                throw new \RuntimeException('Unable to fork parser worker');
            }

            if ($kid === 0) {
                $state = $this->parseSlice(
                    $inputPath,
                    $cuts[$i],
                    $cuts[$i + 1],
                    $idByPath,
                    $pathCount,
                    $readChunk,
                    $readBuf,
                );
                file_put_contents($tmp[$i], (string) ($ig ? igbinary_serialize($state) : serialize($state)));
                exit(0);
            }

            $kids[] = $kid;
        }

        $stateMain = $this->parseSlice(
            $inputPath,
            $cuts[$workers - 1],
            $cuts[$workers],
            $idByPath,
            $pathCount,
            $readChunk,
            $readBuf,
        );

        foreach ($kids as $kid) {
            pcntl_waitpid($kid, $status);
        }

        $order = [];
        $seen = array_fill(0, $pathCount, 0);
        $days = [];
        $dayId = [];
        $matrix = array_fill(0, $pathCount, []);

        foreach ($tmp as $path) {
            $payload = file_get_contents($path);
            @unlink($path);
            $state = $ig ? igbinary_unserialize((string) $payload) : @unserialize((string) $payload);
            $this->absorbSlice($order, $seen, $days, $dayId, $matrix, $state);
        }

        $this->absorbSlice($order, $seen, $days, $dayId, $matrix, $stateMain);

        return [$order, $days, $matrix];
    }

    private function splitOffsets(string $inputPath, int $fileSize, int $workers): array
    {
        $step = intdiv($fileSize, $workers);
        $cuts = [0];

        $h = fopen($inputPath, 'rb');
        if (! is_resource($h)) {
            return [0, $fileSize];
        }

        for ($i = 1; $i < $workers; $i++) {
            fseek($h, $i * $step);
            fgets($h);
            $cuts[] = ftell($h);
        }

        fclose($h);
        $cuts[] = $fileSize;

        return $cuts;
    }

    private function parseSlice(
        string $inputPath,
        int $start,
        int $end,
        array $idByPath,
        int $pathCount,
        int $readChunk,
        int $readBuf,
    ): array
    {
        $order = [];
        $seen = array_fill(0, $pathCount, 0);
        $days = [];
        $dayId = [];
        $dayCount = 0;
        $matrix = array_fill(0, $pathCount, []);

        $h = fopen($inputPath, 'rb');
        if (! is_resource($h)) {
            return [$order, $days, $matrix];
        }

        stream_set_read_buffer($h, $readBuf);
        fseek($h, $start);

        $left = $end - $start;
        $carry = '';

        while ($left > 0) {
            $buf = fread($h, $left > $readChunk ? $readChunk : $left);
            if ($buf === '' || $buf === false) {
                break;
            }

            $left -= strlen($buf);

            if ($carry !== '') {
                $buf = $carry . $buf;
                $carry = '';
            }

            $lastNl = strrpos($buf, "\n");
            if ($lastNl === false) {
                $carry = $buf;
                continue;
            }

            $carry = substr($buf, $lastNl + 1);
            $p = 0;

            while ($p < $lastNl) {
                $nl = strpos($buf, "\n", $p);
                if ($nl === false || $nl > $lastNl) {
                    break;
                }

                $len = $nl - $p;

                if ($len >= self::FIXED_TAIL) {
                    $pathLen = $len - self::FIXED_TAIL;

                    if ($pathLen > 0) {
                        $pathStart = $p + self::PREFIX_LEN;
                        $pid = $idByPath[substr($buf, $pathStart, $pathLen)] ?? null;

                        if (is_int($pid) && $pid >= 0) {
                            if ($seen[$pid] === 0) {
                                $seen[$pid] = 1;
                                $order[] = $pid;
                            }

                            $day = substr($buf, $nl - self::TS_LEN, self::DAY_LEN);
                            $did = $dayId[$day] ?? $dayCount;

                            if ($did === $dayCount) {
                                $dayId[$day] = $did;
                                $days[$did] = $day;
                                $dayCount++;
                            }

                            if (isset($matrix[$pid][$did])) {
                                $matrix[$pid][$did]++;
                            } else {
                                $matrix[$pid][$did] = 1;
                            }
                        }
                    }
                }

                $p = $nl + 1;
            }
        }

        if ($carry !== '') {
            $len = strlen($carry);

            if ($len >= self::FIXED_TAIL) {
                $pathLen = $len - self::FIXED_TAIL;

                if ($pathLen > 0) {
                    $pathStart = self::PREFIX_LEN;
                    $pid = $idByPath[substr($carry, $pathStart, $pathLen)] ?? null;

                    if (is_int($pid) && $pid >= 0) {
                        if ($seen[$pid] === 0) {
                            $seen[$pid] = 1;
                            $order[] = $pid;
                        }

                        $day = substr($carry, $len - self::TS_LEN, self::DAY_LEN);
                        $did = $dayId[$day] ?? $dayCount;

                        if ($did === $dayCount) {
                            $dayId[$day] = $did;
                            $days[$did] = $day;
                            $dayCount++;
                        }

                        if (isset($matrix[$pid][$did])) {
                            $matrix[$pid][$did]++;
                        } else {
                            $matrix[$pid][$did] = 1;
                        }
                    }
                }
            }
        }

        fclose($h);

        return [$order, $days, $matrix];
    }

    private function absorbSlice(array &$order, array &$seen, array &$days, array &$dayId, array &$matrix, mixed $state): void
    {
        if (! is_array($state) || count($state) !== 3) {
            return;
        }

        [$o, $d, $m] = $state;

        foreach ($o as $pid) {
            if ($seen[$pid] === 0) {
                $seen[$pid] = 1;
                $order[] = $pid;
            }
        }

        $next = count($days);
        $remap = [];

        foreach ($d as $local => $day) {
            $gid = $dayId[$day] ?? $next;

            if ($gid === $next) {
                $dayId[$day] = $gid;
                $days[] = $day;
                $next++;
            }

            $remap[$local] = $gid;
        }

        foreach ($m as $pid => $row) {
            $dst = &$matrix[$pid];

            foreach ($row as $local => $count) {
                $gid = $remap[$local];

                if (isset($dst[$gid])) {
                    $dst[$gid] += $count;
                } else {
                    $dst[$gid] = $count;
                }
            }
        }
    }

    private function emitJson(string $outputPath, array $escapedById, array $order, array $days, array $matrix, int $writeBuf, string $outMode): void
    {
        $sortedDays = $days;
        asort($sortedDays);
        $dayOrder = array_keys($sortedDays);
        $totalDays = count($dayOrder);

        $out = fopen($outputPath, 'wb');
        if (! is_resource($out)) {
            return;
        }

        stream_set_write_buffer($out, $writeBuf);
        fwrite($out, '{');

        $firstPath = true;

        foreach ($order as $pathId) {
            if (! isset($escapedById[$pathId])) {
                continue;
            }

            $row = $matrix[$pathId] ?? null;
            if (! is_array($row)) {
                continue;
            }

            $buf = $firstPath ? '' : ',';
            $firstPath = false;

            $buf .= "\n    \"{$escapedById[$pathId]}\": {";

            $firstDate = true;

            $rowCount = count($row);
            $sortRow = $outMode === 'sort' || ($outMode === 'hybrid' && $rowCount * 2 < $totalDays);

            if ($sortRow) {
                $ids = array_keys($row);
                usort($ids, static fn (int $a, int $b): int => $days[$a] <=> $days[$b]);

                foreach ($ids as $did) {
                    $count = $row[$did];

                    if ($firstDate) {
                        $buf .= "\n";
                        $firstDate = false;
                    } else {
                        $buf .= ",\n";
                    }

                    $buf .= "        \"{$days[$did]}\": {$count}";
                }
            } else {
                foreach ($dayOrder as $did) {
                    if (! isset($row[$did])) {
                        continue;
                    }

                    $count = $row[$did];

                    if ($firstDate) {
                        $buf .= "\n";
                        $firstDate = false;
                    } else {
                        $buf .= ",\n";
                    }

                    $buf .= "        \"{$days[$did]}\": {$count}";
                }
            }

            $buf .= "\n    }";
            fwrite($out, $buf);
        }

        fwrite($out, "\n}");
        fclose($out);
    }

    private static function paths(): array
    {
        if (self::$paths !== null) {
            return self::$paths;
        }

        $escapedById = [];
        $idByPath = [];

        foreach (Visit::all() as $id => $visit) {
            $uri = $visit->uri ?? null;

            if (! is_string($uri) || $uri === '') {
                continue;
            }

            $path = substr($uri, self::PREFIX_LEN);
            $escapedById[$id] = str_replace('/', '\\/', $path);
            $idByPath[$path] = $id;
        }

        self::$paths = [
            'escapedById' => $escapedById,
            'idByPath' => $idByPath,
        ];

        return self::$paths;
    }
}
