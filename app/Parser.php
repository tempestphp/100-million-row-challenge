<?php
declare(strict_types=1);

namespace App;

use Exception;

final class Parser
{
    private const int URL_PREFIX = 19;
    private const int DATE_LEN = 8;
    private const int PRE_SCAN_BYTES = 2 * 1024 * 1024;
    private const int READ_BUF = 160 * 1024;
    private const int WORKER_COUNT = 8;
    private const int WRITE_BUF = 1024 * 1024;

    /**
     * @param string $inputPath
     * @param string $outputPath
     * @return void
     * @throws Exception
     */
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $fileSize = filesize($inputPath);
        $dateIdByStr = [];
        $dateStrById = [];
        $numDates = 0;

        for ($y = 21; $y <= 26; $y++) {
            $yy = (string)$y;
            for ($m = 1; $m <= 12; $m++) {
                $daysInMonth = match ($m) {
                    2 => $y === 24 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $mm = $m < 10 ? sprintf('0%d', $m) : (string)$m;
                $ym = sprintf('%s-%s-', $yy, $mm);
                for ($d = 1; $d <= $daysInMonth; $d++) {
                    $dd = $d < 10 ? sprintf('0%d', $d) : (string)$d;
                    $dateStr = sprintf('%s%s', $ym, $dd);
                    $dateIdByStr[$dateStr] = $numDates;
                    $dateStrById[$numDates] = $dateStr;
                    $numDates++;
                }
            }
        }

        $incr = [];
        for ($c = 0; $c < 256; $c++) {
            $incr[chr($c)] = chr(($c + 1) % 256);
        }

        $slugToId = [];
        $idToSlug = [];
        $numSlugs = 0;
        $this->discoverSlugs($inputPath, $fileSize, $slugToId, $idToSlug, $numSlugs);

        $slugBase = array_map(function ($id) use ($numDates) {
            return $id * $numDates;
        }, $slugToId);

        $cellCount = $numSlugs * $numDates;
        $boundaries = $this->computeBoundaries($inputPath, $fileSize);

        $sockets = [];
        for ($w = 0; $w < (count($boundaries) - 1); $w++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            if ($pair === false) {
                throw new Exception('stream_socket_pair failed');
            }
            stream_set_chunk_size($pair[0], $cellCount);
            stream_set_chunk_size($pair[1], $cellCount);

            $pid = pcntl_fork();
            if ($pid === 0) {
                fclose($pair[0]);
                $cells = str_repeat("\0", $cellCount);
                $this->processRange(
                    $inputPath,
                    $boundaries[$w],
                    $boundaries[$w + 1],
                    $slugBase,
                    $dateIdByStr,
                    $incr,
                    $cells
                );
                fwrite($pair[1], $cells);
                fclose($pair[1]);
                exit(0);
            }

            fclose($pair[1]);
            $sockets[(int)$pair[0]] = $pair[0];
        }

        $counts = null;
        while ($sockets !== []) {
            $read = $sockets;
            $write = [];
            $except = [];
            stream_select($read, $write, $except, 5);
            foreach ($read as $sock) {
                $blob = stream_get_contents($sock);
                fclose($sock);
                unset($sockets[(int)$sock]);
                $bytes = unpack('C*', $blob);
                if ($counts === null) {
                    $counts = $bytes;
                } else {
                    foreach ($bytes as $k => $v) {
                        $counts[$k] += $v;
                    }
                }
            }
        }

        $this->writeResult($outputPath, $counts, $idToSlug, $dateStrById, $numSlugs, $numDates);
    }

    /**
     * @param string $path
     * @param int    $fileSize
     * @param array  $slugToId
     * @param array  $idToSlug
     * @param int    $numSlugs
     * @return void
     */
    private function discoverSlugs(
        string $path,
        int    $fileSize,
        array  &$slugToId,
        array  &$idToSlug,
        int    &$numSlugs
    ): void
    {
        $fh = fopen($path, 'rb');
        stream_set_read_buffer($fh, 0);
        $head = fread($fh, min(self::PRE_SCAN_BYTES, $fileSize));
        fclose($fh);

        $pos = 0;
        $lastNewline = strrpos($head, "\n");
        if ($lastNewline === false) {
            $lastNewline = strlen($head);
        }

        $minLineLen = self::URL_PREFIX + 1 + 2 + self::DATE_LEN + 1 + 20;
        while ($pos < $lastNewline) {
            $nl = strpos($head, "\n", $pos);
            if ($nl === false) {
                break;
            }
            $lineLen = $nl - $pos;
            if ($lineLen < $minLineLen) {
                $pos = $nl + 1;
                continue;
            }
            $comma = strpos($head, ',', $pos + self::URL_PREFIX);
            if ($comma === false || $comma > $nl) {
                $pos = $nl + 1;
                continue;
            }
            $slug = substr($head, $pos + self::URL_PREFIX, $comma - $pos - self::URL_PREFIX);
            if (!isset($slugToId[$slug])) {
                $slugToId[$slug] = $numSlugs;
                $idToSlug[$numSlugs] = $slug;
                $numSlugs++;
            }
            $pos = $nl + 1;
        }
    }

    /**
     * @param string $path
     * @param int    $fileSize
     * @return int[]
     */
    private function computeBoundaries(
        string $path,
        int    $fileSize
    ): array
    {
        $n = self::WORKER_COUNT;
        $boundaries = [0];
        if ($fileSize < 1024 * 1024) {
            $boundaries[] = $fileSize;
            return $boundaries;
        }
        $step = (int)($fileSize / $n);
        $fh = fopen($path, 'rb');
        for ($i = 1; $i < $n; $i++) {
            $seek = (int)($i * $step);
            fseek($fh, $seek);
            fgets($fh);
            $boundaries[] = ftell($fh);
        }
        fclose($fh);
        $boundaries[] = $fileSize;
        return $boundaries;
    }

    /**
     * @param string $path
     * @param int    $start
     * @param int    $end
     * @param array  $slugBase
     * @param array  $dateIdByStr
     * @param array  $incr
     * @param string $cells
     * @return void
     */
    private function processRange(
        string $path,
        int    $start,
        int    $end,
        array  $slugBase,
        array  $dateIdByStr,
        array  $incr,
        string &$cells
    ): void
    {
        $fh = fopen($path, 'rb');
        stream_set_read_buffer($fh, 0);
        fseek($fh, $start);

        $remaining = $end - $start;
        $buf = '';

        while ($remaining > 0) {
            $toRead = min(self::READ_BUF, $remaining);
            $chunk = fread($fh, $toRead);
            if ($chunk === false || $chunk === '') {
                break;
            }
            $len = strlen($chunk);
            $remaining -= $len;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) {
                $buf .= $chunk;
                continue;
            }

            $tail = $len - $lastNl - 1;
            if ($tail > 0) {
                fseek($fh, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $chunk = sprintf('%s%s', $buf, substr($chunk, 0, $lastNl + 1));
            $buf = '';

            $p = 0;
            $chunkLen = strlen($chunk);

            while ($p < $chunkLen) {
                $nl = strpos($chunk, "\n", $p);
                if ($nl === false) {
                    $buf = substr($chunk, $p);
                    break;
                }
                $line = substr($chunk, $p, $nl - $p);
                $sep = strpos($line, ',');
                if ($sep !== false && strlen($line) >= self::URL_PREFIX + 1 + 2 + self::DATE_LEN) {
                    $slug = substr($line, self::URL_PREFIX, $sep - self::URL_PREFIX);
                    $dateStr = substr($line, $sep + 1 + 2, self::DATE_LEN);
                    if (isset($slugBase[$slug], $dateIdByStr[$dateStr])) {
                        $idx = $slugBase[$slug] + $dateIdByStr[$dateStr];
                        $cells[$idx] = $incr[$cells[$idx]];
                    }
                }
                $p = $nl + 1;
            }
        }

        fclose($fh);
    }

    /**
     * @param string $path
     * @param array  $counts
     * @param array  $idToSlug
     * @param array  $dateStrById
     * @param int    $numSlugs
     * @param int    $numDates
     * @return void
     */
    private function writeResult(
        string $path,
        array  $counts,
        array  $idToSlug,
        array  $dateStrById,
        int    $numSlugs,
        int    $numDates
    ): void
    {
        $fh = fopen($path, 'wb');
        stream_set_write_buffer($fh, self::WRITE_BUF);

        $datePrefix = [];
        for ($d = 0; $d < $numDates; $d++) {
            $datePrefix[$d] = sprintf('        "20%s": ', $dateStrById[$d]);
        }

        fwrite($fh, "{\n");
        $first = true;
        for ($p = 0; $p < $numSlugs; $p++) {
            $base = 1 + $p * $numDates;
            $entries = [];
            for ($d = 0; $d < $numDates; $d++) {
                $cnt = $counts[$base + $d] ?? 0;
                if ($cnt !== 0) {
                    $entries[] = sprintf('%s%d', $datePrefix[$d], $cnt);
                }
            }
            if ($entries === []) {
                continue;
            }
            $slug = $idToSlug[$p];
            $path = ($slug !== '' && $slug[0] === '/') ? $slug : sprintf('/%s', $slug);
            $key = sprintf('"%s"', addcslashes($path, '"\\/'));
            fwrite($fh, sprintf(
                "%s    %s: {\n%s\n    }",
                $first ? '' : ",\n",
                $key,
                implode(",\n", $entries)
            ));
            $first = false;
        }
        fwrite($fh, "\n}");
        fclose($fh);
    }
}
