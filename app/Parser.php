<?php

namespace App;

use App\Commands\Visit;

use function array_fill;
use function count;
use function fgets;
use function file_put_contents;
use function filesize;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function igbinary_serialize;
use function igbinary_unserialize;
use function json_encode;
use function pcntl_fork;
use function pcntl_wait;
use function stream_set_read_buffer;
use function strlen;
use function strpos;
use function substr;

use const JSON_PRETTY_PRINT;

final class Parser {
    private const int TARGET_CHUNK_BYTES = 32 * 1024 * 1024;
    private const int TARGET_ORDER_SEEN_BYTES = 8 * 1024 * 1024;
    private const int DATE_SLOTS = 2048;
    private const int DATE_SLOT_SHIFT = 11;
    private const string TMP_FILE = '/tmp/Fragonite.tmp';
    private const A_TO_SHORT_PATH = 26;
    private const CARET_TO_SHORT_PATH = 25;
    private const CARET_TO_LONG_PATH = 19;
    private const B_TO_Y = 22;
    private const B_TO_A_EXCLUDING_SHORT_PATH = 52;
    private const A_TO_B_MIN = 55;
    private array $date2id = [];
    private array $id2date = [];
    private array $path2id = [];
    private array $id2path = [];
    private array $orderseen = [];

    public function parse(string $inputPath, string $outputPath): void {
        gc_disable();

        $this->generate_date_maps();
        $this->generate_path_maps();

        $pid = pcntl_fork();
        if ($pid === 0) {
            $this->run_child($inputPath);
        } else {
            $this->run_parent($inputPath, $outputPath);
        }
    }

    private function generate_date_maps(): void {
        $today = new \DateTimeImmutable('tomorrow', new \DateTimeZone('UTC'));
        $date = $today->sub(new \DateInterval('P' . (self::DATE_SLOTS - 1) . 'D'));

        $date2id = [];
        $id2date = [];
        for ($id = 0; $id < self::DATE_SLOTS; $id++) {
            $fulldate = $date->format('Y-m-d');
            $date2id[substr($fulldate, 3, 7)] = $id;
            $id2date[$id] = $fulldate;
            $date = $date->add(new \DateInterval('P1D'));
        }

        $this->date2id = $date2id;
        $this->id2date = $id2date;
    }

    private function generate_path_maps(): void {
        $id2path = [];
        $path2id = [];
        foreach (Visit::all() as $visit) {
            $id2path[] = substr($visit->uri, self::CARET_TO_LONG_PATH);
            $path2id[] = substr($visit->uri, self::CARET_TO_SHORT_PATH);
        }
        $this->id2path = $id2path;
        $this->path2id = array_flip($path2id);
    }

    private function run_child(string $in): void {
        $handle = fopen($in, 'rb');
        stream_set_read_buffer($handle, 0);
        $filesize = filesize($in);
        fseek($handle, intdiv($filesize, 2));
        fgets($handle);
        $start = max(ftell($handle), self::TARGET_ORDER_SEEN_BYTES);
        $end = $filesize;
        $counts = array_fill(0, count($this->id2path) * self::DATE_SLOTS, 0);
        $this->hotloop($handle, $start, $end, $counts);

        $payload = igbinary_serialize($counts);
        file_put_contents(self::TMP_FILE, $payload);
    }

    private function run_parent(string $in, string $out): void {
        $handle = fopen($in, 'rb');
        stream_set_read_buffer($handle, 0);
        $filesize = filesize($in);
        fseek($handle, intdiv($filesize, 2));
        fgets($handle);
        $start = 0;
        $end = ftell($handle);
        $counts = array_fill(0, count($this->id2path) * self::DATE_SLOTS, 0);
        fseek($handle, self::TARGET_ORDER_SEEN_BYTES);
        fgets($handle);
        $firstchunk = ftell($handle);
        $this->hotloop_first_run($handle, $start, $firstchunk, $counts);
        $this->hotloop($handle, $firstchunk, $end, $counts);

        $json = [];
        foreach ($this->orderseen as $pathid) {
            $json[$this->id2path[$pathid]] = [];
        }

        pcntl_wait($status);
        $counts2 = igbinary_unserialize(file_get_contents(self::TMP_FILE));
        for ($i = 0; $i < count($this->id2path) * self::DATE_SLOTS; $i++) {
            $count = $counts[$i] + $counts2[$i];
            if ($count > 0) {
                $pathid = $i >> self::DATE_SLOT_SHIFT;
                $dateid = $i & ((1 << self::DATE_SLOT_SHIFT) - 1);
                $json[$this->id2path[$pathid]][$this->id2date[$dateid]] = $count;
            }
        }

        file_put_contents($out, json_encode($json, JSON_PRETTY_PRINT));
    }

    private function hotloop($handle, int $cur, int $end, array &$counts): void {
        $remaining = min(self::TARGET_CHUNK_BYTES, $end - $cur);
        if ($remaining <= 0) {
            return;
        }

        fseek($handle, $cur);
        $page = fread($handle, $remaining);
        $pagelen = strlen($page);

        $path2id = $this->path2id;
        $date2id = $this->date2id;

        while ($cur < $end) {
            $a = -1;
            // if ($a + self::A_TO_B_MIN >= $pagelen) {
            //     break;
            // }
            $b = strpos($page, "\n", self::A_TO_B_MIN);

            while ($b !== false) {
                $pathid = $path2id[substr($page, $a + self::A_TO_SHORT_PATH, $b - $a - self::B_TO_A_EXCLUDING_SHORT_PATH)];
                $dateid = $date2id[substr($page, $b - self::B_TO_Y, 7)];
                $counts[($pathid << self::DATE_SLOT_SHIFT) + $dateid]++;

                $a = $b;
                if ($a + self::A_TO_B_MIN >= $pagelen) {
                    break;
                }
                $b = strpos($page, "\n", $a + self::A_TO_B_MIN);
            }

            $marker = $a + 1;
            fseek($handle, $cur + $marker);
            $cur += $marker;

            $remaining = min(self::TARGET_CHUNK_BYTES, $end - $cur);
            if ($remaining <= 0) {
                break;
            }

            $page = fread($handle, $remaining);
            $pagelen = strlen($page);
        }
    }

    private function hotloop_first_run($handle, int $cur, int $end, array &$counts): void {
        fseek($handle, $cur);
        $page = fread($handle, min(self::TARGET_CHUNK_BYTES, $end - $cur));
        $pagelen = strlen($page);

        $path2id = $this->path2id;
        $date2id = $this->date2id;
        $orderseen = [];
        $seen = [];

        while ($cur < $end) {
            $a = -1;
            if ($a + self::A_TO_B_MIN >= $pagelen) {
                break;
            }
            $b = strpos($page, "\n", self::A_TO_B_MIN);

            while ($b !== false) {
                $pathid = $path2id[substr($page, $a + self::A_TO_SHORT_PATH, $b - $a - self::B_TO_A_EXCLUDING_SHORT_PATH)];
                if (!isset($seen[$pathid])) {
                    $seen[$pathid] = true;
                    $orderseen[] = $pathid;
                }

                $dateid = $date2id[substr($page, $b - self::B_TO_Y, 7)];
                $counts[($pathid << self::DATE_SLOT_SHIFT) + $dateid]++;

                $a = $b;
                if ($a + self::A_TO_B_MIN >= $pagelen) {
                    break;
                }
                $b = strpos($page, "\n", $a + self::A_TO_B_MIN);
            }

            $marker = $a + 1;
            fseek($handle, $cur + $marker);
            $cur += $marker;

            $remaining = min(self::TARGET_CHUNK_BYTES, $end - $cur);
            if ($remaining <= 0) {
                break;
            }

            $page = fread($handle, $remaining);
            $pagelen = strlen($page);
        }

        $this->orderseen = $orderseen;
    }
}
