<?php

namespace App;

use App\Commands\Visit;

use function chr;
use function fgets;
use function file_put_contents;
use function filesize;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function gc_disable;
use function intdiv;
use function json_encode;
use function microtime;
use function ord;
use function pcntl_fork;
use function pcntl_setcpuaffinity;
use function sem_acquire;
use function sem_get;
use function sem_release;
use function sem_remove;
use function shm_attach;
use function shm_detach;
use function shm_get_var;
use function shm_put_var;
use function shm_remove;
use function socket_create_pair;
use function socket_read;
use function socket_write;
use function sprintf;
use function str_repeat;
use function stream_set_read_buffer;
use function strlen;
use function strpos;
use function substr;
use function fwrite;

use const JSON_PRETTY_PRINT;

final class Parser {
    public const bool DEBUG_TIMING = false;
    public const int DATE_SLOTS = (365 * 5) + 15 + 16;
    // public const int TOTAL_SLOTS = 493120;
    public const int TOTAL_SLOTS = 497408;

    public const int WORKERS = 8;
    public const int CHUNKS = 1024 * 16;
    public const int PARENT_SEED_SIZE = 1024 * 256;
    public const A_TO_SHORT_PATH = 26;
    public const CARET_TO_SHORT_PATH = 25;
    public const CARET_TO_LONG_PATH = 19;
    public const B_TO_Y = 22;
    public const B_TO_A_EXCLUDING_SHORT_PATH = 52;
    public const A_TO_B_MIN = 55;
    public const string CHAR_LOOKUP = "\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x0B\x0C\x0D\x0E\x0F\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1A\x1B\x1C\x1D\x1E\x1F\x20\x21\x22\x23\x24\x25\x26\x27\x28\x29\x2A\x2B\x2C\x2D\x2E\x2F\x30\x31\x32\x33\x34\x35\x36\x37\x38\x39\x3A\x3B\x3C\x3D\x3E\x3F\x40\x41\x42\x43\x44\x45\x46\x47\x48\x49\x4A\x4B\x4C\x4D\x4E\x4F\x50\x51\x52\x53\x54\x55\x56\x57\x58\x59\x5A\x5B\x5C\x5D\x5E\x5F\x60\x61\x62\x63\x64\x65\x66\x67\x68\x69\x6A\x6B\x6C\x6D\x6E\x6F\x70\x71\x72\x73\x74\x75\x76\x77\x78\x79\x7A\x7B\x7C\x7D\x7E\x7F\x80\x81\x82\x83\x84\x85\x86\x87\x88\x89\x8A\x8B\x8C\x8D\x8E\x8F\x90\x91\x92\x93\x94\x95\x96\x97\x98\x99\x9A\x9B\x9C\x9D\x9E\x9F\xA0\xA1\xA2\xA3\xA4\xA5\xA6\xA7\xA8\xA9\xAA\xAB\xAC\xAD\xAE\xAF\xB0\xB1\xB2\xB3\xB4\xB5\xB6\xB7\xB8\xB9\xBA\xBB\xBC\xBD\xBE\xBF\xC0\xC1\xC2\xC3\xC4\xC5\xC6\xC7\xC8\xC9\xCA\xCB\xCC\xCD\xCE\xCF\xD0\xD1\xD2\xD3\xD4\xD5\xD6\xD7\xD8\xD9\xDA\xDB\xDC\xDD\xDE\xDF\xE0\xE1\xE2\xE3\xE4\xE5\xE6\xE7\xE8\xE9\xEA\xEB\xEC\xED\xEE\xEF\xF0\xF1\xF2\xF3\xF4\xF5\xF6\xF7\xF8\xF9\xFA\xFB\xFC\xFD\xFE\xFF\x00";
    public array $date2id = [];
    public array $id2date = [];
    public array $path2id = [];
    public array $id2path = [];
    public array $orderseen = [];
    public array $sockets = [];
    public float $scriptstart = 0.0;

    public function debug_timing(string $label): void {
        if (!self::DEBUG_TIMING) {
            return;
        }

        fwrite(STDERR, sprintf("[parser 8 +%.3fs] %s\n", microtime(true) - $this->scriptstart, $label));
    }

    public function parse(string $inputPath, string $outputPath): void {
        gc_disable();
        $this->scriptstart = microtime(true);

        $this->generate_date_maps();
        $this->generate_path_maps();

        if (self::PARENT_SEED_SIZE > intdiv(filesize($inputPath), self::CHUNKS)) {
            $this->run_m1_small($inputPath, $outputPath);
            return;
        }

        ChunkQueue::create(1);

        for ($i = 0; $i < 4; $i++) {
            socket_create_pair(AF_UNIX, SOCK_STREAM, 0, $pair);
            $this->sockets[] = $pair;
        }

        for ($i = 1; $i < self::WORKERS; $i++) {
            $pid = pcntl_fork();
            if ($pid === 0) {
                // pcntl_setcpuaffinity(null, [$i * 2]);
                pcntl_setcpuaffinity(null, [$i]);
                $this->run_m1_string_child($inputPath, $i);
                exit();
            }
        }
        pcntl_setcpuaffinity(null, [0]);

        $this->run_m1_string_parent($inputPath, $outputPath);
        $this->debug_timing('parse complete');
    }

    public function generate_date_maps(): void {
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

    public function generate_path_maps(): void {
        $id2path = [];
        $path2id = [];
        foreach (Visit::all() as $visit) {
            $id2path[] = substr($visit->uri, self::CARET_TO_LONG_PATH);
            $path2id[] = substr($visit->uri, self::CARET_TO_SHORT_PATH);
        }
        $this->id2path = $id2path;
        $this->path2id = array_flip($path2id);
    }

    public function run_m1_string_child(string $in, int $index): void {
        $handle = fopen($in, 'rb');
        stream_set_read_buffer($handle, 0);
        $filesize = filesize($in);

        $counts = str_repeat("\0", self::TOTAL_SLOTS);

        $queue = ChunkQueue::connect(1);
        $chunk = $queue->claim();
        while ($chunk !== null) {
            if ($chunk === self::CHUNKS - 1) {
                $end = $filesize;
            } else {
                fseek($handle, intdiv($filesize, self::CHUNKS) * ($chunk + 1));
                fgets($handle);
                $end = ftell($handle);
            }
            fseek($handle, intdiv($filesize, self::CHUNKS) * $chunk);
            fgets($handle);
            $start = ftell($handle);
            $this->hotloop_string_faster($handle, $end - $start, $counts);// 
            $chunk = $queue->claim();
        }

        // $this->debug_timing("worker {$index} hotloop end");

        if ($index % 2 === 0) {
            // Even index children listen and merge before writing.

            $payload = $this->read_exact($this->sockets[intdiv($index, 2)][1], strlen($counts));

            for ($i = 0; $i < self::TOTAL_SLOTS; $i++) {
                $counts[$i] = chr(ord($counts[$i]) + ord($payload[$i]));
            }

            $this->write_all($this->sockets[intdiv($index, 2)][1], $counts);
            $this->debug_timing("worker {$index} wrote");

        } else {
            // Odd index children write directly.

            $this->write_all($this->sockets[intdiv($index, 2)][0], $counts);
            $this->debug_timing("worker {$index} wrote");
        }
    }

    public function run_m1_string_parent(string $in, string $out): void {
        $handle = fopen($in, 'rb');
        stream_set_read_buffer($handle, 0);
        $filesize = filesize($in);

        $payload01 = str_repeat("\0", self::TOTAL_SLOTS);

        $start = 0;
        fseek($handle, intdiv($filesize, self::CHUNKS));
        fgets($handle);
        $end = ftell($handle);
        fseek($handle, 0);
        $this->hotloop_string_first_run_faster($handle, $end - $start, $payload01);

        $queue = ChunkQueue::connect(1);
        while (true) {
            $chunk = $queue->claim();
            if ($chunk === null) {
                break;
            }
            fseek($handle, intdiv($filesize, self::CHUNKS) * ($chunk + 1));
            fgets($handle);
            $end = ftell($handle);
            fseek($handle, intdiv($filesize, self::CHUNKS) * $chunk);
            fgets($handle);
            $start = ftell($handle);
            $this->hotloop_string_faster($handle, $end - $start, $payload01);
        }

        $this->debug_timing('parent hotloop end');

        $json = [];
        foreach ($this->orderseen as $pathid) {
            $json[$this->id2path[$pathid]] = [];
        }

        $payload1 = $this->read_exact($this->sockets[0][1], strlen($payload01));
        for ($i = 0; $i < self::TOTAL_SLOTS; $i++) {
            $payload01[$i] = chr(ord($payload01[$i]) + ord($payload1[$i]));
        }

        $payload23 = $this->read_exact($this->sockets[1][0], strlen($payload01));
        $payload45 = $this->read_exact($this->sockets[2][0], strlen($payload01));
        $payload67 = $this->read_exact($this->sockets[3][0], strlen($payload01));
        $this->debug_timing('parent collected payloads');

        for ($i = 0; $i < self::TOTAL_SLOTS; $i++) {
            $sum = ord($payload01[$i]) + ord($payload23[$i]) + ord($payload45[$i]) + ord($payload67[$i]);
            if ($sum > 0) {
                $pathid = intdiv($i, self::DATE_SLOTS);
                $dateid = $i % self::DATE_SLOTS;
                $json[$this->id2path[$pathid]][$this->id2date[$dateid]] = $sum;
            }
        }

        $this->debug_timing('parent merged payloads');

        file_put_contents($out, json_encode($json, JSON_PRETTY_PRINT));

        $this->debug_timing('parent wrote output');


        $queue->close(true);
    }

    public function run_m1_small(string $in, string $out): void {
        $handle = fopen($in, 'rb');
        stream_set_read_buffer($handle, 0);
        $filesize = filesize($in);

        $payload0 = str_repeat("\0", self::TOTAL_SLOTS);

        fseek($handle, 0);
        $this->hotloop_string_first_run_faster($handle, $filesize, $payload0);

        $json = [];
        foreach ($this->orderseen as $pathid) {
            $json[$this->id2path[$pathid]] = [];
        }

        for ($i = 0; $i < self::TOTAL_SLOTS; $i++) {
            $sum = ord($payload0[$i]);
            if ($sum > 0) {
                $pathid = intdiv($i, self::DATE_SLOTS);
                $dateid = $i % self::DATE_SLOTS;
                $json[$this->id2path[$pathid]][$this->id2date[$dateid]] = $sum;
            }
        }

        file_put_contents($out, json_encode($json, JSON_PRETTY_PRINT));
    }

    public function hotloop_string_faster($handle, int $size, string &$counts): void {
        $page = fread($handle, $size);

        $path2id = &$this->path2id;
        $date2id = &$this->date2id;

        $a = -1;
        $b = strpos($page, "\n", self::A_TO_B_MIN);
        while ($b !== false) {
            $pathid = $path2id[substr($page, $a + self::A_TO_SHORT_PATH, $b - $a - self::B_TO_A_EXCLUDING_SHORT_PATH)];
            $dateid = $date2id[substr($page, $b - self::B_TO_Y, 7)];
            $i = $pathid * self::DATE_SLOTS + $dateid;
            // $counts[$i] = chr(ord($counts[$i]) + 1);
            // $counts[$i] = self::CHAR_LOOKUP[ord($counts[$i])];
            $counts[$i] = match ($counts[$i]) {
                "\x00" => "\x01",
                "\x01" => "\x02",
                "\x02" => "\x03",
                "\x03" => "\x04",
                "\x04" => "\x05",
                "\x05" => "\x06",
                "\x06" => "\x07",
                "\x07" => "\x08",
                "\x08" => "\x09",
                "\x09" => "\x0A",
                "\x0A" => "\x0B",
                "\x0B" => "\x0C",
                "\x0C" => "\x0D",
                "\x0D" => "\x0E",
                "\x0E" => "\x0F",
                "\x0F" => "\x10",
                "\x10" => "\x11",
                "\x11" => "\x12",
                "\x12" => "\x13",
                "\x13" => "\x14",
                "\x14" => "\x15",
                "\x15" => "\x16",
                "\x16" => "\x17",
                "\x17" => "\x18",
                "\x18" => "\x19",
                "\x19" => "\x1A",
                "\x1A" => "\x1B",
                "\x1B" => "\x1C",
                "\x1C" => "\x1D",
                "\x1D" => "\x1E",
                "\x1E" => "\x1F",
                "\x1F" => "\x20",
                "\x20" => "\x21",
                "\x21" => "\x22",
                "\x22" => "\x23",
                "\x23" => "\x24",
                "\x24" => "\x25",
                "\x25" => "\x26",
                "\x26" => "\x27",
                "\x27" => "\x28",
                "\x28" => "\x29",
                "\x29" => "\x2A",
                "\x2A" => "\x2B",
                "\x2B" => "\x2C",
                "\x2C" => "\x2D",
                "\x2D" => "\x2E",
                "\x2E" => "\x2F",
                "\x2F" => "\x30",
                "\x30" => "\x31",
                "\x31" => "\x32",
                "\x32" => "\x33",
                "\x33" => "\x34",
                "\x34" => "\x35",
                "\x35" => "\x36",
                "\x36" => "\x37",
                "\x37" => "\x38",
                "\x38" => "\x39",
                "\x39" => "\x3A",
                "\x3A" => "\x3B",
                "\x3B" => "\x3C",
                "\x3C" => "\x3D",
                "\x3D" => "\x3E",
                "\x3E" => "\x3F",
                "\x3F" => "\x40",
                "\x40" => "\x41",
                "\x41" => "\x42",
                "\x42" => "\x43",
                "\x43" => "\x44",
                "\x44" => "\x45",
                "\x45" => "\x46",
                "\x46" => "\x47",
                "\x47" => "\x48",
                "\x48" => "\x49",
                "\x49" => "\x4A",
                "\x4A" => "\x4B",
                "\x4B" => "\x4C",
                "\x4C" => "\x4D",
                "\x4D" => "\x4E",
                "\x4E" => "\x4F",
                "\x4F" => "\x50",
                "\x50" => "\x51",
                "\x51" => "\x52",
                "\x52" => "\x53",
                "\x53" => "\x54",
                "\x54" => "\x55",
                "\x55" => "\x56",
                "\x56" => "\x57",
                "\x57" => "\x58",
                "\x58" => "\x59",
                "\x59" => "\x5A",
                "\x5A" => "\x5B",
                "\x5B" => "\x5C",
                "\x5C" => "\x5D",
                "\x5D" => "\x5E",
                "\x5E" => "\x5F",
                "\x5F" => "\x60",
                "\x60" => "\x61",
                "\x61" => "\x62",
                "\x62" => "\x63",
                "\x63" => "\x64",
                "\x64" => "\x65",
                "\x65" => "\x66",
                "\x66" => "\x67",
                "\x67" => "\x68",
                "\x68" => "\x69",
                "\x69" => "\x6A",
                "\x6A" => "\x6B",
                "\x6B" => "\x6C",
                "\x6C" => "\x6D",
                "\x6D" => "\x6E",
                "\x6E" => "\x6F",
                "\x6F" => "\x70",
                "\x70" => "\x71",
                "\x71" => "\x72",
                "\x72" => "\x73",
                "\x73" => "\x74",
                "\x74" => "\x75",
                "\x75" => "\x76",
                "\x76" => "\x77",
                "\x77" => "\x78",
                "\x78" => "\x79",
                "\x79" => "\x7A",
                "\x7A" => "\x7B",
                "\x7B" => "\x7C",
                "\x7C" => "\x7D",
                "\x7D" => "\x7E",
                "\x7E" => "\x7F",
                "\x7F" => "\x80",
                "\x80" => "\x81",
                "\x81" => "\x82",
                "\x82" => "\x83",
                "\x83" => "\x84",
                "\x84" => "\x85",
                "\x85" => "\x86",
                "\x86" => "\x87",
                "\x87" => "\x88",
                "\x88" => "\x89",
                "\x89" => "\x8A",
                "\x8A" => "\x8B",
                "\x8B" => "\x8C",
                "\x8C" => "\x8D",
                "\x8D" => "\x8E",
                "\x8E" => "\x8F",
                "\x8F" => "\x90",
                "\x90" => "\x91",
                "\x91" => "\x92",
                "\x92" => "\x93",
                "\x93" => "\x94",
                "\x94" => "\x95",
                "\x95" => "\x96",
                "\x96" => "\x97",
                "\x97" => "\x98",
                "\x98" => "\x99",
                "\x99" => "\x9A",
                "\x9A" => "\x9B",
                "\x9B" => "\x9C",
                "\x9C" => "\x9D",
                "\x9D" => "\x9E",
                "\x9E" => "\x9F",
                "\x9F" => "\xA0",
                "\xA0" => "\xA1",
                "\xA1" => "\xA2",
                "\xA2" => "\xA3",
                "\xA3" => "\xA4",
                "\xA4" => "\xA5",
                "\xA5" => "\xA6",
                "\xA6" => "\xA7",
                "\xA7" => "\xA8",
                "\xA8" => "\xA9",
                "\xA9" => "\xAA",
                "\xAA" => "\xAB",
                "\xAB" => "\xAC",
                "\xAC" => "\xAD",
                "\xAD" => "\xAE",
                "\xAE" => "\xAF",
                "\xAF" => "\xB0",
                "\xB0" => "\xB1",
                "\xB1" => "\xB2",
                "\xB2" => "\xB3",
                "\xB3" => "\xB4",
                "\xB4" => "\xB5",
                "\xB5" => "\xB6",
                "\xB6" => "\xB7",
                "\xB7" => "\xB8",
                "\xB8" => "\xB9",
                "\xB9" => "\xBA",
                "\xBA" => "\xBB",
                "\xBB" => "\xBC",
                "\xBC" => "\xBD",
                "\xBD" => "\xBE",
                "\xBE" => "\xBF",
                "\xBF" => "\xC0",
                "\xC0" => "\xC1",
                "\xC1" => "\xC2",
                "\xC2" => "\xC3",
                "\xC3" => "\xC4",
                "\xC4" => "\xC5",
                "\xC5" => "\xC6",
                "\xC6" => "\xC7",
                "\xC7" => "\xC8",
                "\xC8" => "\xC9",
                "\xC9" => "\xCA",
                "\xCA" => "\xCB",
                "\xCB" => "\xCC",
                "\xCC" => "\xCD",
                "\xCD" => "\xCE",
                "\xCE" => "\xCF",
                "\xCF" => "\xD0",
                "\xD0" => "\xD1",
                "\xD1" => "\xD2",
                "\xD2" => "\xD3",
                "\xD3" => "\xD4",
                "\xD4" => "\xD5",
                "\xD5" => "\xD6",
                "\xD6" => "\xD7",
                "\xD7" => "\xD8",
                "\xD8" => "\xD9",
                "\xD9" => "\xDA",
                "\xDA" => "\xDB",
                "\xDB" => "\xDC",
                "\xDC" => "\xDD",
                "\xDD" => "\xDE",
                "\xDE" => "\xDF",
                "\xDF" => "\xE0",
                "\xE0" => "\xE1",
                "\xE1" => "\xE2",
                "\xE2" => "\xE3",
                "\xE3" => "\xE4",
                "\xE4" => "\xE5",
                "\xE5" => "\xE6",
                "\xE6" => "\xE7",
                "\xE7" => "\xE8",
                "\xE8" => "\xE9",
                "\xE9" => "\xEA",
                "\xEA" => "\xEB",
                "\xEB" => "\xEC",
                "\xEC" => "\xED",
                "\xED" => "\xEE",
                "\xEE" => "\xEF",
                "\xEF" => "\xF0",
                "\xF0" => "\xF1",
                "\xF1" => "\xF2",
                "\xF2" => "\xF3",
                "\xF3" => "\xF4",
                "\xF4" => "\xF5",
                "\xF5" => "\xF6",
                "\xF6" => "\xF7",
                "\xF7" => "\xF8",
                "\xF8" => "\xF9",
                "\xF9" => "\xFA",
                "\xFA" => "\xFB",
                "\xFB" => "\xFC",
                "\xFC" => "\xFD",
                "\xFD" => "\xFE",
                "\xFE" => "\xFF",
                "\xFF" => "\x00"
            };

            $a = $b;
            if ($a + self::A_TO_B_MIN >= $size) {
                return;
            }
            $b = strpos($page, "\n", $a + self::A_TO_B_MIN);
        }
    }

    public function hotloop_string_first_run_faster($handle, int $size, string &$counts): void {
        $page = fread($handle, $size);

        $path2id = $this->path2id;
        $date2id = $this->date2id;

        $orderseen = [];
        $seen = [];

        $a = -1;
        $b = strpos($page, "\n", self::A_TO_B_MIN);

        while ($b !== false) {
            $pathid = $path2id[substr($page, $a + self::A_TO_SHORT_PATH, $b - $a - self::B_TO_A_EXCLUDING_SHORT_PATH)];
            if (!isset($seen[$pathid])) {
                $seen[$pathid] = true;
                $orderseen[] = $pathid;
            }

            $dateid = $date2id[substr($page, $b - self::B_TO_Y, 7)];
            $i = $pathid * self::DATE_SLOTS + $dateid;
            $counts[$i] = self::CHAR_LOOKUP[ord($counts[$i])];

            $a = $b;
            if ($a + self::A_TO_B_MIN >= $size) {
                break;
            }
            $b = strpos($page, "\n", $a + self::A_TO_B_MIN);
        }

        $this->orderseen = $orderseen;
    }

    public function write_all($sock, string $buf): void {
        $off = 0;
        $len = strlen($buf);
        while ($off < $len) {
            $n = socket_write($sock, substr($buf, $off), $len - $off);
            if ($n === false || $n === 0) {
                throw new \RuntimeException('socket_write failed');
            }
            $off += $n;
        }
    }

    public function read_exact($sock, int $len): string {
        $buf = '';
        while (strlen($buf) < $len) {
            $chunk = socket_read($sock, $len - strlen($buf));
            if ($chunk === false || $chunk === '') {
                throw new \RuntimeException('socket_read failed/EOF');
            }
            $buf .= $chunk;
        }
        return $buf;
    }
}

final class ChunkQueue {
    public const int TOTAL_CHUNKS = 1024 * 16;
    public const int SHM_NEXT_VAR_KEY = 1;

    public int $ipckey;
    public mixed $shm;
    public mixed $sem;

    public function __construct(int $ipckey, mixed $shm, mixed $sem) {
        $this->ipckey = $ipckey;
        $this->shm = $shm;
        $this->sem = $sem;
    }

    public static function create(int $ipckey, int $shmBytes = 1024): self {
        $oldsem = @sem_get($ipckey, 1, 0666, true);
        if ($oldsem !== false) {
            @sem_remove($oldsem);
        }

        $oldshm = @shm_attach($ipckey, $shmBytes, 0666);
        if ($oldshm !== false) {
            @shm_remove($oldshm);
            @shm_detach($oldshm);
        }

        $sem = sem_get($ipckey, 1, 0666, true);
        if ($sem === false) {
            throw new \RuntimeException('Unable to create semaphore');
        }

        $shm = shm_attach($ipckey, $shmBytes, 0666);
        if ($shm === false) {
            throw new \RuntimeException('Unable to attach shared memory');
        }

        if (!sem_acquire($sem)) {
            throw new \RuntimeException('Unable to acquire semaphore');
        }

        shm_put_var($shm, self::SHM_NEXT_VAR_KEY, 1);

        sem_release($sem);

        return new self($ipckey, $shm, $sem);
    }

    public static function connect(int $ipckey, int $shmbytes = 1024): self {
        $sem = sem_get($ipckey, 1, 0666, true);
        if ($sem === false) {
            throw new \RuntimeException('Unable to connect semaphore');
        }

        $shm = shm_attach($ipckey, $shmbytes, 0666);
        if ($shm === false) {
            throw new \RuntimeException('Unable to connect shared memory');
        }

        return new self($ipckey, $shm, $sem);
    }

    public function claim(): ?int {
        if (!sem_acquire($this->sem)) {
            throw new \RuntimeException('Unable to acquire semaphore');
        }

        $next = shm_get_var($this->shm, self::SHM_NEXT_VAR_KEY);

        if ($next >= self::TOTAL_CHUNKS) {
            sem_release($this->sem);
            return null;
        }

        shm_put_var($this->shm, self::SHM_NEXT_VAR_KEY, $next + 1);
        sem_release($this->sem);

        return $next;
    }

    public function total(): int {
        return self::TOTAL_CHUNKS;
    }

    public function close(bool $destroy = false): void {
        if ($destroy) {
            @shm_remove($this->shm);
            @sem_remove($this->sem);
        }

        @shm_detach($this->shm);
    }

    public function ipc_key(): int {
        return $this->ipckey;
    }
}
