<?php

namespace App;

use App\Commands\Visit;

use function strpos;
use function strrpos;
use function substr;
use function strlen;
use function fread;
use function fseek;
use function fwrite;
use function fopen;
use function fclose;
use function fgets;
use function ftell;
use function pack;
use function unpack;
use function array_fill;
use function implode;
use function str_replace;
use function count;
use function gc_disable;
use function getmypid;
use function pcntl_fork;
use function pcntl_wait;
use function pcntl_signal;
use function pcntl_async_signals;
use function stream_get_contents;
use function stream_select;
use function stream_set_blocking;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function stream_socket_pair;

use const SEEK_CUR;
use const SIG_DFL;
use const SIGTERM;
use const SIGINT;
use const WNOHANG;
use const STREAM_PF_UNIX;
use const STREAM_SOCK_STREAM;
use const STREAM_IPPROTO_IP;

final class Parser
{
    private const int CHUNK_SIZE    = 2_097_152;
    private const int READ_BUFFER   = 1_048_576;
    private const int WORKER_COUNT  = 8;
    private const int SEGMENT_COUNT = 32;
    private const int URI_OFFSET    = 25;
    private const int FILE_SIZE     = 7_509_674_827;
    private const int LOOP_FENCE    = 1600; // 16 * (48 + 52)
    private const int MIN_SLUG_LEN  = 4;

    private array $childPids = [];
    private array $pipes     = [];

    public static function parse(string $source, string $destination): void
    {
        gc_disable();
        (new self())->execute($source, $destination);
    }

    private function execute(string $input, string $output): void
    {
        [$dateIds, $dateList] = $this->buildDateRegistry();
        $dateCount = count($dateList);

        $slugs     = $this->discoverSlugs($input);
        $slugCount = count($slugs);

        $slugMap = [];
        foreach ($slugs as $id => $slug) {
            $slugMap[$slug] = $id * $dateCount;
        }

        $boundaries = $this->calculateSplits($input);

        // Strided assignment: worker $i owns segments $i, $i+W, $i+2W ...
        // 32 segments / 8 workers = exactly 4 segments each.
        $workerSegments = [];
        for ($s = 0; $s < self::SEGMENT_COUNT; $s++) {
            $workerSegments[$s % self::WORKER_COUNT][] = $s;
        }

        // Create one pipe pair per child worker before any forking.
        // [0] = child write end, [1] = parent read end.
        for ($i = 0; $i < self::WORKER_COUNT - 1; $i++) {
            $this->pipes[$i] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            if ($this->pipes[$i] === false) {
                throw new \RuntimeException("stream_socket_pair() failed for worker $i");
            }
        }

        $this->registerShutdownHandlers();

        $pids = [];
        for ($i = 0; $i < self::WORKER_COUNT - 1; $i++) {
            $pid = pcntl_fork();

            if ($pid === -1) {
                throw new \RuntimeException("pcntl_fork() failed for worker $i");
            }

            if ($pid === 0) {
                pcntl_signal(SIGTERM, SIG_DFL);
                pcntl_signal(SIGINT,  SIG_DFL);

                // Child only needs its own write end — close the parent's read end
                // of our pipe and let the OS clean up everything else on exit.
                fclose($this->pipes[$i][1]);

                $this->runWorker(
                    $input, $boundaries, $slugMap, $dateIds,
                    $dateCount, $slugCount,
                    $workerSegments[$i], $this->pipes[$i][0]
                );
                exit(0);
            }

            // Parent closes the child's write end after forking.
            fclose($this->pipes[$i][0]);

            $pids[$pid]          = $i;
            $this->childPids[$i] = $pid;
        }

        // Parent processes its own segments while children work in parallel.
        $aggregated     = array_fill(0, $slugCount * $dateCount, 0);
        $parentSegments = $workerSegments[self::WORKER_COUNT - 1];

        $fh = fopen($input, 'rb');
        stream_set_read_buffer($fh, 0);
        foreach ($parentSegments as $s) {
            $this->parseRange($fh, $boundaries[$s], $boundaries[$s + 1], $slugMap, $dateIds, $aggregated);
        }
        fclose($fh);

        // Drain pipes concurrently to avoid deadlock: children may produce more
        // data than the pipe buffer can hold (~64KB on Linux). We must read from
        // the pipes while also waiting for children to exit.
        $workerBuffers = array_fill(0, self::WORKER_COUNT - 1, '');

        // Set all parent read ends to non-blocking so stream_select can drain them.
        $readPipes = [];
        for ($i = 0; $i < self::WORKER_COUNT - 1; $i++) {
            stream_set_blocking($this->pipes[$i][1], false);
            $readPipes[$i] = $this->pipes[$i][1];
        }

        while ($pids || $readPipes) {
            // Drain whatever is available right now across all open pipes.
            if ($readPipes) {
                $read   = array_values($readPipes);
                $write  = null;
                $except = null;
                if (stream_select($read, $write, $except, 0, 50_000)) {
                    foreach ($read as $pipe) {
                        $idx   = array_search($pipe, $readPipes);
                        $chunk = fread($pipe, 65_536);
                        if ($chunk !== false && $chunk !== '') {
                            $workerBuffers[$idx] .= $chunk;
                        }
                        if (feof($pipe)) {
                            fclose($pipe);
                            unset($readPipes[$idx]);
                        }
                    }
                }
            }

            // Non-blocking wait: reap any child that has already exited.
            if ($pids) {
                $pid = pcntl_wait($status, WNOHANG);
                if ($pid > 0) {
                    $workerIdx = $pids[$pid];
                    unset($pids[$pid], $this->childPids[$workerIdx]);
                }
            }
        }

        // Aggregate all child results into $aggregated.
        $total = $slugCount * $dateCount;
        for ($w = 0; $w < self::WORKER_COUNT - 1; $w++) {
            $workerCounts = array_values(unpack('v*', $workerBuffers[$w]));
            for ($j = 0; $j < $total; $j++) {
                $aggregated[$j] += $workerCounts[$j];
            }
        }

        $this->generateJson($output, $aggregated, $slugs, $dateList);
    }

    private function registerShutdownHandlers(): void
    {
        pcntl_async_signals(true);

        $handler = function (int $sig): void {
            foreach ($this->childPids as $pid) {
                @posix_kill($pid, SIGTERM);
            }
            foreach ($this->childPids as $pid) {
                @pcntl_waitpid($pid, $status);
            }
            foreach ($this->pipes as $pair) {
                foreach ($pair as $fd) {
                    @fclose($fd);
                }
            }
            exit(1);
        };

        pcntl_signal(SIGTERM, $handler);
        pcntl_signal(SIGINT,  $handler);
    }

    private function runWorker(
        string $path, array $splits, array $slugMap, array $dateIds,
        int $dateCount, int $slugCount,
        array $segments, $pipe
    ): void {
        $counts = array_fill(0, $slugCount * $dateCount, 0);

        $fh = fopen($path, 'rb');
        stream_set_read_buffer($fh, 0);
        foreach ($segments as $s) {
            $this->parseRange($fh, $splits[$s], $splits[$s + 1], $slugMap, $dateIds, $counts);
        }
        fclose($fh);

        fwrite($pipe, pack('v*', ...$counts));
        fclose($pipe);
    }

    private function parseRange($fh, $start, $end, $slugMap, $dateIds, &$counts): void
    {
        fseek($fh, $start);
        $remaining = $end - $start;
        $bufSize   = self::READ_BUFFER;

        while ($remaining > 0) {
            $buffer = fread($fh, $remaining > $bufSize ? $bufSize : $remaining);
            if ($buffer === false || $buffer === '') break;

            $len       = strlen($buffer);
            $remaining -= $len;
            $lastNl    = strrpos($buffer, "\n");

            if ($lastNl === false) break;

            $overhang = $len - $lastNl - 1;
            if ($overhang > 0) {
                fseek($fh, -$overhang, SEEK_CUR);
                $remaining += $overhang;
            }

            $p     = self::URI_OFFSET;
            $fence = $lastNl - self::LOOP_FENCE;

            while ($p < $fence) {
                $comma = strpos($buffer, ',', $p + self::MIN_SLUG_LEN);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p + self::MIN_SLUG_LEN);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p + self::MIN_SLUG_LEN);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p + self::MIN_SLUG_LEN);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p + self::MIN_SLUG_LEN);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p + self::MIN_SLUG_LEN);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p + self::MIN_SLUG_LEN);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p + self::MIN_SLUG_LEN);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p + self::MIN_SLUG_LEN);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p + self::MIN_SLUG_LEN);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p + self::MIN_SLUG_LEN);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p + self::MIN_SLUG_LEN);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p + self::MIN_SLUG_LEN);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p + self::MIN_SLUG_LEN);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p + self::MIN_SLUG_LEN);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p + self::MIN_SLUG_LEN);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;
            }

            while ($p < $lastNl) {
                $comma = strpos($buffer, ',', $p + self::MIN_SLUG_LEN);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;
            }
        }
    }

    private function buildDateRegistry(): array
    {
        $map = []; $list = []; $id = 0;
        for ($y = 21; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => ($y % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                for ($d = 1; $d <= $maxD; $d++) {
                    $date        = $y . '-' . ($m < 10 ? '0' : '') . $m . '-' . ($d < 10 ? '0' : '') . $d;
                    $key         = substr($date, 1);
                    $map[$key]   = $id;
                    $list[$id++] = $date;
                }
            }
        }
        return [$map, $list];
    }

    private function discoverSlugs(string $path): array
    {
        $fh  = fopen($path, 'rb');
        $raw = fread($fh, self::CHUNK_SIZE);
        fclose($fh);

        $slugs = [];
        $pos   = 0;
        $limit = strrpos($raw, "\n") ?: 0;
        while ($pos < $limit) {
            $eol = strpos($raw, "\n", $pos + 52);
            if ($eol === false) break;
            $slugs[substr($raw, $pos + self::URI_OFFSET, $eol - $pos - 51)] = true;
            $pos = $eol + 1;
        }

        foreach (Visit::all() as $v) {
            $slugs[substr($v->uri, self::URI_OFFSET)] = true;
        }
        return array_keys($slugs);
    }

    private function calculateSplits(string $path): array
    {
        $pts     = [0];
        $fh      = fopen($path, 'rb');
        $segSize = (int) (self::FILE_SIZE / self::SEGMENT_COUNT);

        for ($i = 1; $i < self::SEGMENT_COUNT; $i++) {
            fseek($fh, $i * $segSize);
            fgets($fh);
            $pts[] = ftell($fh);
        }

        fclose($fh);
        $pts[] = self::FILE_SIZE;
        return $pts;
    }

    private function generateJson(string $out, array $counts, array $slugs, array $dates): void
    {
        $fp = fopen($out, 'wb');
        stream_set_write_buffer($fp, 4_194_304);
        fwrite($fp, '{');

        $dCount = count($dates);

        $datePrefixes = [];
        for ($d = 0; $d < $dCount; $d++) {
            $datePrefixes[$d] = "        \"20{$dates[$d]}\": ";
        }

        $escapedSlugs = [];
        foreach ($slugs as $idx => $slug) {
            $escapedSlugs[$idx] = "\"\\/blog\\/" . str_replace('/', '\\/', $slug) . "\"";
        }

        $isFirst = true;
        $base    = 0;
        foreach ($slugs as $sIdx => $_) {
            $entries = [];
            for ($d = 0; $d < $dCount; $d++) {
                if ($val = $counts[$base + $d]) {
                    $entries[] = $datePrefixes[$d] . $val;
                }
            }
            if ($entries) {
                $comma   = $isFirst ? "" : ",";
                $isFirst = false;
                fwrite($fp, "$comma\n    {$escapedSlugs[$sIdx]}: {\n" . implode(",\n", $entries) . "\n    }");
            }
            $base += $dCount;
        }

        fwrite($fp, "\n}");
        fclose($fp);
    }
}