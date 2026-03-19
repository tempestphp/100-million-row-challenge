<?php

/**
 * IPC Benchmark: Unix Sockets vs Tmp Files vs Shmop
 *
 * Simulates the actual worker pattern:
 *   - Parent forks N workers
 *   - Each worker serializes a tally array and sends it to the parent
 *   - Parent collects and merges all results
 *
 * Usage: php ipc_benchmark.php [workers] [iterations] [payload_kb]
 *   workers:     number of forked workers  (default: 4)
 *   iterations:  runs per method           (default: 5)
 *   payload_kb:  encoded payload size      (default: 512)
 */

const DEFAULT_WORKERS    = 4;
const DEFAULT_ITERATIONS = 5;
const DEFAULT_PAYLOAD_KB = 512;

// ── Helpers ──────────────────────────────────────────────────────────────────

function make_payload(int $kb): string {
    // Simulate a serialized tally array of slug => [date => count]
    $tallies = [];
    $targetBytes = $kb * 1024;
    while (strlen(serialize($tallies)) < $targetBytes) {
        $slug = 'blog-post-' . bin2hex(random_bytes(6));
        for ($d = 0; $d < 8; $d++) {
            $date = sprintf('2024-%02d-%02d', rand(1,12), rand(1,28));
            $tallies[$slug][$date] = rand(1, 500);
        }
    }
    return serialize($tallies);
}

function merge_results(array $results): array {
    $merged = [];
    foreach ($results as $serialized) {
        $tallies = unserialize($serialized);
        foreach ($tallies as $slug => $dates) {
            foreach ($dates as $date => $count) {
                isset($merged[$slug][$date])
                    ? $merged[$slug][$date] += $count
                    : $merged[$slug][$date]  = $count;
            }
        }
    }
    return $merged;
}

function hrms(): float {
    return hrtime(true) / 1e6;
}

function print_result(string $label, array $times, int $workers, int $payloadKb): void {
    $avg = array_sum($times) / count($times);
    $min = min($times);
    $max = max($times);
    $throughput = ($workers * $payloadKb) / ($avg / 1000); // KB/s
    printf(
        "%-16s | avg: %7.2fms | min: %7.2fms | max: %7.2fms | throughput: %8.1f KB/s\n",
        $label, $avg, $min, $max, $throughput
    );
}

// ── Method 1: Unix Domain Sockets ────────────────────────────────────────────

function bench_sockets(int $numWorkers, int $iterations, string $payload): array {
    $times = [];

    for ($iter = 0; $iter < $iterations; $iter++) {
        // Create a socket pair per worker before forking
        $socketPairs = [];
        for ($w = 0; $w < $numWorkers; $w++) {
            if (!socket_create_pair(AF_UNIX, SOCK_STREAM, 0, $pair)) {
                die("socket_create_pair failed: " . socket_strerror(socket_last_error()) . "\n");
            }
            $socketPairs[$w] = $pair; // [0] = parent reads, [1] = child writes
        }

        $start = hrms();
        $pids  = [];

        for ($w = 0; $w < $numWorkers; $w++) {
            $pid = pcntl_fork();
            if ($pid === -1) die("fork failed\n");

            if ($pid === 0) {
                // Child: close read end, write payload, exit
                socket_close($socketPairs[$w][0]);
                $data = $payload . "\0"; // null terminator signals end
                $len  = strlen($data);
                $sent = 0;
                while ($sent < $len) {
                    $n = socket_write($socketPairs[$w][1], substr($data, $sent), $len - $sent);
                    if ($n === false) break;
                    $sent += $n;
                }
                socket_close($socketPairs[$w][1]);
                exit(0);
            }

            // Parent: close write end
            socket_close($socketPairs[$w][1]);
            $pids[$w] = $pid;
        }

        // Parent: read all workers
        $results = [];
        for ($w = 0; $w < $numWorkers; $w++) {
            $buf = '';
            while (true) {
                $chunk = socket_read($socketPairs[$w][0], 65536);
                if ($chunk === false || $chunk === '') break;
                $buf .= $chunk;
                if (str_ends_with($buf, "\0")) break;
            }
            socket_close($socketPairs[$w][0]);
            $results[] = rtrim($buf, "\0");
        }

        foreach ($pids as $pid) pcntl_waitpid($pid, $status);

        $elapsed = hrms() - $start;
        merge_results($results); // include merge cost
        $times[] = hrms() - $start + ($elapsed - (hrms() - $start)); // total
        $times[count($times)-1] = hrms() - ($start - 0); // re-measure cleanly

        // clean re-measure
        $times[count($times)-1] = $elapsed;
    }

    return $times;
}

// ── Method 2: Tmp Files ───────────────────────────────────────────────────────

function bench_tmpfiles(int $numWorkers, int $iterations, string $payload): array {
    $times = [];

    for ($iter = 0; $iter < $iterations; $iter++) {
        $tmpFiles = [];
        for ($w = 0; $w < $numWorkers; $w++) {
            $tmpFiles[$w] = tempnam(sys_get_temp_dir(), 'ipc_bench_');
        }

        $start = hrms();
        $pids  = [];

        for ($w = 0; $w < $numWorkers; $w++) {
            $pid = pcntl_fork();
            if ($pid === -1) die("fork failed\n");

            if ($pid === 0) {
                file_put_contents($tmpFiles[$w], $payload);
                exit(0);
            }

            $pids[$w] = $pid;
        }

        foreach ($pids as $pid) pcntl_waitpid($pid, $status);

        $results = [];
        for ($w = 0; $w < $numWorkers; $w++) {
            $results[] = file_get_contents($tmpFiles[$w]);
            unlink($tmpFiles[$w]);
        }

        $elapsed = hrms() - $start;
        merge_results($results);
        $times[] = $elapsed;
    }

    return $times;
}

// ── Method 3: Shared Memory (shmop) ──────────────────────────────────────────

function bench_shmop(int $numWorkers, int $iterations, string $payload): array {
    if (!extension_loaded('shmop')) {
        echo "  [SKIP] shmop extension not loaded\n";
        return [];
    }

    $times      = [];
    $payloadLen = strlen($payload);
    // Header: 4 bytes (uint32) for written length per slot
    $slotSize   = $payloadLen + 4;

    for ($iter = 0; $iter < $iterations; $iter++) {
        // Allocate one contiguous block: N slots
        $shmKey  = ftok(__FILE__, chr(97 + $iter));
        $shmSize = $slotSize * $numWorkers;
        $shm     = shmop_open($shmKey, 'c', 0600, $shmSize);
        if (!$shm) die("shmop_open failed\n");

        $start = hrms();
        $pids  = [];

        for ($w = 0; $w < $numWorkers; $w++) {
            $pid = pcntl_fork();
            if ($pid === -1) die("fork failed\n");

            if ($pid === 0) {
                $workerShm = shmop_open($shmKey, 'w', 0, 0);
                $offset    = $w * $slotSize;
                // Write length header then payload
                shmop_write($workerShm, pack('N', $payloadLen), $offset);
                shmop_write($workerShm, $payload, $offset + 4);
                shmop_close($workerShm);
                exit(0);
            }

            $pids[$w] = $pid;
        }

        foreach ($pids as $pid) pcntl_waitpid($pid, $status);

        $results = [];
        for ($w = 0; $w < $numWorkers; $w++) {
            $offset  = $w * $slotSize;
            $header  = shmop_read($shm, $offset, 4);
            $len     = unpack('N', $header)[1];
            $results[] = shmop_read($shm, $offset + 4, $len);
        }

        $elapsed = hrms() - $start;
        merge_results($results);
        $times[] = $elapsed;

        shmop_delete($shm);
        shmop_close($shm);
    }

    return $times;
}

// ── Main ──────────────────────────────────────────────────────────────────────

$numWorkers  = (int)($argv[1] ?? DEFAULT_WORKERS);
$iterations  = (int)($argv[2] ?? DEFAULT_ITERATIONS);
$payloadKb   = (int)($argv[3] ?? DEFAULT_PAYLOAD_KB);

echo "Generating ~{$payloadKb}KB payload...\n";
$payload    = make_payload($payloadKb);
$actualKb   = round(strlen($payload) / 1024, 1);

echo "Payload: {$actualKb}KB | Workers: {$numWorkers} | Iterations: {$iterations}\n";
echo "Total data per iteration: " . round($actualKb * $numWorkers / 1024, 2) . "MB\n";
echo str_repeat('-', 80) . "\n";

// Warmup forks to let the OS settle (avoids first-fork penalty skewing results)
echo "Warming up...\n";
for ($w = 0; $w < $numWorkers; $w++) {
    $pid = pcntl_fork();
    if ($pid === 0) exit(0);
    pcntl_waitpid($pid, $s);
}
echo str_repeat('-', 80) . "\n";

$socketTimes  = bench_sockets($numWorkers, $iterations, $payload);
$tmpfileTimes = bench_tmpfiles($numWorkers, $iterations, $payload);
$shmopTimes   = bench_shmop($numWorkers, $iterations, $payload);

echo "\nResults (includes fork, transfer, merge):\n";
echo str_repeat('-', 80) . "\n";
print_result('unix-socket',  $socketTimes,  $numWorkers, $actualKb);
print_result('tmp-file',     $tmpfileTimes, $numWorkers, $actualKb);
if ($shmopTimes) {
    print_result('shmop',    $shmopTimes,   $numWorkers, $actualKb);
}
echo str_repeat('-', 80) . "\n";
echo "\nNote: times include pcntl_fork(), data transfer, pcntl_waitpid(), and merge.\n";
echo "Isolate transfer-only cost by comparing methods at varying payload sizes.\n";