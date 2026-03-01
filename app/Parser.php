<?php

namespace App;

use App\Commands\Visit;

use function array_count_values;
use function array_fill;
use function chr;
use function count;
use function fclose;
use function fgets;
use function file_get_contents;
use function file_put_contents;
use function filesize;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function getmypid;
use function implode;
use function intdiv;
use function min;
use function pack;
use function pcntl_fork;
use function pcntl_wait;
use function sem_acquire;
use function sem_get;
use function sem_release;
use function sem_remove;
use function set_error_handler;
use function shmop_delete;
use function shmop_open;
use function shmop_read;
use function shmop_write;
use function str_replace;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function sys_get_temp_dir;
use function unlink;
use function unpack;

use const SEEK_CUR;

final class Parser
{
    private const int READ_BLOCK_BYTES = 65_536;
    private const int SLUG_SCAN_BYTES   = 2_097_152;
    private const int URL_PREFIX_BYTES  = 25;
    private const int PROCESS_COUNT     = 8;
    private const int QUEUE_CHUNKS      = 64;

    public function parse($inputPath, $outputPath)
    {
        $runStartNs = \hrtime(true);
        $profileEnabled = (\getenv('PARSER_PROFILE') === '1');
        $phaseStartNs = $runStartNs;
        $phaseMarks = [];
        $markPhase = static function (string $name) use (&$phaseMarks, &$phaseStartNs, $runStartNs, $profileEnabled): void {
            if (! $profileEnabled) {
                return;
            }

            $now = \hrtime(true);
            $phaseMarks[] = [
                'name' => $name,
                'delta_ms' => ($now - $phaseStartNs) / 1_000_000,
                'total_ms' => ($now - $runStartNs) / 1_000_000,
            ];
            $phaseStartNs = $now;
        };
        $dumpPhases = static function (string $planId, int $workerTotal, int $chunkTotal) use (&$phaseMarks, $profileEnabled): void {
            if (! $profileEnabled) {
                return;
            }

            \fwrite(STDERR, "[parser-profile] plan={$planId} workers={$workerTotal} chunks={$chunkTotal}\n");
            foreach ($phaseMarks as $mark) {
                \fwrite(
                    STDERR,
                    \sprintf(
                        "  %-24s delta=%8.3f ms total=%8.3f ms\n",
                        $mark['name'],
                        $mark['delta_ms'],
                        $mark['total_ms'],
                    ),
                );
            }
        };

        gc_disable();

        $inputBytes   = filesize($inputPath);
        $canUseSem = function_exists('sem_get')
            && function_exists('sem_acquire')
            && function_exists('sem_release')
            && function_exists('sem_remove');
        $tuningState = self::loadTuningState();
        [$tuningKey, $planId, $workerTotal, $chunkTotal] = self::pickRuntimePlan($tuningState, $inputBytes, $canUseSem);
        $markPhase('runtime-plan');

        $dayIdByKey   = [];
        $dayKeyById     = [];
        $dateCount = 0;

        for ($y = 21; $y <= 26; $y++) {
            $yStr = (string)$y;
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2           => $y === 24 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default     => 31,
                };
                $mStr  = ($m < 10 ? '0' : '') . $m;
                $ymStr = $yStr . '-' . $mStr . '-';
                for ($d = 1; $d <= $maxD; $d++) {
                    $key               = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $dayIdByKey[$key]     = $dateCount;
                    $dayKeyById[$dateCount] = $key;
                    $dateCount++;
                }
            }
        }

        $dayIdTokens = [];
        foreach ($dayIdByKey as $date => $id) {
            $dayIdTokens[$date] = chr($id & 0xFF) . chr($id >> 8);
        }
        $markPhase('date-maps');

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $raw = fread($handle, min(self::SLUG_SCAN_BYTES, $inputBytes));
        fclose($handle);

        $slugIdByKey   = [];
        $slugKeyById     = [];
        $slugTotal = 0;
        $pos       = 0;
        $lastNl    = strrpos($raw, "\n") ?: 0;

        while ($pos < $lastNl) {
            $nl = strpos($raw, "\n", $pos + 52);
            if ($nl === false) break;

            $slug = substr($raw, $pos + self::URL_PREFIX_BYTES, $nl - $pos - 51);

            if (!isset($slugIdByKey[$slug])) {
                $slugIdByKey[$slug]    = $slugTotal;
                $slugKeyById[$slugTotal] = $slug;
                $slugTotal++;
            }

            $pos = $nl + 1;
        }
        unset($raw);
        $markPhase('slug-scan');

        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, self::URL_PREFIX_BYTES);
            if (!isset($slugIdByKey[$slug])) {
                $slugIdByKey[$slug]    = $slugTotal;
                $slugKeyById[$slugTotal] = $slug;
                $slugTotal++;
            }
        }
        $markPhase('visit-merge');

        $scaledPlans = [];
        foreach (self::planCatalog() as $candidatePlanId => $plan) {
            $scaledPlans[$candidatePlanId] = self::scalePlanForInput($plan, $inputBytes);
        }

        $profileRuns = (int) ($tuningState['profiles'][$tuningKey]['runs'] ?? 0);
        $forcePilot = (\getenv('PARSER_FORCE_PILOT') === '1');
        $disableAutoTune = (\getenv('PARSER_DISABLE_AUTOTUNE') === '1');
        $shouldRunPilot = $forcePilot || (!$disableAutoTune && ($profileRuns < 6 || ($profileRuns % 25 === 0)));
        $pilotPlanId = null;

        if ($shouldRunPilot) {
            $pilotPlanId = self::choosePlanWithinCurrentRun(
                $inputPath,
                $inputBytes,
                $scaledPlans,
                $slugIdByKey,
                $dayIdTokens,
                $planId,
            );
        }

        if ($pilotPlanId !== null && isset($scaledPlans[$pilotPlanId])) {
            $planId = $pilotPlanId;
            $workerTotal = $scaledPlans[$planId]['workers'];
            $chunkTotal = $scaledPlans[$planId]['chunks'];
        }

        $forcedWorkers = (int) (\getenv('PARSER_FORCE_WORKERS') ?: 0);
        $forcedChunks = (int) (\getenv('PARSER_FORCE_CHUNKS') ?: 0);
        if ($forcedWorkers > 0) {
            $workerTotal = $forcedWorkers;
        }
        if ($forcedChunks > 0) {
            $chunkTotal = $forcedChunks;
        }
        if ($chunkTotal < $workerTotal) {
            $chunkTotal = $workerTotal;
        }
        $markPhase('in-run-pilot');

        $chunkOffsets = [0];
        $bh = fopen($inputPath, 'rb');
        for ($i = 1; $i < $chunkTotal; $i++) {
            fseek($bh, intdiv($inputBytes * $i, $chunkTotal));
            fgets($bh);
            $chunkOffsets[] = ftell($bh);
        }
        fclose($bh);
        $chunkOffsets[] = $inputBytes;
        $markPhase('chunk-offsets');

        $tmpDir    = sys_get_temp_dir();
        $myPid     = getmypid();
        $tmpPrefix = $tmpDir . '/p100m_' . $myPid;

        $shmSegSize = $slugTotal * $dateCount * 2;
        $shmHandles = [];
        $useShm     = false;

        $allOk = true;
        for ($w = 0; $w < $workerTotal - 1; $w++) {
            $shmKey = $myPid * 100 + $w;
            set_error_handler(null);
            $shm = @shmop_open($shmKey, 'c', 0644, $shmSegSize);
            set_error_handler(null);
            if ($shm === false) {
                foreach ($shmHandles as [$k, $s]) {
                    shmop_delete($s);
                }
                $shmHandles = [];
                $allOk      = false;
                break;
            }
            $shmHandles[$w] = [$shmKey, $shm];
        }
        $useShm = $allOk;

        $useSemQueue = false;
        $semKey      = $myPid + 1;
        $queueShmKey = $myPid + 2;
        $queueShm    = null;
        $sem         = null;
        $queueFile   = null;

        $skipSem = (PHP_OS_FAMILY === 'Darwin') || (\getenv('PARSER_FORCE_FLOCK') === '1');
        if ($canUseSem && !$skipSem) {
            set_error_handler(null);
            $sem      = @sem_get($semKey, 1, 0644, true);
            $queueShm = @shmop_open($queueShmKey, 'c', 0644, 4);
            set_error_handler(null);
        }

        if ($sem !== false && $sem !== null && $queueShm !== false && $queueShm !== null) {
            shmop_write($queueShm, pack('V', 0), 0);
            $useSemQueue = true;
        }

        $useStaticSchedule = !$useSemQueue && self::shouldUseStaticChunkSchedule($workerTotal, $chunkTotal);

        if (!$useSemQueue && !$useStaticSchedule) {
            $queueFile = $tmpPrefix . '_queue';
            file_put_contents($queueFile, pack('V', 0));
        }
        $markPhase('ipc-setup');

        $childMap = [];

        for ($w = 0; $w < $workerTotal - 1; $w++) {
            $tmpFile = $tmpPrefix . '_' . $w;
            $pid     = pcntl_fork();
            if ($pid === -1) throw new \RuntimeException('pcntl_fork failed');

            if ($pid === 0) {
                $buckets = array_fill(0, $slugTotal, '');
                $fh      = fopen($inputPath, 'rb');
                stream_set_read_buffer($fh, 0);

                if ($useStaticSchedule) {
                    self::consumeAssignedChunks($fh, $chunkOffsets, $chunkTotal, $w, $workerTotal, $slugIdByKey, $dayIdTokens, $buckets);
                } elseif ($useSemQueue) {
                    while (true) {
                        $ci = self::claimChunkFromSharedQueue($queueShm, $sem, $chunkTotal);
                        if ($ci === -1) break;
                        self::consumeRangeIntoBuckets($fh, $chunkOffsets[$ci], $chunkOffsets[$ci + 1], $slugIdByKey, $dayIdTokens, $buckets);
                    }
                } else {
                    $qf = fopen($queueFile, 'c+b');
                    while (true) {
                        $ci = self::claimChunkFromFileQueue($qf, $chunkTotal);
                        if ($ci === -1) break;
                        self::consumeRangeIntoBuckets($fh, $chunkOffsets[$ci], $chunkOffsets[$ci + 1], $slugIdByKey, $dayIdTokens, $buckets);
                    }
                    fclose($qf);
                }

                fclose($fh);

                $counts = self::reduceBucketsToCounts($buckets, $slugTotal, $dateCount);
                $packed = pack('v*', ...$counts);

                if ($useShm) {
                    shmop_write($shmHandles[$w][1], $packed, 0);
                } else {
                    file_put_contents($tmpFile, $packed);
                }

                exit(0);
            }

            $childMap[$pid] = $w;
        }

        $buckets = array_fill(0, $slugTotal, '');
        $fh      = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);

        if ($useStaticSchedule) {
            self::consumeAssignedChunks($fh, $chunkOffsets, $chunkTotal, $workerTotal - 1, $workerTotal, $slugIdByKey, $dayIdTokens, $buckets);
        } elseif ($useSemQueue) {
            while (true) {
                $ci = self::claimChunkFromSharedQueue($queueShm, $sem, $chunkTotal);
                if ($ci === -1) break;
                self::consumeRangeIntoBuckets($fh, $chunkOffsets[$ci], $chunkOffsets[$ci + 1], $slugIdByKey, $dayIdTokens, $buckets);
            }
        } else {
            $qf = fopen($queueFile, 'c+b');
            while (true) {
                $ci = self::claimChunkFromFileQueue($qf, $chunkTotal);
                if ($ci === -1) break;
                self::consumeRangeIntoBuckets($fh, $chunkOffsets[$ci], $chunkOffsets[$ci + 1], $slugIdByKey, $dayIdTokens, $buckets);
            }
            fclose($qf);
        }

        fclose($fh);

        $counts = self::reduceBucketsToCounts($buckets, $slugTotal, $dateCount);
        $n      = $slugTotal * $dateCount;

        while ($childMap) {
            $pid = pcntl_wait($status);
            if (!isset($childMap[$pid])) continue;

            $w = $childMap[$pid];
            unset($childMap[$pid]);

            if ($useShm) {
                $packed = shmop_read($shmHandles[$w][1], 0, $shmSegSize);
                shmop_delete($shmHandles[$w][1]);
            } else {
                $tmpFile = $tmpPrefix . '_' . $w;
                $packed  = file_get_contents($tmpFile);
                unlink($tmpFile);
            }

            $childCounts = unpack('v*', $packed);
            for ($j = 0, $k = 1; $j < $n; $j++, $k++) {
                $counts[$j] += $childCounts[$k];
            }
        }
        $markPhase('parse-and-reduce');

        if ($useSemQueue) {
            shmop_delete($queueShm);
            sem_remove($sem);
        } elseif ($queueFile !== null) {
            unlink($queueFile);
        }

        self::flushJsonOutput($outputPath, $counts, $slugKeyById, $dayKeyById, $dateCount);
        $markPhase('json-output');

        $elapsedMs = (\hrtime(true) - $runStartNs) / 1_000_000;
        self::rememberPlanRun($tuningState, $tuningKey, $planId, $elapsedMs);
        self::persistTuningState($tuningState);
        $markPhase('tuning-persist');
        $dumpPhases($planId, $workerTotal, $chunkTotal);
    }

    /**
     * @return array{0: string, 1: string, 2: int, 3: int}
     */
    private static function pickRuntimePlan(array &$state, int $inputBytes, bool $canUseSem): array
    {
        $profileKey = self::buildTuningKey($inputBytes, $canUseSem);
        $plans = self::planCatalog();

        $entry = $state['profiles'][$profileKey] ?? [
            'runs' => 0,
            'best_plan' => 'balanced',
            'best_ms' => PHP_FLOAT_MAX,
            'plans' => [],
        ];

        foreach ($plans as $planId => $_plan) {
            if (!isset($entry['plans'][$planId])) {
                $entry['plans'][$planId] = [
                    'runs' => 0,
                    'avg_ms' => 0.0,
                    'best_ms' => PHP_FLOAT_MAX,
                ];
            }
        }

        $selectedPlan = isset($plans[$entry['best_plan'] ?? ''])
            ? $entry['best_plan']
            : 'balanced';

        $runCount = (int) ($entry['runs'] ?? 0);
        $disableAutoTune = (\getenv('PARSER_DISABLE_AUTOTUNE') === '1');
        $explore = !$disableAutoTune && ($runCount < (\count($plans) * 2) || ($runCount % 25 === 0));

        if ($explore) {
            $leastRuns = PHP_INT_MAX;
            foreach ($plans as $planId => $_plan) {
                $planRuns = (int) ($entry['plans'][$planId]['runs'] ?? 0);
                if ($planRuns < $leastRuns) {
                    $leastRuns = $planRuns;
                    $selectedPlan = $planId;
                }
            }
        }

        $state['profiles'][$profileKey] = $entry;

        $picked = self::scalePlanForInput($plans[$selectedPlan], $inputBytes);

        return [$profileKey, $selectedPlan, $picked['workers'], $picked['chunks']];
    }

    /**
     * @param array{workers:int,chunks:int} $plan
     * @return array{workers:int,chunks:int}
     */
    private static function scalePlanForInput(array $plan, int $inputBytes): array
    {
        $workers = $plan['workers'];
        $chunks = $plan['chunks'];

        if ($inputBytes < 64 * 1024 * 1024) {
            return ['workers' => 1, 'chunks' => 1];
        }

        if ($inputBytes < 512 * 1024 * 1024) {
            $workers = \min($workers, 4);
            $chunks = \min($chunks, 8);
        } elseif ($inputBytes < 2 * 1024 * 1024 * 1024) {
            $workers = \min($workers, 6);
            $chunks = \min($chunks, 12);
        }

        if ($chunks < $workers) {
            $chunks = $workers;
        }

        return ['workers' => $workers, 'chunks' => $chunks];
    }

    /**
     * @return array<string, array{workers:int,chunks:int}>
     */
    private static function planCatalog(): array
    {
        return [
            'balanced' => ['workers' => self::PROCESS_COUNT, 'chunks' => self::QUEUE_CHUNKS],
            'throughput' => ['workers' => 10, 'chunks' => 24],
            'lean' => ['workers' => 6, 'chunks' => 12],
        ];
    }

    private static function buildTuningKey(int $inputBytes, bool $canUseSem): string
    {
        $sizeBucket = $inputBytes < 512 * 1024 * 1024
            ? 'small'
            : ($inputBytes < 2 * 1024 * 1024 * 1024 ? 'medium' : 'large');

        return 'v1'
            . '|php:' . PHP_MAJOR_VERSION . '.' . PHP_MINOR_VERSION
            . '|os:' . PHP_OS_FAMILY
            . '|sem:' . ($canUseSem ? '1' : '0')
            . '|shm:' . (function_exists('shmop_open') ? '1' : '0')
            . '|size:' . $sizeBucket;
    }

    private static function tuningStatePath(): string
    {
        return \dirname(__DIR__) . '/data/.parser-tuning.json';
    }

    private static function loadTuningState(): array
    {
        $path = self::tuningStatePath();
        if (!is_file($path)) {
            return ['version' => 1, 'profiles' => []];
        }

        $json = file_get_contents($path);

        if ($json === false || $json === '') {
            return ['version' => 1, 'profiles' => []];
        }

        $decoded = json_decode($json, true);
        if (!is_array($decoded) || !isset($decoded['profiles']) || !is_array($decoded['profiles'])) {
            return ['version' => 1, 'profiles' => []];
        }

        $decoded['version'] = 1;

        return $decoded;
    }

    private static function persistTuningState(array $state): void
    {
        $state['version'] = 1;
        $state['updated_at'] = time();

        $json = json_encode($state, JSON_UNESCAPED_SLASHES);
        if (!is_string($json)) {
            return;
        }

        @file_put_contents(self::tuningStatePath(), $json, LOCK_EX);
    }

    private static function rememberPlanRun(array &$state, string $profileKey, string $planId, float $elapsedMs): void
    {
        if (!isset($state['profiles'][$profileKey])) {
            return;
        }

        $entry = &$state['profiles'][$profileKey];
        if (!isset($entry['plans'][$planId])) {
            $entry['plans'][$planId] = [
                'runs' => 0,
                'avg_ms' => 0.0,
                'best_ms' => PHP_FLOAT_MAX,
            ];
        }

        $plan = &$entry['plans'][$planId];
        $runs = (int) ($plan['runs'] ?? 0);
        $avg = (float) ($plan['avg_ms'] ?? 0.0);

        $plan['runs'] = $runs + 1;
        $plan['avg_ms'] = $runs === 0
            ? $elapsedMs
            : (($avg * $runs) + $elapsedMs) / ($runs + 1);
        $plan['best_ms'] = \min((float) ($plan['best_ms'] ?? PHP_FLOAT_MAX), $elapsedMs);

        $entry['runs'] = (int) ($entry['runs'] ?? 0) + 1;

        $bestPlanId = $planId;
        $bestMs = (float) $plan['best_ms'];

        foreach ($entry['plans'] as $candidateId => $candidateStats) {
            $candidateRuns = (int) ($candidateStats['runs'] ?? 0);
            if ($candidateRuns === 0) {
                continue;
            }

            $candidateBest = (float) ($candidateStats['best_ms'] ?? PHP_FLOAT_MAX);
            if ($candidateBest < $bestMs) {
                $bestMs = $candidateBest;
                $bestPlanId = $candidateId;
            }
        }

        $entry['best_plan'] = $bestPlanId;
        $entry['best_ms'] = $bestMs;
    }

    /**
     * @param array<string, array{workers:int,chunks:int}> $plans
     * @param array<string,int> $slugIdByKey
     * @param array<string,string> $dayIdTokens
     */
    private static function choosePlanWithinCurrentRun(
        string $inputPath,
        int $inputBytes,
        array $plans,
        array $slugIdByKey,
        array $dayIdTokens,
        string $baselinePlanId,
    ): ?string {
        if ($inputBytes < 1024 * 1024 * 1024 || \count($plans) < 2 || !isset($plans[$baselinePlanId])) {
            return null;
        }

        $pilotRanges = self::buildPilotRanges($inputPath, $inputBytes);
        if ($pilotRanges === []) {
            return null;
        }

        $challengerPlanId = self::pickPilotChallenger($plans, $baselinePlanId);
        if ($challengerPlanId === null) {
            return null;
        }

        $baselineMs = self::benchmarkPilotPlan(
            $inputPath,
            $pilotRanges,
            $plans[$baselinePlanId]['workers'],
            $slugIdByKey,
            $dayIdTokens,
        );

        $challengerMs = self::benchmarkPilotPlan(
            $inputPath,
            $pilotRanges,
            $plans[$challengerPlanId]['workers'],
            $slugIdByKey,
            $dayIdTokens,
        );

        if ($challengerMs >= $baselineMs * 0.98) {
            return null;
        }

        return $challengerPlanId;
    }

    /**
     * @param array<string, array{workers:int,chunks:int}> $plans
     */
    private static function pickPilotChallenger(array $plans, string $baselinePlanId): ?string
    {
        $preferredOrder = ['throughput', 'balanced', 'lean'];

        foreach ($preferredOrder as $candidatePlanId) {
            if ($candidatePlanId !== $baselinePlanId && isset($plans[$candidatePlanId])) {
                return $candidatePlanId;
            }
        }

        foreach ($plans as $candidatePlanId => $_plan) {
            if ($candidatePlanId !== $baselinePlanId) {
                return $candidatePlanId;
            }
        }

        return null;
    }

    /**
     * @return list<array{start:int,end:int}>
     */
    private static function buildPilotRanges(string $inputPath, int $inputBytes): array
    {
        $targetBytes = \min(8 * 1024 * 1024, (int) ($inputBytes / 20));
        if ($targetBytes < 2 * 1024 * 1024) {
            return [];
        }

        $segments = 3;
        $segmentBytes = \max(512 * 1024, (int) ($targetBytes / $segments));
        $ranges = [];

        $handle = \fopen($inputPath, 'rb');
        if ($handle === false) {
            return [];
        }

        for ($i = 0; $i < $segments; $i++) {
            $center = (int) ($inputBytes * ($i + 1) / ($segments + 1));
            $start = $center - (int) ($segmentBytes / 2);
            if ($start < 0) {
                $start = 0;
            }

            $end = $start + $segmentBytes;
            if ($end > $inputBytes) {
                $end = $inputBytes;
            }

            if ($start > 0) {
                \fseek($handle, $start);
                \fgets($handle);
                $start = \ftell($handle);
            }

            if ($end < $inputBytes) {
                \fseek($handle, $end);
                \fgets($handle);
                $end = \ftell($handle);
            }

            if ($end > $start) {
                $ranges[] = ['start' => $start, 'end' => $end];
            }
        }

        \fclose($handle);

        return $ranges;
    }

    /**
     * @param list<array{start:int,end:int}> $ranges
     * @param array<string,int> $slugIdByKey
     * @param array<string,string> $dayIdTokens
     */
    private static function benchmarkPilotPlan(
        string $inputPath,
        array $ranges,
        int $workers,
        array $slugIdByKey,
        array $dayIdTokens,
    ): float {
        $rangeCount = \count($ranges);
        if ($rangeCount === 0) {
            return PHP_FLOAT_MAX;
        }

        $workerTotal = \max(1, \min($workers, $rangeCount));
        $queueFile = \sys_get_temp_dir() . '/parser_pilot_' . \getmypid() . '_' . \uniqid('', true);
        \file_put_contents($queueFile, \pack('V', 0));

        $pids = [];
        $startNs = \hrtime(true);

        for ($i = 0; $i < $workerTotal - 1; $i++) {
            $pid = \pcntl_fork();
            if ($pid === 0) {
                self::runPilotWorker($inputPath, $queueFile, $ranges, $rangeCount, $slugIdByKey, $dayIdTokens);
                exit(0);
            }

            if ($pid > 0) {
                $pids[] = $pid;
            }
        }

        self::runPilotWorker($inputPath, $queueFile, $ranges, $rangeCount, $slugIdByKey, $dayIdTokens);

        foreach ($pids as $pid) {
            \pcntl_waitpid($pid, $status);
        }

        @\unlink($queueFile);

        return (\hrtime(true) - $startNs) / 1_000_000;
    }

    /**
     * @param list<array{start:int,end:int}> $ranges
     * @param array<string,int> $slugIdByKey
     * @param array<string,string> $dayIdTokens
     */
    private static function runPilotWorker(
        string $inputPath,
        string $queueFile,
        array $ranges,
        int $rangeCount,
        array $slugIdByKey,
        array $dayIdTokens,
    ): void {
        $fh = \fopen($inputPath, 'rb');
        $qf = \fopen($queueFile, 'c+b');

        if ($fh === false || $qf === false) {
            if ($fh !== false) {
                \fclose($fh);
            }
            if ($qf !== false) {
                \fclose($qf);
            }

            return;
        }

        while (true) {
            $idx = self::claimChunkFromFileQueue($qf, $rangeCount);
            if ($idx === -1) {
                break;
            }

            $range = $ranges[$idx];
            self::consumeRangeForPilot($fh, $range['start'], $range['end'], $slugIdByKey, $dayIdTokens);
        }

        \fclose($qf);
        \fclose($fh);
    }

    private static function consumeRangeForPilot($handle, $start, $end, $slugIdByKey, $dayIdTokens): void
    {
        fseek($handle, $start);

        $remaining = $end - $start;
        $bufSize   = self::READ_BLOCK_BYTES;
        $prefixLen = self::URL_PREFIX_BYTES;

        while ($remaining > 0) {
            $toRead = $remaining > $bufSize ? $bufSize : $remaining;
            $chunk  = fread($handle, $toRead);
            if (!$chunk) break;

            $chunkLen   = strlen($chunk);
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) continue;

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $p = $prefixLen;
            while ($p < $lastNl) {
                $sep = strpos($chunk, ',', $p);
                if ($sep === false || $sep >= $lastNl) break;

                $slug = substr($chunk, $p, $sep - $p);
                $day = substr($chunk, $sep + 3, 8);
                isset($slugIdByKey[$slug], $dayIdTokens[$day]);
                $p = $sep + 52;
            }
        }
    }

    private static function claimChunkFromSharedQueue($queueShm, $sem, $chunkTotal)
    {
        sem_acquire($sem);
        $idx = unpack('V', shmop_read($queueShm, 0, 4))[1];
        if ($idx >= $chunkTotal) {
            sem_release($sem);
            return -1;
        }
        shmop_write($queueShm, pack('V', $idx + 1), 0);
        sem_release($sem);
        return $idx;
    }

    private static function claimChunkFromFileQueue($qf, $chunkTotal)
    {
        flock($qf, LOCK_EX);
        fseek($qf, 0);
        $idx = unpack('V', fread($qf, 4))[1];
        if ($idx >= $chunkTotal) {
            flock($qf, LOCK_UN);
            return -1;
        }
        fseek($qf, 0);
        fwrite($qf, pack('V', $idx + 1));
        fflush($qf);
        flock($qf, LOCK_UN);
        return $idx;
    }

    private static function shouldUseStaticChunkSchedule(int $workerTotal, int $chunkTotal): bool
    {
        $env = \getenv('PARSER_STATIC_SCHEDULE');
        if (\is_string($env) && $env !== '') {
            return $env === '1';
        }

        return false;
    }

    private static function consumeAssignedChunks(
        $handle,
        array $chunkOffsets,
        int $chunkTotal,
        int $workerIndex,
        int $workerTotal,
        array $slugIdByKey,
        array $dayIdTokens,
        array &$buckets,
    ): void {
        $from = intdiv($chunkTotal * $workerIndex, $workerTotal);
        $to = intdiv($chunkTotal * ($workerIndex + 1), $workerTotal);

        for ($ci = $from; $ci < $to; $ci++) {
            self::consumeRangeIntoBuckets($handle, $chunkOffsets[$ci], $chunkOffsets[$ci + 1], $slugIdByKey, $dayIdTokens, $buckets);
        }
    }

    private static function consumeRangeIntoBuckets($handle, $start, $end, $slugIdByKey, $dayIdTokens, &$buckets)
    {
        fseek($handle, $start);

        $remaining = $end - $start;
        $bufSize = self::READ_BLOCK_BYTES;
        $prefixLen = self::URL_PREFIX_BYTES;

        while ($remaining > 0) {
            $toRead = $remaining > $bufSize ? $bufSize : $remaining;
            $chunk = fread($handle, $toRead);
            if (!$chunk) break;

            $chunkLen = strlen($chunk);
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) continue;

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $p     = $prefixLen;
            $fence = $lastNl - 594;

            while ($p < $fence) {
                $sep = strpos($chunk, ',', $p);
                $buckets[$slugIdByKey[substr($chunk, $p, $sep - $p)]] .= $dayIdTokens[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$slugIdByKey[substr($chunk, $p, $sep - $p)]] .= $dayIdTokens[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$slugIdByKey[substr($chunk, $p, $sep - $p)]] .= $dayIdTokens[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$slugIdByKey[substr($chunk, $p, $sep - $p)]] .= $dayIdTokens[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$slugIdByKey[substr($chunk, $p, $sep - $p)]] .= $dayIdTokens[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$slugIdByKey[substr($chunk, $p, $sep - $p)]] .= $dayIdTokens[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;
            }

            while ($p < $lastNl) {
                $sep = strpos($chunk, ',', $p);
                if ($sep === false || $sep >= $lastNl) break;
                $buckets[$slugIdByKey[substr($chunk, $p, $sep - $p)]] .= $dayIdTokens[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;
            }
        }
    }

    private static function reduceBucketsToCounts(&$buckets, $slugTotal, $dateCount)
    {
        $counts = array_fill(0, $slugTotal * $dateCount, 0);
        $base = 0;
        $smallBucketThreshold = self::smallBucketThreshold();

        foreach ($buckets as $bucket) {
            if ($bucket === '') {
                $base += $dateCount;
                continue;
            }

            $len = strlen($bucket);
            if ($len === 2) {
                $did = \ord($bucket[0]) | (\ord($bucket[1]) << 8);
                $counts[$base + $did]++;
                $base += $dateCount;
                continue;
            }

            if ($len === 4) {
                $did0 = \ord($bucket[0]) | (\ord($bucket[1]) << 8);
                $did1 = \ord($bucket[2]) | (\ord($bucket[3]) << 8);
                if ($did0 === $did1) {
                    $counts[$base + $did0] += 2;
                } else {
                    $counts[$base + $did0]++;
                    $counts[$base + $did1]++;
                }
                $base += $dateCount;
                continue;
            }

            if ($len <= $smallBucketThreshold) {
                for ($i = 0; $i < $len; $i += 2) {
                    $did = \ord($bucket[$i]) | (\ord($bucket[$i + 1]) << 8);
                    $counts[$base + $did]++;
                }
            } else {
                foreach (array_count_values(unpack('v*', $bucket)) as $did => $cnt) {
                    $counts[$base + $did] += $cnt;
                }
            }

            $base += $dateCount;
        }

        return $counts;
    }

    private static function smallBucketThreshold(): int
    {
        static $threshold = null;

        if ($threshold !== null) {
            return $threshold;
        }

        $raw = \getenv('PARSER_REDUCE_SMALL_BUCKET');
        if (!\is_string($raw) || $raw === '') {
            $threshold = 0;
            return $threshold;
        }

        $value = (int) $raw;
        if ($value < 0) {
            $value = 0;
        } elseif ($value > 256) {
            $value = 256;
        }

        if (($value & 1) === 1) {
            $value--;
        }

        $threshold = $value;

        return $threshold;
    }

    private static function flushJsonOutput($outputPath, $counts, $slugKeyById, $dayKeyById, $dateCount)
    {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);

        $slugTotal    = count($slugKeyById);
        $datePrefixes = [];
        $escapedPaths = [];

        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "20' . $dayKeyById[$d] . '": ';
        }

        for ($p = 0; $p < $slugTotal; $p++) {
            $escapedPaths[$p] = "\"\\/blog\\/" . str_replace('/', '\\/', $slugKeyById[$p]) . '"';
        }

        fwrite($out, '{');
        $firstPath = true;
        $base      = 0;

        for ($p = 0; $p < $slugTotal; $p++) {
            $dateEntries = [];

            for ($d = 0; $d < $dateCount; $d++) {
                $count = $counts[$base + $d];
                if ($count !== 0) {
                    $dateEntries[] = $datePrefixes[$d] . $count;
                }
            }

            if (empty($dateEntries)) {
                $base += $dateCount;
                continue;
            }

            $sep2      = $firstPath ? '' : ',';
            $firstPath = false;

            fwrite($out,
                $sep2 .
                "\n    " . $escapedPaths[$p] . ": {\n" .
                implode(",\n", $dateEntries) .
                "\n    }"
            );

            $base += $dateCount;
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
