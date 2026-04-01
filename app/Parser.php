<?php

namespace App;

use App\Commands\Visit;

final class Parser
{
    private const int NUM_PROCS = 10;
    private const int CHUNK_BYTES = 163_840;
    private const int SAMPLE_SIZE = 2_097_152;

    public function __call(string $name, array $arguments): mixed
    {
        return static::$name(...$arguments);
    }

    public static function parse($inputPath, $outputPath, $workers = null)
    {
        gc_disable();

        $totalBytes = filesize($inputPath);

        // Build date mapping: "YY-MM-DD" => sequential id, and reverse
        $dateMap = [];
        $dateLabels = [];
        $numDates = 0;
        foreach (range(21, 26) as $yr) {
            foreach (range(1, 12) as $mo) {
                $daysInMonth = match ($mo) {
                    2 => $yr === 24 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $moStr = str_pad($mo, 2, '0', STR_PAD_LEFT);
                for ($day = 1; $day <= $daysInMonth; $day++) {
                    $label = "{$yr}-{$moStr}-" . str_pad($day, 2, '0', STR_PAD_LEFT);
                    $dateMap[$label] = $numDates;
                    $dateLabels[$numDates] = $label;
                    $numDates++;
                }
            }
        }

        // Pre-encode date ids as 2-byte little-endian binary
        $dateBin = [];
        foreach ($dateMap as $label => $id) {
            $dateBin[$label] = chr($id & 0xFF) . chr($id >> 8);
        }

        // Sample first portion of file to discover URL slugs
        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);
        $sampleLen = min($totalBytes, self::SAMPLE_SIZE);
        $sample = fread($fh, $sampleLen);
        fclose($fh);

        $slugIndex = [];
        $slugList = [];
        $numSlugs = 0;
        $cursor = 0;
        $sampleEnd = strrpos($sample, "\n");

        while ($cursor < $sampleEnd) {
            $eol = strpos($sample, "\n", $cursor + 52);
            if ($eol === false) break;

            $s = substr($sample, $cursor + 25, $eol - $cursor - 51);
            if (!isset($slugIndex[$s])) {
                $slugIndex[$s] = $numSlugs;
                $slugList[$numSlugs] = $s;
                $numSlugs++;
            }
            $cursor = $eol + 1;
        }
        unset($sample);

        // Ensure all known visit URIs are indexed
        foreach (Visit::all() as $v) {
            $s = substr($v->uri, 25);
            if (!isset($slugIndex[$s])) {
                $slugIndex[$s] = $numSlugs;
                $slugList[$numSlugs] = $s;
                $numSlugs++;
            }
        }

        // Split file into chunks aligned on newline boundaries
        $offsets = [0];
        $fh = fopen($inputPath, 'rb');
        for ($i = 1; $i < self::NUM_PROCS; $i++) {
            fseek($fh, (int) ($totalBytes * $i / self::NUM_PROCS));
            fgets($fh);
            $offsets[] = ftell($fh);
        }
        fclose($fh);
        $offsets[] = $totalBytes;

        // Fork workers
        $shmDir = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
        $parentPid = getmypid();
        $children = [];

        for ($w = 0; $w < self::NUM_PROCS - 1; $w++) {
            $outFile = "{$shmDir}/parse_{$parentPid}_{$w}";
            $pid = pcntl_fork();
            if ($pid === 0) {
                $result = self::processSegment(
                    $inputPath, $offsets[$w], $offsets[$w + 1],
                    $slugList, $slugIndex, $dateBin, $numSlugs, $numDates,
                );
                file_put_contents($outFile, pack('v*', ...$result));
                posix_kill(posix_getpid(), SIGKILL);
            }
            $children[$pid] = $outFile;
        }

        // Parent handles last segment
        $counts = self::processSegment(
            $inputPath, $offsets[self::NUM_PROCS - 1], $offsets[self::NUM_PROCS],
            $slugList, $slugIndex, $dateBin, $numSlugs, $numDates,
        );

        // Collect child results
        $remaining = count($children);
        while ($remaining > 0) {
            $pid = pcntl_wait($st, WNOHANG);
            if ($pid <= 0) {
                $pid = pcntl_wait($st);
            }
            if (isset($children[$pid])) {
                $outFile = $children[$pid];
                $childData = unpack('v*', file_get_contents($outFile));
                unlink($outFile);
                $idx = 0;
                foreach ($childData as $val) {
                    $counts[$idx++] += $val;
                }
                $remaining--;
            }
        }

        self::emitJson($outputPath, $counts, $slugList, $dateLabels, $numDates);
    }

    private static function processSegment(
        $inputPath, $from, $to,
        $slugList, $slugIndex, $dateBin,
        $numSlugs, $numDates,
    ) {
        // Key buckets by slug string directly — avoids extra hash lookup vs integer indirection
        $bins = array_fill_keys($slugList, '');

        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);
        fseek($fh, $from);
        $left = $to - $from;

        while ($left > 0) {
            $readLen = $left > self::CHUNK_BYTES ? self::CHUNK_BYTES : $left;
            $buf = fread($fh, $readLen);
            $bufLen = strlen($buf);
            if ($bufLen === 0) break;
            $left -= $bufLen;

            $nl = strrpos($buf, "\n");
            if ($nl === false) break;

            $overflow = $bufLen - $nl - 1;
            if ($overflow > 0) {
                fseek($fh, -$overflow, SEEK_CUR);
                $left += $overflow;
            }

            $i = 25;
            $safeLimit = $nl - 720;

            // Unrolled 6x for the bulk of each buffer
            while ($i < $safeLimit) {
                $c = strpos($buf, ',', $i);
                $bins[substr($buf, $i, $c - $i)] .= $dateBin[substr($buf, $c + 3, 8)];
                $i = $c + 52;

                $c = strpos($buf, ',', $i);
                $bins[substr($buf, $i, $c - $i)] .= $dateBin[substr($buf, $c + 3, 8)];
                $i = $c + 52;

                $c = strpos($buf, ',', $i);
                $bins[substr($buf, $i, $c - $i)] .= $dateBin[substr($buf, $c + 3, 8)];
                $i = $c + 52;

                $c = strpos($buf, ',', $i);
                $bins[substr($buf, $i, $c - $i)] .= $dateBin[substr($buf, $c + 3, 8)];
                $i = $c + 52;

                $c = strpos($buf, ',', $i);
                $bins[substr($buf, $i, $c - $i)] .= $dateBin[substr($buf, $c + 3, 8)];
                $i = $c + 52;

                $c = strpos($buf, ',', $i);
                $bins[substr($buf, $i, $c - $i)] .= $dateBin[substr($buf, $c + 3, 8)];
                $i = $c + 52;
            }

            // Remaining lines near buffer end
            while ($i < $nl) {
                $c = strpos($buf, ',', $i);
                if ($c === false || $c >= $nl) break;
                $bins[substr($buf, $i, $c - $i)] .= $dateBin[substr($buf, $c + 3, 8)];
                $i = $c + 52;
            }
        }

        fclose($fh);

        // Tally: convert binary bucket strings into flat count array
        $tally = array_fill(0, $numSlugs * $numDates, 0);
        foreach ($bins as $slug => $raw) {
            if ($raw === '') continue;
            $base = $slugIndex[$slug] * $numDates;
            foreach (array_count_values(unpack('v*', $raw)) as $dateId => $n) {
                $tally[$base + $dateId] += $n;
            }
        }

        return $tally;
    }

    private static function emitJson(
        $outputPath, $counts, $slugList,
        $dateLabels, $numDates,
    ) {
        $fp = fopen($outputPath, 'wb');
        stream_set_write_buffer($fp, 1_048_576);
        fwrite($fp, '{');

        $dateFmt = [];
        for ($d = 0; $d < $numDates; $d++) {
            $dateFmt[$d] = '        "20' . $dateLabels[$d] . '": ';
        }

        $numSlugs = count($slugList);
        $pathFmt = [];
        for ($i = 0; $i < $numSlugs; $i++) {
            $pathFmt[$i] = "\"\\/blog\\/" . str_replace('/', '\\/', $slugList[$i]) . "\"";
        }

        $first = true;
        for ($i = 0; $i < $numSlugs; $i++) {
            $base = $i * $numDates;
            $entries = [];

            for ($d = 0; $d < $numDates; $d++) {
                $n = $counts[$base + $d];
                if ($n === 0) continue;
                $entries[] = $dateFmt[$d] . $n;
            }

            if ($entries === []) continue;

            $line = $first ? "\n    " : ",\n    ";
            $first = false;
            $line .= $pathFmt[$i] . ": {\n" . implode(",\n", $entries) . "\n    }";
            fwrite($fp, $line);
        }

        fwrite($fp, "\n}");
        fclose($fp);
    }
}
