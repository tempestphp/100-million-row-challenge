<?php

namespace App\Traits;

/**
 * V2 Goals
 * -
 *
 * Trying to account for the difference in processing speed between performance and economy
 * cores so that we're not sitting around waiting for the 4 economy cores because we assigned
 * all cores the same workload. We're taking a 50ms hit on overhead to assign work optimally.
 */
trait WorkerHybridV1Trait {
    private function work(int $start, int $end, int $index, $outPath): array
    {
        $buckets = array_fill(0, $this->urlCount, '');

        // Hoist these properties to prevent Zend engine hash lookups from $this within the hot path
        $urlTokens  = $this->urlTokens;
        $dateChars  = $this->dateChars;
        $minLineLen = $this->minLineLength;

        $handle = fopen($this->inputPath, 'rb', false);
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);

        $totalRowCount = 0;
        $tReadTotal = 0;

        $fastPathRows  = 0;
        $cleanupRows   = 0;
        $minRowLen     = PHP_INT_MAX;
        $maxRowLen     = 0;
        $totalRowBytes = 0;
        $rowCount      = 0;

        $remaining = $end - $start;

        while ($remaining > 0) {

            // -- Read one window ----
            $toRead = min($remaining, self::READ_BUFFER);
            $window = fread($handle, $toRead);

            if ($window === false || $window === '') break;

            $windowLen = strlen($window);
            $lastNl = strrpos($window, "\n");

            if ($lastNl === false) {
                $remaining -= $windowLen;
                continue;
            }

            $tail = $windowLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
            }

            $remaining -= ($windowLen - $tail);
            $windowEnd = $lastNl;
            $wStart = 0;

            // Original 5x unrolled fence value
            //$fence = $windowEnd - 600;

            // Optimized fence value - (unroll_factor * maxRowLen) + buffer
            $fence = $windowEnd - ((15 * 99) + 0);

            while ($wStart < $fence) {

                // MinLineLen = 35
                // DOMAIN_LENGTH = 25
                // DATE_WIDTH = 25
                // DATE_LENGTH = 10
                // $wEnd = strpos($window, "\n", $wStart + $minLineLen);
                // $buckets[$urlTokens[substr($window, $wStart + self::DOMAIN_LENGTH, $wEnd - $wStart - self::DOMAIN_LENGTH - self::DATE_WIDTH - 1)]]
                //     .= $dateChars[substr($window, $wEnd - self::DATE_WIDTH, self::DATE_LENGTH)];
                // $wStart = $wEnd + 1;


                //       DOMAIN LENGTH      VARIABLE         DATE WIDTH
                //                        |         | |
                // http://example.org/blog/sample-url,2026-01-01T01:01:01+00:00

                $wEnd = strpos($window, "\n", $wStart + 35);
                $buckets[$urlTokens[substr($window, $wStart + 25, $wEnd - $wStart - 51)]]
                    .= $dateChars[substr($window, $wEnd - 25, 10)];
                $wStart = $wEnd + 1;

                $wEnd = strpos($window, "\n", $wStart + 35);
                $buckets[$urlTokens[substr($window, $wStart + 25, $wEnd - $wStart - 51)]]
                    .= $dateChars[substr($window, $wEnd - 25, 10)];
                $wStart = $wEnd + 1;

                $wEnd = strpos($window, "\n", $wStart + 35);
                $buckets[$urlTokens[substr($window, $wStart + 25, $wEnd - $wStart - 51)]]
                    .= $dateChars[substr($window, $wEnd - 25, 10)];
                $wStart = $wEnd + 1;

                $wEnd = strpos($window, "\n", $wStart + 35);
                $buckets[$urlTokens[substr($window, $wStart + 25, $wEnd - $wStart - 51)]]
                    .= $dateChars[substr($window, $wEnd - 25, 10)];
                $wStart = $wEnd + 1;

                $wEnd = strpos($window, "\n", $wStart + 35);
                $buckets[$urlTokens[substr($window, $wStart + 25, $wEnd - $wStart - 51)]]
                    .= $dateChars[substr($window, $wEnd - 25, 10)];
                $wStart = $wEnd + 1;

                // --

                $wEnd = strpos($window, "\n", $wStart + 35);
                $buckets[$urlTokens[substr($window, $wStart + 25, $wEnd - $wStart - 51)]]
                    .= $dateChars[substr($window, $wEnd - 25, 10)];
                $wStart = $wEnd + 1;

                $wEnd = strpos($window, "\n", $wStart + 35);
                $buckets[$urlTokens[substr($window, $wStart + 25, $wEnd - $wStart - 51)]]
                    .= $dateChars[substr($window, $wEnd - 25, 10)];
                $wStart = $wEnd + 1;

                $wEnd = strpos($window, "\n", $wStart + 35);
                $buckets[$urlTokens[substr($window, $wStart + 25, $wEnd - $wStart - 51)]]
                    .= $dateChars[substr($window, $wEnd - 25, 10)];
                $wStart = $wEnd + 1;

                $wEnd = strpos($window, "\n", $wStart + 35);
                $buckets[$urlTokens[substr($window, $wStart + 25, $wEnd - $wStart - 51)]]
                    .= $dateChars[substr($window, $wEnd - 25, 10)];
                $wStart = $wEnd + 1;

                $wEnd = strpos($window, "\n", $wStart + 35);
                $buckets[$urlTokens[substr($window, $wStart + 25, $wEnd - $wStart - 51)]]
                    .= $dateChars[substr($window, $wEnd - 25, 10)];
                $wStart = $wEnd + 1;

                // --

                $wEnd = strpos($window, "\n", $wStart + 35);
                $buckets[$urlTokens[substr($window, $wStart + 25, $wEnd - $wStart - 51)]]
                    .= $dateChars[substr($window, $wEnd - 25, 10)];
                $wStart = $wEnd + 1;

                $wEnd = strpos($window, "\n", $wStart + 35);
                $buckets[$urlTokens[substr($window, $wStart + 25, $wEnd - $wStart - 51)]]
                    .= $dateChars[substr($window, $wEnd - 25, 10)];
                $wStart = $wEnd + 1;

                $wEnd = strpos($window, "\n", $wStart + 35);
                $buckets[$urlTokens[substr($window, $wStart + 25, $wEnd - $wStart - 51)]]
                    .= $dateChars[substr($window, $wEnd - 25, 10)];
                $wStart = $wEnd + 1;

                $wEnd = strpos($window, "\n", $wStart + 35);
                $buckets[$urlTokens[substr($window, $wStart + 25, $wEnd - $wStart - 51)]]
                    .= $dateChars[substr($window, $wEnd - 25, 10)];
                $wStart = $wEnd + 1;

                $wEnd = strpos($window, "\n", $wStart + 35);
                $buckets[$urlTokens[substr($window, $wStart + 25, $wEnd - $wStart - 51)]]
                    .= $dateChars[substr($window, $wEnd - 25, 10)];
                $wStart = $wEnd + 1;

                // --
            }

            // Smaller unroll to compensate for much larger initial unroll
            $fence = $windowEnd - 200;

            while ($wStart < $fence) {
                $wEnd = strpos($window, "\n", $wStart + 35);
                $buckets[$urlTokens[substr($window, $wStart + 25, $wEnd - $wStart - 51)]]
                    .= $dateChars[substr($window, $wEnd - 25, 10)];
                $wStart = $wEnd + 1;

                $wEnd = strpos($window, "\n", $wStart + 35);
                $buckets[$urlTokens[substr($window, $wStart + 25, $wEnd - $wStart - 51)]]
                    .= $dateChars[substr($window, $wEnd - 25, 10)];
                $wStart = $wEnd + 1;
            }

            // -- Cleanup loop for rows after the fence ----
            while ($wStart < $windowEnd) {
                $wEnd = strpos($window, "\n", $wStart + 35);
                if ($wEnd === false || $wEnd > $windowEnd) break;
                $buckets[$urlTokens[substr($window, $wStart + 25, $wEnd - $wStart - 51)]]
                    .= $dateChars[substr($window, $wEnd - 25, 10)];
                $wStart = $wEnd + 1;
            }
        }

        fclose($handle);

        // Convert buckets to flat counts array
        $counts = array_fill(0, $this->urlCount * $this->dateCount, 0);
        $maxVal = 0;

        for ($s = 0; $s < $this->urlCount; $s++) {
            if ($buckets[$s] === '') continue;
            $base = $s * $this->dateCount;
            foreach (array_count_values(unpack('v*', $buckets[$s])) as $dateId => $count) {
                $counts[$base + $dateId] = $count;
                if ($count > $maxVal) $maxVal = $count;
            }
        }

        if (null !== $outPath) {
            // Send flat counts to parent via file
            file_put_contents($outPath, pack('V*', ...$counts));
            print("Worker {$index} wrote to {$outPath}").PHP_EOL;
        }

        return $counts;
    }
}