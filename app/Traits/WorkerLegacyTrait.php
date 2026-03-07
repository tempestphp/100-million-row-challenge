<?php

namespace App\Traits;

trait WorkerLegacyTrait {

    private function work(
        int $start,
        int $end,
        int $index,
        mixed $shmSegment = null,
        int $shmOffset = 0,
        mixed $controlSocket = null
    ): array
    {
        // Hoist these properties to prevent Zend engine hash lookups from $this within the hot path
        $urlTokens  = $this->urlTokens;
        $dateChars  = $this->dateChars;

        $buckets = array_fill(0, $this->urlCount, '');

        $handle = fopen($this->inputPath, 'rb', false);
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);

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

        if ($shmSegment !== null) {
            shmop_write($shmSegment, pack('C*', ...$counts), $shmOffset);

            // Ping parent that we're done
            if ($controlSocket !== null) {
                fwrite($controlSocket, "\x01");
            }
        }

        print("Worker $index max: $maxVal").PHP_EOL;

        return $counts;
    }
}