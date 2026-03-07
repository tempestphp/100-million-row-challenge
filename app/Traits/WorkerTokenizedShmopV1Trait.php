<?php

namespace App\Traits;

trait WorkerTokenizedShmopV1Trait {

    private function work(int $start, int $end, int $index, \Shmop $shm, int $sliceSize): void
    {
        $buckets = array_fill(0, $this->urlCount, '');

        $handle = fopen($this->inputPath, 'rb', false);
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);

        // Hoist these properties to prevent Zend engine hash lookups from $this within the hot path
        $urlTokens  = $this->urlTokens;
        $dateChars  = $this->dateChars;
        $minLineLen = $this->minLineLength;

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

            // 5x Unrolled fast path
            $fence = $windowEnd - 600;

            while ($wStart < $fence) {
                $wEnd = strpos($window, "\n", $wStart + $minLineLen);
                $buckets[$urlTokens[substr($window, $wStart + self::DOMAIN_LENGTH, $wEnd - $wStart - self::DOMAIN_LENGTH - self::DATE_WIDTH - 1)] ?? -1]
                    .= $dateChars[substr($window, $wEnd - self::DATE_WIDTH, self::DATE_LENGTH)] ?? '';
                $wStart = $wEnd + 1;

                $wEnd = strpos($window, "\n", $wStart + $minLineLen);
                $buckets[$urlTokens[substr($window, $wStart + self::DOMAIN_LENGTH, $wEnd - $wStart - self::DOMAIN_LENGTH - self::DATE_WIDTH - 1)] ?? -1]
                    .= $dateChars[substr($window, $wEnd - self::DATE_WIDTH, self::DATE_LENGTH)] ?? '';
                $wStart = $wEnd + 1;

                $wEnd = strpos($window, "\n", $wStart + $minLineLen);
                $buckets[$urlTokens[substr($window, $wStart + self::DOMAIN_LENGTH, $wEnd - $wStart - self::DOMAIN_LENGTH - self::DATE_WIDTH - 1)] ?? -1]
                    .= $dateChars[substr($window, $wEnd - self::DATE_WIDTH, self::DATE_LENGTH)] ?? '';
                $wStart = $wEnd + 1;

                $wEnd = strpos($window, "\n", $wStart + $minLineLen);
                $buckets[$urlTokens[substr($window, $wStart + self::DOMAIN_LENGTH, $wEnd - $wStart - self::DOMAIN_LENGTH - self::DATE_WIDTH - 1)] ?? -1]
                    .= $dateChars[substr($window, $wEnd - self::DATE_WIDTH, self::DATE_LENGTH)] ?? '';
                $wStart = $wEnd + 1;

                $wEnd = strpos($window, "\n", $wStart + $minLineLen);
                $buckets[$urlTokens[substr($window, $wStart + self::DOMAIN_LENGTH, $wEnd - $wStart - self::DOMAIN_LENGTH - self::DATE_WIDTH - 1)] ?? -1]
                    .= $dateChars[substr($window, $wEnd - self::DATE_WIDTH, self::DATE_LENGTH)] ?? '';
                $wStart = $wEnd + 1;
            }

            // -- Cleanup loop for rows after the fence ----
            while ($wStart < $windowEnd) {
                $wEnd = strpos($window, "\n", $wStart + $minLineLen);

                if ($wEnd === false || $wEnd > $windowEnd) break;

                $urlToken = $urlTokens[substr($window, $wStart + self::DOMAIN_LENGTH, $wEnd - $wStart - self::DOMAIN_LENGTH - self::DATE_WIDTH - 1)];
                $dateChar = $dateChars[substr($window, $wEnd - self::DATE_WIDTH, self::DATE_LENGTH)];

                $buckets[$urlToken] .= $dateChar;

                $wStart = $wEnd + 1;
            }
        }

        fclose($handle);

        // Convert buckets to flat counts array
        $counts = array_fill(0, $this->urlCount * $this->dateCount, 0);

        for ($s = 0; $s < $this->urlCount; $s++) {
            if ($buckets[$s] === '') continue;
            $base = $s * $this->dateCount;
            foreach (array_count_values(unpack('v*', $buckets[$s])) as $dateId => $count) {
                $counts[$base + $dateId] = $count;
            }
        }

        // Send flat counts to parent via shmop
        shmop_write($shm, pack('V*', ...$counts), $index * $sliceSize);
    }

}