<?php

namespace App\Traits;

/**
 * V3
 *
 * I hate that this is faster...
 */
trait WorkerTokenizedSocketV3Trait {
    private function work(int $start, int $end, int $index, $writeSocket = null): array
    {
        $buckets = array_fill(0, $this->urlCount, '');

        // Hoist these properties to prevent Zend engine hash lookups from $this within the hot path
        $urlTokens  = $this->urlTokens;
        $dateChars  = $this->dateChars;

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

            // MinLineLen = 35
            // DOMAIN_LENGTH = 25
            // DATE_WIDTH = 25
            // DATE_LENGTH = 10
            // $wEnd = strpos($window, "\n", $wStart + $minLineLen);
            // $buckets[$urlTokens[substr($window, $wStart + self::DOMAIN_LENGTH, $wEnd - $wStart - self::DOMAIN_LENGTH - self::DATE_WIDTH - 1)]]
            //     .= $dateChars[substr($window, $wEnd - self::DATE_WIDTH, self::DATE_LENGTH)];
            // $wStart = $wEnd + 1;

            // Use the optimization at C level already done here with a single system call
            // SO annoyed that this is faster at least locally.
            preg_match_all('/blog\/([^,]+),(\d{4}-\d{2}-\d{2})/', $window, $m);

            $slugs = $m[1];
            $dates = $m[2];
            $count = count($slugs);

            for ($i = 0; $i < $count; $i++) {
                $buckets[$urlTokens[$slugs[$i]]] .= $dateChars[$dates[$i]];
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

        // Send flat counts to parent via socket
        //$maxVal = count($counts) > 0 ? max($counts) : 0;
        $v16 = $maxVal <= 65535;

        if ($writeSocket !== null) {
            $prefix = $v16 ? "\x00" : "\x01";
            $fmt = $v16 ? 'v*' : 'V*';
            $output = $prefix . pack($fmt, ...$counts);
            fwrite($writeSocket, $output);

            print("Worker $index Output: len: " . strlen($output) . " max: ").PHP_EOL;
        }

        return $counts;
    }
}