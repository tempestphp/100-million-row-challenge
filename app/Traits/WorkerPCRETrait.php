<?php

namespace App\Traits;

trait WorkerPCRETrait {
    private function work(int $start, int $end, int $index, string $outPath): void
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

            // Use the optimization at C level already done here with a single system call
            // I'm very annoyed that this is faster ...
            $safeWindow = substr($window, 0, $lastNl + 1);
            preg_match_all('/blog\/([^,]+),(\d{4}-\d{2}-\d{2})/', $safeWindow, $m);

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

        // Send flat counts to parent via file
        file_put_contents($outPath, pack('V*', ...$counts));
    }
}