<?php

namespace App\Traits;

trait WorkerLegacyTraitV2 {

    private function work(
        int $start,
        int $end,
        int $index,
        ?string $outPath = null,
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
            $chunk = fread($handle, $toRead);

            if ($chunk === false || $chunk === '') break;

            $chunkLen = strlen($chunk);
            $lastNl = strrpos($chunk, "\n");

            if ($lastNl === false) {
                $remaining -= $chunkLen;
                continue;
            }

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
            }

            $remaining -= ($chunkLen - $tail);
            $chunkEnd = $lastNl;
            $rowOffset = 0;

            // Original 5x unrolled fence value
            //$fence = $windowEnd - 600;

            // Optimized fence value - (unroll_factor * maxRowLen) + buffer
            $fence = $chunkEnd - ((15 * 99) + 0);

            while ($rowOffset < $fence) {

                // MinLineLen = 35
                // DOMAIN_LENGTH = 25
                // DATE_WIDTH = 25
                // DATE_LENGTH = 10
                // $wEnd = strpos($window, "\n", $wStart + $minLineLen);
                // $buckets[$urlTokens[substr($window, $wStart + self::DOMAIN_LENGTH, $wEnd - $wStart - self::DOMAIN_LENGTH - self::DATE_WIDTH - 1)]]
                //     .= $dateChars[substr($window, $wEnd - self::DATE_WIDTH, self::DATE_LENGTH)];
                // $wStart = $wEnd + 1;

//                $rowComma = strpos($chunk, ",", $rowOffset + 30); // MinUrlSlug (5) + Domain Prefix (25)
//
//                echo "#$index Full Line: " . substr($chunk, $rowOffset, 120) . "\n";
//                echo "#$index URL: " . substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25) . "\n";
//                echo "#$index Date: " . substr($chunk, $rowComma + 1,10) . "\n";
//                echo "#$index Next Line: " . substr($chunk, $rowComma + 27, 100) . "\n";
//                exit;

                $rowComma = strpos($chunk, ",", $rowOffset + 29); // MinUrlSlug (4) + Domain Prefix (25)
                $buckets[$urlTokens[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)]]  // Start after Domain/blog and capture from blog/ to comma
                    .= $dateChars[substr($chunk, $rowComma + 1,10)];    // Capture prefix of date right after the comma
                $rowOffset = $rowComma + 27;   // Timestamp width + 1 for comma + 1 for newline

                $rowComma = strpos($chunk, ",", $rowOffset + 29); // MinUrlSlug (4) + Domain Prefix (25)
                $buckets[$urlTokens[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)]]  // Start after Domain/blog and capture from blog/ to comma
                    .= $dateChars[substr($chunk, $rowComma + 1,10)];    // Capture prefix of date right after the comma
                $rowOffset = $rowComma + 27;   // Timestamp width + 1 for comma + 1 for newline

                $rowComma = strpos($chunk, ",", $rowOffset + 29); // MinUrlSlug (4) + Domain Prefix (25)
                $buckets[$urlTokens[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)]]  // Start after Domain/blog and capture from blog/ to comma
                    .= $dateChars[substr($chunk, $rowComma + 1,10)];    // Capture prefix of date right after the comma
                $rowOffset = $rowComma + 27;   // Timestamp width + 1 for comma + 1 for newline

                $rowComma = strpos($chunk, ",", $rowOffset + 29); // MinUrlSlug (4) + Domain Prefix (25)
                $buckets[$urlTokens[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)]]  // Start after Domain/blog and capture from blog/ to comma
                    .= $dateChars[substr($chunk, $rowComma + 1,10)];    // Capture prefix of date right after the comma
                $rowOffset = $rowComma + 27;   // Timestamp width + 1 for comma + 1 for newline

                $rowComma = strpos($chunk, ",", $rowOffset + 29); // MinUrlSlug (4) + Domain Prefix (25)
                $buckets[$urlTokens[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)]]  // Start after Domain/blog and capture from blog/ to comma
                    .= $dateChars[substr($chunk, $rowComma + 1,10)];    // Capture prefix of date right after the comma
                $rowOffset = $rowComma + 27;   // Timestamp width + 1 for comma + 1 for newline

                //

                $rowComma = strpos($chunk, ",", $rowOffset + 29); // MinUrlSlug (4) + Domain Prefix (25)
                $buckets[$urlTokens[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)]]  // Start after Domain/blog and capture from blog/ to comma
                    .= $dateChars[substr($chunk, $rowComma + 1,10)];    // Capture prefix of date right after the comma
                $rowOffset = $rowComma + 27;   // Timestamp width + 1 for comma + 1 for newline

                $rowComma = strpos($chunk, ",", $rowOffset + 29); // MinUrlSlug (4) + Domain Prefix (25)
                $buckets[$urlTokens[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)]]  // Start after Domain/blog and capture from blog/ to comma
                    .= $dateChars[substr($chunk, $rowComma + 1,10)];    // Capture prefix of date right after the comma
                $rowOffset = $rowComma + 27;   // Timestamp width + 1 for comma + 1 for newline

                $rowComma = strpos($chunk, ",", $rowOffset + 29); // MinUrlSlug (4) + Domain Prefix (25)
                $buckets[$urlTokens[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)]]  // Start after Domain/blog and capture from blog/ to comma
                    .= $dateChars[substr($chunk, $rowComma + 1,10)];    // Capture prefix of date right after the comma
                $rowOffset = $rowComma + 27;   // Timestamp width + 1 for comma + 1 for newline

                $rowComma = strpos($chunk, ",", $rowOffset + 29); // MinUrlSlug (4) + Domain Prefix (25)
                $buckets[$urlTokens[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)]]  // Start after Domain/blog and capture from blog/ to comma
                    .= $dateChars[substr($chunk, $rowComma + 1,10)];    // Capture prefix of date right after the comma
                $rowOffset = $rowComma + 27;   // Timestamp width + 1 for comma + 1 for newline

                $rowComma = strpos($chunk, ",", $rowOffset + 29); // MinUrlSlug (4) + Domain Prefix (25)
                $buckets[$urlTokens[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)]]  // Start after Domain/blog and capture from blog/ to comma
                    .= $dateChars[substr($chunk, $rowComma + 1,10)];    // Capture prefix of date right after the comma
                $rowOffset = $rowComma + 27;   // Timestamp width + 1 for comma + 1 for newline

                //

                $rowComma = strpos($chunk, ",", $rowOffset + 29); // MinUrlSlug (4) + Domain Prefix (25)
                $buckets[$urlTokens[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)]]  // Start after Domain/blog and capture from blog/ to comma
                    .= $dateChars[substr($chunk, $rowComma + 1,10)];    // Capture prefix of date right after the comma
                $rowOffset = $rowComma + 27;   // Timestamp width + 1 for comma + 1 for newline

                $rowComma = strpos($chunk, ",", $rowOffset + 29); // MinUrlSlug (4) + Domain Prefix (25)
                $buckets[$urlTokens[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)]]  // Start after Domain/blog and capture from blog/ to comma
                    .= $dateChars[substr($chunk, $rowComma + 1,10)];    // Capture prefix of date right after the comma
                $rowOffset = $rowComma + 27;   // Timestamp width + 1 for comma + 1 for newline

                $rowComma = strpos($chunk, ",", $rowOffset + 29); // MinUrlSlug (4) + Domain Prefix (25)
                $buckets[$urlTokens[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)]]  // Start after Domain/blog and capture from blog/ to comma
                    .= $dateChars[substr($chunk, $rowComma + 1,10)];    // Capture prefix of date right after the comma
                $rowOffset = $rowComma + 27;   // Timestamp width + 1 for comma + 1 for newline

                $rowComma = strpos($chunk, ",", $rowOffset + 29); // MinUrlSlug (4) + Domain Prefix (25)
                $buckets[$urlTokens[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)]]  // Start after Domain/blog and capture from blog/ to comma
                    .= $dateChars[substr($chunk, $rowComma + 1,10)];    // Capture prefix of date right after the comma
                $rowOffset = $rowComma + 27;   // Timestamp width + 1 for comma + 1 for newline

                $rowComma = strpos($chunk, ",", $rowOffset + 29); // MinUrlSlug (4) + Domain Prefix (25)
                $buckets[$urlTokens[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)]]  // Start after Domain/blog and capture from blog/ to comma
                    .= $dateChars[substr($chunk, $rowComma + 1,10)];    // Capture prefix of date right after the comma
                $rowOffset = $rowComma + 27;   // Timestamp width + 1 for comma + 1 for newline

                //
            }

            // Smaller unroll to compensate for much larger initial unroll
            $fence = $chunkEnd - 200;

            while ($rowOffset < $fence) {
                $rowComma = strpos($chunk, ",", $rowOffset + 29); // MinUrlSlug (4) + Domain Prefix (25)
                $buckets[$urlTokens[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)]]  // Start after Domain/blog and capture from blog/ to comma
                    .= $dateChars[substr($chunk, $rowComma + 1,10)];    // Capture prefix of date right after the comma
                $rowOffset = $rowComma + 27;   // Timestamp width + 1 for comma + 1 for newline

                $rowComma = strpos($chunk, ",", $rowOffset + 29); // MinUrlSlug (4) + Domain Prefix (25)
                $buckets[$urlTokens[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)]]  // Start after Domain/blog and capture from blog/ to comma
                    .= $dateChars[substr($chunk, $rowComma + 1,10)];    // Capture prefix of date right after the comma
                $rowOffset = $rowComma + 27;   // Timestamp width + 1 for comma + 1 for newline
            }

            // -- Cleanup loop for rows after the fence ----
            while ($rowOffset < $chunkEnd) {
                $rowComma = strpos($chunk, ",", $rowOffset + 29); // MinUrlSlug (4) + Domain Prefix (25)
                if ($rowComma === false || $rowComma + 26 > $chunkEnd) break;
                $buckets[$urlTokens[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)]]  // Start after Domain/blog and capture from blog/ to comma
                    .= $dateChars[substr($chunk, $rowComma + 1,10)];    // Capture prefix of date right after the comma
                $rowOffset = $rowComma + 27;   // Timestamp width + 1 for comma + 1 for newline
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

        if ($outPath !== null) {
            file_put_contents($outPath, pack('C*', ...$counts));

            // Ping parent that we're done
            if ($controlSocket !== null) {
                fwrite($controlSocket, "\x01");
            }
        }

        return $counts;
    }
}