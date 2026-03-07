<?php

namespace App\Traits;

trait WorkerLegacyTraitV3 {

    private function work(
        int $start,
        int $end,
        int $index,
        ?string $outPath = null,
        mixed $controlSocket = null
    ): array
    {
        // Hoist these properties to prevent Zend engine hash lookups from $this within the hot path
        $urlTokens  = $this->urlTokensShifted;
        $dateChars  = $this->dateChars;
        $dateTokens = $this->dateTokens;

        $counts = array_fill(0, $this->urlCount * $this->dateCount, 0);

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
            $fence = $chunkEnd - (4 * 99);

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

//                $rowComma = strpos($chunk, ",", $rowOffset + 29); // MinUrlSlug (4) + Domain Prefix (25)
//                $u = $urlTokens[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)];
//                $d = $dateTokens[substr($chunk, $rowComma + 1,10)];
//                echo "#$index URL Token: " . $u . PHP_EOL;
//                echo "#$index Date Token: " . $d . PHP_EOL;
//                exit;

                $rowComma = strpos($chunk, ",", $rowOffset + 29); // MinUrlSlug (4) + Domain Prefix (25)
                $counts[
                    $urlTokens[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)]
                    + $dateTokens[substr($chunk, $rowComma + 1,10)]
                ]++;    // Capture prefix of date right after the comma
                $rowOffset = $rowComma + 27;   // Timestamp width + 1 for comma + 1 for newline

                $rowComma = strpos($chunk, ",", $rowOffset + 29); // MinUrlSlug (4) + Domain Prefix (25)
                $counts[
                    $urlTokens[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)]
                    + $dateTokens[substr($chunk, $rowComma + 1,10)]
                ]++;    // Capture prefix of date right after the comma
                $rowOffset = $rowComma + 27;   // Timestamp width + 1 for comma + 1 for newline

                $rowComma = strpos($chunk, ",", $rowOffset + 29); // MinUrlSlug (4) + Domain Prefix (25)
                $counts[
                    $urlTokens[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)]
                    + $dateTokens[substr($chunk, $rowComma + 1,10)]
                ]++;    // Capture prefix of date right after the comma
                $rowOffset = $rowComma + 27;   // Timestamp width + 1 for comma + 1 for newline

                $rowComma = strpos($chunk, ",", $rowOffset + 29); // MinUrlSlug (4) + Domain Prefix (25)
                $counts[
                    $urlTokens[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)]
                    + $dateTokens[substr($chunk, $rowComma + 1,10)]
                ]++;    // Capture prefix of date right after the comma
                $rowOffset = $rowComma + 27;   // Timestamp width + 1 for comma + 1 for newline



                //
            }

            // -- Cleanup loop for rows after the fence ----
            while ($rowOffset < $chunkEnd) {
                $rowComma = strpos($chunk, ",", $rowOffset + 29); // MinUrlSlug (4) + Domain Prefix (25)
                if ($rowComma === false || $rowComma + 26 > $chunkEnd) break;
                $counts[
                    $urlTokens[substr($chunk, $rowOffset + 25, $rowComma - $rowOffset - 25)]
                    + $dateTokens[substr($chunk, $rowComma + 1,10)]
                ]++;    // Capture prefix of date right after the comma
                $rowOffset = $rowComma + 27;   // Timestamp width + 1 for comma + 1 for newline
            }
        }

        fclose($handle);

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