<?php

namespace App;

use App\Commands\Visit;

final class Parser
{
    private const PREFIX_LEN = 25;
    private const DAY_SECONDS = 86400;
    private const WINDOW_DAYS = 1825; // 365*5 (matches generator)
    private const CHUNK = 8_388_608; // 8MB
//    private const CHUNK = 64_000_000; // 64MB
    private const OUT_FLUSH = 1_048_576; // 1MB

    /** @var array<string,int> slug => id */
    private array $slugToId = [];

    /** @var array<int,string> id => slug */
    private array $idToSlug = [];

    /** @var array<int,int> YYYYMMDD => dayIndex */
    private array $dateToIndex = [];

    /** @var array<int,string> dayIndex => YYYY-MM-DD */
    private array $indexToDate = [];

    private int $days = 0;

    public function parse(string $inputPath, string $outputPath, int $seed = 1772177204): void
    {
        \gc_disable();

        $this->initSlugs();
        $this->initDatesFromSeed($seed);

        $slugCount = \count($this->idToSlug);
        $counts = \array_fill(0, $slugCount * $this->days, 0);

        $h = \fopen($inputPath, 'rb');
        if ($h === false) {
            throw new \RuntimeException("Failed to read input file: $inputPath");
        }

        $leftover = '';

        while (!\feof($h)) {
            $raw = \fread($h, self::CHUNK);
            if ($raw === '' || $raw === false) {
                break;
            }

            // avoid concat if no leftover
            $chunk = ($leftover !== '') ? ($leftover . $raw) : $raw;
            $leftover = '';

            $pos = 0;
            $len = \strlen($chunk);

            while (true) {
                // minimal bytes for: prefix + "," + "YYYY-MM-DD" + "\n"
                if ($pos + self::PREFIX_LEN + 1 + 10 + 1 > $len) {
                    $leftover = \substr($chunk, $pos);
                    break;
                }

                // comma after prefix
                $comma = \strpos($chunk, ',', $pos + self::PREFIX_LEN);
                if ($comma === false) {
                    $leftover = \substr($chunk, $pos);
                    break;
                }

                // newline: fast-path assumes fixed-length timestamp like date('c')
                $nl = $comma + 26;
                if ($nl >= $len || $chunk[$nl] !== "\n") {
                    $nl = \strpos($chunk, "\n", $comma);
                    if ($nl === false) {
                        $leftover = \substr($chunk, $pos);
                        break;
                    }
                }

                // slug substring (yes, per line). Still fastest overall without hashing.
                $slugStart = $pos + self::PREFIX_LEN;
                $slugLen   = $comma - $slugStart;

                // empty slug guard
                if ($slugLen > 0) {
                    $slug = \substr($chunk, $slugStart, $slugLen);
                    $id = $this->slugToId[$slug] ?? -1;

                    if ($id !== -1) {
                        // parse YYYY-MM-DD into YYYYMMDD without allocations
                        // positions: comma+1..4 year, comma+6..7 month, comma+9..10 day
                        $y =
                            (\ord($chunk[$comma + 1]) - 48) * 1000 +
                            (\ord($chunk[$comma + 2]) - 48) * 100 +
                            (\ord($chunk[$comma + 3]) - 48) * 10 +
                            (\ord($chunk[$comma + 4]) - 48);

                        $m =
                            (\ord($chunk[$comma + 6]) - 48) * 10 +
                            (\ord($chunk[$comma + 7]) - 48);

                        $d =
                            (\ord($chunk[$comma + 9]) - 48) * 10 +
                            (\ord($chunk[$comma + 10]) - 48);

                        $dateInt = $y * 10000 + $m * 100 + $d;

                        $di = $this->dateToIndex[$dateInt] ?? -1;
                        if ($di !== -1) {
                            $counts[$id * $this->days + $di]++;
                        }
                    }
                }

                $pos = $nl + 1;
                if ($pos >= $len) {
                    break;
                }
            }
        }

        \fclose($h);

        $this->writeJson($outputPath, $counts, $slugCount);
    }

    private function initSlugs(): void
    {
        $id = 0;

        foreach (Visit::all() as $visit) {
            // generator writes full URI; your parser expects prefix length 25,
            // so slug is uri[25..]
            $slug = \substr($visit->uri, self::PREFIX_LEN);

            // de-dupe, stable ids
            if (!isset($this->slugToId[$slug])) {
                $this->slugToId[$slug] = $id;
                $this->idToSlug[$id] = $slug;
                $id++;
            }
        }
    }

    private function initDatesFromSeed(int $seed): void
    {
        $endTs = ($seed === 0) ? \time() : $seed;
        $startTs = $endTs - (self::WINDOW_DAYS * self::DAY_SECONDS);

        // Build by stepping days in UTC; index is naturally sorted.
        $idx = 0;

        // include both endpoints -> +1 day count
        for ($ts = $startTs; $ts <= $endTs; $ts += self::DAY_SECONDS) {
            $y = (int)\gmdate('Y', $ts);
            $m = (int)\gmdate('m', $ts);
            $d = (int)\gmdate('d', $ts);

            $dateInt = $y * 10000 + $m * 100 + $d;

            // if two timestamps land on same UTC date (DST/oddities shouldn’t in UTC), keep first index
            if (!isset($this->dateToIndex[$dateInt])) {
                $this->dateToIndex[$dateInt] = $idx;
                $this->indexToDate[$idx] = \sprintf('%04d-%02d-%02d', $y, $m, $d);
                $idx++;
            }
        }

        $this->days = $idx;
    }

    private function writeJson(string $outputPath, array $counts, int $slugCount): void
    {
        $out = \fopen($outputPath, 'wb');
        if ($out === false) {
            throw new \RuntimeException("Failed to write output file: $outputPath");
        }

        $buf = "{\n";

        for ($id = 0; $id < $slugCount; $id++) {
            if ($id !== 0) {
                $buf .= ",\n";
            }

            $slug = $this->idToSlug[$id];
            // If slugs are strictly safe, keep as-is. Otherwise uncomment:
            // $slug = \addcslashes($slug, "\\\"\n\r\t");

            $buf .= "    \"\\/blog\\/$slug\": {\n";

            $base = $id * $this->days;
            $first = true;

            for ($di = 0; $di < $this->days; $di++) {
                $c = $counts[$base + $di];
                if ($c === 0) {
                    continue;
                }

                if (!$first) {
                    $buf .= ",\n";
                }
                $first = false;

                $date = $this->indexToDate[$di];
                $buf .= "        \"$date\": $c";

                if (\strlen($buf) >= self::OUT_FLUSH) {
                    \fwrite($out, $buf);
                    $buf = '';
                }
            }

            $buf .= "\n    }";

            if (\strlen($buf) >= self::OUT_FLUSH) {
                \fwrite($out, $buf);
                $buf = '';
            }
        }

        $buf .= "\n}\n";
        \fwrite($out, $buf);
        \fclose($out);
    }
}