<?php

namespace App\Traits;

use App\Commands\Visit;

use function chr;

trait SetupTokenizedV1Trait {

    // Date generation configurations
    const int DATE_RANGE_YEARS = 5;
    const int DATE_RANGE_BUFFER_MONTHS = 6;

    /**
     * While there are optimizations to be made here, this code is currently
     * running in around 9ms so there is no point in optimizing it.
     */
    private function setup(): void
    {
        // Hoist any class variables
        $inputPath = $this->inputPath;

        $this->buildDateTokenTable();
        $this->buildUrlPool();
        $this->prescanUrlOrder($inputPath);

        // Since we know the smallest url length and time is fixed width,
        // we can optimize our search for newlines to skip this amount
        // and not waste time searching for newline characters where we
        // know they won't exist.
        $this->minLineLength = $this->minUrlLength + 1 + self::DATE_WIDTH;
    }
    private function buildDateTokenTable(): void
    {
        $totalMonths = self::DATE_RANGE_YEARS * 12 + self::DATE_RANGE_BUFFER_MONTHS;
        $startTs = mktime(
            0, 0, 0,
            (int)date('n') - $totalMonths,
            (int)date('j'),
            (int)date('Y')
        );
        $endTs = mktime(0, 0, 0, (int)date('n'), (int)date('j'), (int)date('Y'));

        $dateId = 0;
        $ts     = $startTs;

        while ($ts <=  $endTs) {
            $dateStr = date('Y-m-d', $ts);
            $packed  = (int)substr($dateStr, 0, 4) * 10000
                + (int)substr($dateStr, 5, 2) * 100
                + (int)substr($dateStr, 8, 2);

            //$this->dateTokens[$packed] = $dateId;
            $this->dateStrings[$dateId] = $dateStr;
            $this->dateStrTokens[$dateStr] = $dateId;
            $this->dateChars[$dateStr] = chr($dateId & 0xFF) . chr($dateId >> 8);

            $dateId++;
            $ts = strtotime('+1 day', $ts);
        }

        $this->dateCount = $dateId;
    }

    /**
     * We can't rely 100% on the Visit::all() as we also need the order the urls show
     * up in the input file.
     */
    private function buildUrlPool(): void
    {
        foreach (Visit::all() as $visit) {
            $url = substr($visit->uri, self::DOMAIN_LENGTH);
            $this->urlPool[$url] = true;

            if (strlen($url) < $this->minUrlLength) {
                $this->minUrlLength = strlen($url);
            }
        }
    }

    private function prescanUrlOrder(string $inputPath): void
    {
        $handle = fopen($inputPath, 'rb', false);
        $buffer = '';
        $urlId = 0;
        $poolSize = count($this->urlPool);

        while (!feof($handle)) {
            $buffer .= fread($handle, self::PRESCAN_BUFFER);
            $lines = explode("\n", $buffer);
            $buffer = array_pop($lines);

            foreach ($lines as $line) {
                if ($line === '') continue;

                $url = $this->extractUrl($line);
                if ($url === '' || !isset($this->urlPool[$url]) || isset($this->urlTokens[$url])) {
                    continue;
                }

                $this->urlTokens[$url] = $urlId;
                $this->urlStrings[$urlId] = $url;
                $urlId++;

                if ($urlId === $poolSize) goto done;
            }
        }

        done:
        fclose($handle);
        $this->urlCount = $urlId;
    }

    private function extractUrl(string $line): string
    {
        $len = strlen($line);
        if ($len < self::DOMAIN_LENGTH + self::DATE_WIDTH + 2) return '';

        return substr(
            $line,
            self::DOMAIN_LENGTH,
            $len - self::DOMAIN_LENGTH - self::DATE_WIDTH - 1
        );
    }
}