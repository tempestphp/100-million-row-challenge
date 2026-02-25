<?php

namespace App;

final class Parser
{
    private const CHUNK_SIZE  = 32 * 1024 * 1024;
    private const NUM_WORKERS = 2;

    public function parse(string $inputPath, string $outputPath): void
    {
        $size   = filesize($inputPath);
        $bounds = $this->splitBoundaries($inputPath, $size);

        $tmp = sys_get_temp_dir() . '/parser_' . getmypid() . '_';
        $id  = 0;

        for ($i = 1; $i < self::NUM_WORKERS; $i++) {
            $pid = pcntl_fork();
            if ($pid === -1) break;
            if ($pid === 0) { $id = $i; break; }
        }

        $visits = $this->processSegment($inputPath, $bounds[$id], $bounds[$id + 1]);

        if ($id !== 0) {
            file_put_contents($tmp . $id, igbinary_serialize($visits));
            exit(0);
        }

        while (pcntl_waitpid(-1, $status) > 0) {}

        for ($i = 1; $i < self::NUM_WORKERS; $i++) {
            $f     = $tmp . $i;
            $child = igbinary_unserialize(file_get_contents($f));
            unlink($f);

            foreach ($child as $url => $dates) {
                if (!isset($visits[$url])) {
                    $visits[$url] = $dates;
                    continue;
                }
                foreach ($dates as $date => $count) {
                    isset($visits[$url][$date])
                        ? ($visits[$url][$date] += $count)
                        : ($visits[$url][$date]  = $count);
                }
            }
        }

        foreach ($visits as &$dates) ksort($dates);
        unset($dates);

        file_put_contents($outputPath, json_encode($visits, JSON_PRETTY_PRINT));
    }

    private function splitBoundaries(string $path, int $size): array
    {
        $fh = fopen($path, 'rb');
        $b  = [0];

        for ($i = 1; $i < self::NUM_WORKERS; $i++) {
            fseek($fh, (int)($size * $i / self::NUM_WORKERS));
            fgets($fh);
            $b[] = ftell($fh);
        }

        $b[] = $size;
        fclose($fh);
        return $b;
    }

    private function processSegment(string $path, int $start, int $end): array
    {
        $fh = fopen($path, 'rb');
        fseek($fh, $start);

        $visits = [];
        $carry  = '';
        $left   = $end - $start;

        while ($left > 0) {
            $raw = fread($fh, min(self::CHUNK_SIZE, $left));
            if ($raw === false || $raw === '') break;
            $left -= strlen($raw);

            $buf = $carry . $raw;

            if (($nl = strrpos($buf, "\n")) === false) { $carry = $buf; continue; }

            $carry  = substr($buf, $nl + 1);
            $offset = 0;

            while (($nl = strpos($buf, "\n", $offset)) !== false) {
                $url  = substr($buf, $offset + 19, $nl - $offset - 45);
                $date = substr($buf, $nl - 25, 10);
                isset($visits[$url][$date]) ? $visits[$url][$date]++ : $visits[$url][$date] = 1;
                $offset = $nl + 1;
            }
        }

        if ($carry !== '' && ($len = strlen($carry)) >= 45) {
            $url  = substr($carry, 19, $len - 45);
            $date = substr($carry, $len - 25, 10);
            isset($visits[$url][$date]) ? $visits[$url][$date]++ : $visits[$url][$date] = 1;
        }

        fclose($fh);
        return $visits;
    }
}
