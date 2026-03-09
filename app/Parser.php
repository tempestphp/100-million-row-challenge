<?php

namespace App;

final class Parser
{
    private const int BUFFER_SIZE = 262_144;
    private const int OUTPUT_BUFFER_SIZE = 1_048_576;

    private static array $keepAlive = [];

    public final static function parse(string $inputPath, string $outputPath): void
    {
        \gc_disable();
        \ini_set('memory_limit', '-1');
        \set_time_limit(0);

        $dateIds = []; self::$keepAlive[] = &$dateIds;
        $id = 0;
        for($year = 1; $year <= 6; $year++) {
            for($month = 1; $month <= 12; $month++) {
                $yearMonthS = $year.'-'.($month < 10 ? '0'.$month : (string) $month);

                $dateIds[$yearMonthS.'-01'] = $id++;
                $dateIds[$yearMonthS.'-02'] = $id++;
                $dateIds[$yearMonthS.'-03'] = $id++;
                $dateIds[$yearMonthS.'-04'] = $id++;
                $dateIds[$yearMonthS.'-05'] = $id++;
                $dateIds[$yearMonthS.'-06'] = $id++;
                $dateIds[$yearMonthS.'-07'] = $id++;
                $dateIds[$yearMonthS.'-08'] = $id++;
                $dateIds[$yearMonthS.'-09'] = $id++;
                $dateIds[$yearMonthS.'-10'] = $id++;
                $dateIds[$yearMonthS.'-11'] = $id++;
                $dateIds[$yearMonthS.'-12'] = $id++;
                $dateIds[$yearMonthS.'-13'] = $id++;
                $dateIds[$yearMonthS.'-14'] = $id++;
                $dateIds[$yearMonthS.'-15'] = $id++;
                $dateIds[$yearMonthS.'-16'] = $id++;
                $dateIds[$yearMonthS.'-17'] = $id++;
                $dateIds[$yearMonthS.'-18'] = $id++;
                $dateIds[$yearMonthS.'-19'] = $id++;
                $dateIds[$yearMonthS.'-20'] = $id++;
                $dateIds[$yearMonthS.'-21'] = $id++;
                $dateIds[$yearMonthS.'-22'] = $id++;
                $dateIds[$yearMonthS.'-23'] = $id++;
                $dateIds[$yearMonthS.'-24'] = $id++;
                $dateIds[$yearMonthS.'-25'] = $id++;
                $dateIds[$yearMonthS.'-26'] = $id++;
                $dateIds[$yearMonthS.'-27'] = $id++;
                $dateIds[$yearMonthS.'-28'] = $id++;

                if (2 === $month && 4 !== $year) {
                    $id += 3;
                    continue;
                }

                $dateIds[$yearMonthS.'-29'] = $id++;
                if (2 === $month) {
                    $id += 2;
                    continue;
                }

                $dateIds[$yearMonthS.'-30'] = $id++;
                switch($month) {
                    case 4:
                    case 6:
                    case 9:
                    case 11:
                        ++$id;
                        continue 2;
                }

                $dateIds[$yearMonthS.'-31'] = $id++;
            }
        }

        $partialIds = []; self::$keepAlive[] = &$partialIds;
        $uriIds = []; self::$keepAlive[] = &$uriIds;
        $sequence = []; self::$keepAlive[] = &$sequence;
        $maxPartialLen = 0;
        $id = 0;
        foreach(\App\Commands\Visit::all() as $visit) {
            $partial = \substr($visit->uri, 25);

            $partialIds[$partial] = $id;

            if (\strlen($partial) > $maxPartialLen) $maxPartialLen = \strlen($partial);

            $sequence[$id] = null;
            $uriIds[$id] = $partial;

            $id += 37200;
        }
        $counts = \array_fill(0, \count($partialIds) * 37200, 0); self::$keepAlive[] = &$counts;

        $fo = \fopen($outputPath, 'wb');
        if ($fo === false) throw new \Exception('Output file could not be created: '.$outputPath);
        \stream_set_write_buffer($fo, self::OUTPUT_BUFFER_SIZE);
        self::$keepAlive[] = &$fo;

        $f = \fopen($inputPath, 'rb');
        if (false === $f) throw new \Exception('Input file could not be opened: '.$inputPath);
        \stream_set_read_buffer($f, 0);
        \stream_set_chunk_size($f, self::BUFFER_SIZE);
        self::$keepAlive[] = &$f;

        $trailingBufferLen = 65*(52+$maxPartialLen);

        $b = \fread($f, self::BUFFER_SIZE);
        $bp = 25;

        $sequenceRem = \count($sequence);
        $sequenceId = 0;

        if ("\n" === $b[\strlen($b) - 1]) goto s2;

s1:
        $bm = \strlen($b) - $trailingBufferLen;
        while ($bp<$bm) {
            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1; }
        }

s1e:
        $bm = \strrpos($b, "\n");
        while($bp < $bm) {
            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l1e; }
        }

        $bRem = \substr($b, $bp-25);
        $b = \fread($f, self::BUFFER_SIZE);
        $bp = \strpos($b, "\n", 0);
        $bRem .= \substr($b, 0, $bp);
        $bp += 26;

        ++$counts[($id = $partialIds[\substr($bRem, 25, \strlen($bRem)-51)]) + $dateIds[\substr($bRem, \strlen($bRem)-22, 7)]];
        if (!isset($sequence[$id])) {
            $sequence[$id] = $sequenceId++;
            if (!(--$sequenceRem)) {
                if ($bp >= \strlen($b)) goto o0;
                if ("\n" === $b[\strlen($b)-1]) goto l2;
                goto l1;
            }
        }

        if ($bp >= \strlen($b)) goto o0;
        if ("\n" === $b[\strlen($b)-1]) goto s2;
        goto s1;

s2:
        $bm = \strlen($b) - $trailingBufferLen;
        while ($bp<$bm) {
            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }

            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2; }
        }

s2e:
        $bm = \strlen($b);
        while($bp < $bm) {
            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) { $sequence[$id] = $sequenceId++; if (!(--$sequenceRem)) goto l2e; }
        }

        $b = \fread($f, self::BUFFER_SIZE);
        if (0 === \strlen($b)) goto o0;

        $bp = 25;
        if ("\n" === $b[\strlen($b)-1]) goto s2;
        goto s1;

l1:
        $bm = \strlen($b) - $trailingBufferLen;
        while ($bp<$bm) {
            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
        }

l1e:
        $bm = \strrpos($b, "\n");
        while($bp < $bm) {
            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
        }

        $bRem = \substr($b, $bp-25);
        $b = \fread($f, self::BUFFER_SIZE);
        $bp = \strpos($b, "\n", 0);
        $bRem .= \substr($b, 0, $bp);
        $bp += 26;

        ++$counts[($id = $partialIds[\substr($bRem, 25, \strlen($bRem)-51)]) + $dateIds[\substr($bRem, \strlen($bRem)-22, 7)]];

        if ($bp >= \strlen($b)) goto o0;
        if ("\n" === $b[\strlen($b)-1]) goto l2;
        goto l1;

l2:
        $bm = \strlen($b) - $trailingBufferLen;
        while ($bp<$bm) {
            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;

            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
        }

l2e:
        $bm = \strlen($b);
        while($bp < $bm) {
            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
        }

        $b = \fread($f, self::BUFFER_SIZE);
        if (0 === \strlen($b)) goto o0;

        $bp = 25;
        if ("\n" === $b[\strlen($b)-1]) goto l2;
        goto l1;

o0:
        \asort($sequence, \SORT_NUMERIC);

        \fwrite($fo, "{\n");

        $fu = true;
        foreach($sequence as $partialId => $sequenceNo) {
            if (!isset($sequenceNo)) continue;

            $partial = $uriIds[$partialId];
            if (false === $fu)
                $j = ",\n".'    "\\/blog\\/'.$partial."\": {\n";
            else
                $j = '    "\\/blog\\/'.$partial."\": {\n";

            $fy = true;
            $year = 2021;
            while($year <= 2026) {
                $id = $partialId + ((($year-21) % 100) * 372);

                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-01-01": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-01-02": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-01-03": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-01-04": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-01-05": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-01-06": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-01-07": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-01-08": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-01-09": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-01-10": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-01-11": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-01-12": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-01-13": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-01-14": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-01-15": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-01-16": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-01-17": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-01-18": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-01-19": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-01-20": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-01-21": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-01-22": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-01-23": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-01-24": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-01-25": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-01-26": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-01-27": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-01-28": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-01-29": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-01-30": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-01-31": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-02-01": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-02-02": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-02-03": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-02-04": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-02-05": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-02-06": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-02-07": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-02-08": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-02-09": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-02-10": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-02-11": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-02-12": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-02-13": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-02-14": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-02-15": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-02-16": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-02-17": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-02-18": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-02-19": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-02-20": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-02-21": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-02-22": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-02-23": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-02-24": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-02-25": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-02-26": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-02-27": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-02-28": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-02-29": '.$counts[$id-1];
                }
                $id += 2;
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-03-01": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-03-02": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-03-03": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-03-04": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-03-05": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-03-06": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-03-07": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-03-08": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-03-09": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-03-10": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-03-11": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-03-12": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-03-13": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-03-14": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-03-15": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-03-16": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-03-17": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-03-18": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-03-19": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-03-20": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-03-21": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-03-22": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-03-23": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-03-24": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-03-25": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-03-26": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-03-27": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-03-28": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-03-29": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-03-30": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-03-31": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-04-01": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-04-02": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-04-03": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-04-04": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-04-05": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-04-06": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-04-07": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-04-08": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-04-09": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-04-10": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-04-11": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-04-12": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-04-13": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-04-14": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-04-15": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-04-16": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-04-17": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-04-18": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-04-19": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-04-20": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-04-21": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-04-22": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-04-23": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-04-24": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-04-25": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-04-26": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-04-27": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-04-28": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-04-29": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-04-30": '.$counts[$id-1];
                }
                ++$id;
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-05-01": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-05-02": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-05-03": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-05-04": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-05-05": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-05-06": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-05-07": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-05-08": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-05-09": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-05-10": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-05-11": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-05-12": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-05-13": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-05-14": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-05-15": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-05-16": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-05-17": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-05-18": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-05-19": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-05-20": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-05-21": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-05-22": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-05-23": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-05-24": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-05-25": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-05-26": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-05-27": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-05-28": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-05-29": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-05-30": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-05-31": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-06-01": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-06-02": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-06-03": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-06-04": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-06-05": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-06-06": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-06-07": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-06-08": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-06-09": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-06-10": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-06-11": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-06-12": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-06-13": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-06-14": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-06-15": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-06-16": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-06-17": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-06-18": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-06-19": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-06-20": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-06-21": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-06-22": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-06-23": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-06-24": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-06-25": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-06-26": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-06-27": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-06-28": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-06-29": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-06-30": '.$counts[$id-1];
                }
                ++$id;
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-07-01": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-07-02": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-07-03": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-07-04": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-07-05": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-07-06": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-07-07": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-07-08": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-07-09": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-07-10": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-07-11": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-07-12": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-07-13": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-07-14": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-07-15": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-07-16": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-07-17": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-07-18": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-07-19": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-07-20": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-07-21": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-07-22": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-07-23": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-07-24": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-07-25": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-07-26": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-07-27": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-07-28": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-07-29": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-07-30": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-07-31": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-08-01": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-08-02": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-08-03": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-08-04": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-08-05": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-08-06": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-08-07": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-08-08": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-08-09": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-08-10": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-08-11": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-08-12": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-08-13": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-08-14": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-08-15": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-08-16": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-08-17": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-08-18": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-08-19": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-08-20": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-08-21": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-08-22": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-08-23": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-08-24": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-08-25": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-08-26": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-08-27": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-08-28": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-08-29": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-08-30": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-08-31": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-09-01": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-09-02": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-09-03": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-09-04": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-09-05": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-09-06": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-09-07": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-09-08": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-09-09": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-09-10": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-09-11": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-09-12": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-09-13": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-09-14": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-09-15": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-09-16": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-09-17": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-09-18": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-09-19": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-09-20": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-09-21": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-09-22": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-09-23": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-09-24": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-09-25": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-09-26": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-09-27": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-09-28": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-09-29": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-09-30": '.$counts[$id-1];
                }
                ++$id;
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-10-01": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-10-02": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-10-03": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-10-04": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-10-05": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-10-06": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-10-07": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-10-08": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-10-09": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-10-10": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-10-11": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-10-12": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-10-13": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-10-14": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-10-15": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-10-16": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-10-17": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-10-18": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-10-19": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-10-20": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-10-21": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-10-22": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-10-23": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-10-24": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-10-25": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-10-26": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-10-27": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-10-28": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-10-29": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-10-30": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-10-31": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-11-01": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-11-02": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-11-03": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-11-04": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-11-05": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-11-06": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-11-07": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-11-08": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-11-09": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-11-10": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-11-11": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-11-12": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-11-13": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-11-14": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-11-15": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-11-16": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-11-17": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-11-18": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-11-19": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-11-20": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-11-21": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-11-22": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-11-23": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-11-24": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-11-25": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-11-26": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-11-27": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-11-28": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-11-29": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-11-30": '.$counts[$id-1];
                }
                ++$id;
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-12-01": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-12-02": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-12-03": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-12-04": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-12-05": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-12-06": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-12-07": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-12-08": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-12-09": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-12-10": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-12-11": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-12-12": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-12-13": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-12-14": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-12-15": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-12-16": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-12-17": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-12-18": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-12-19": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-12-20": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-12-21": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-12-22": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-12-23": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-12-24": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-12-25": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-12-26": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-12-27": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-12-28": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-12-29": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-12-30": '.$counts[$id-1];
                }
                if ($counts[$id++]) {
                    if (false === $fy) $j .= ",\n"; else $fy = false;
                    $j .= '        "'.$year.'-12-31": '.$counts[$id-1];
                }

                ++$year;
            }

            if (false === $fy) {
                $j .= "\n    }";
                \fwrite($fo, $j);
                $fu = false;
            }
        }

        \fwrite($fo, "\n}");
        \fclose($fo);
    }
}