<?php

namespace App;

final class Parser
{
    private const int BUFFER_SIZE = 262144;
    private const int CHUNK_SIZE = 262144;
    private const int OUTPUT_BUFFER_SIZE = 1048576;

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
        $id = 0;
        foreach(\App\Commands\Visit::all() as $visit) {
            $partial = \substr($visit->uri, 25);

            $partialIds[$partial] = $id;

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
        \stream_set_chunk_size($f, self::CHUNK_SIZE);
        self::$keepAlive[] = &$f;

        $b = \fread($f, self::BUFFER_SIZE);
        $bp = 25;

        $sequenceRem = \count($sequence);
        $sequenceId = 0;

s1:
        $bm = \strrpos($b, "\n");
        do {
            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) {
                $sequence[$id] = $sequenceId++;
                if (0 === (--$sequenceRem)) {
                    if ($bp>=$bm) goto l1e;
                    goto l1s;
                }
            }
        } while ($bp<$bm);

s1e:
        if ($bp === (25+\strlen($b))) {
            if (0 === \strlen($b = \fread($f, self::BUFFER_SIZE))) goto o0;
            $bp = 25;
            goto s1;
        }

        $bRem = \substr($b, $bp-25);
        $b = \fread($f, self::BUFFER_SIZE);
        $bp = 26+\strpos($b, "\n", 0);
        $bRem .= \substr($b, 0, $bp-26);

        ++$counts[($id = $partialIds[\substr($bRem, 25, \strlen($bRem)-51)]) + $dateIds[\substr($bRem, \strlen($bRem)-22, 7)]];
        if (!isset($sequence[$id])) {
            $sequence[$id] = $sequenceId++;
            if (0 === (--$sequenceRem)) {
                $bm = \strrpos($b, "\n");
                if ($bp>=$bm) goto l1e;
                goto l1s;
            }
        }

        goto s1;

l1:
        $bm = \strrpos($b, "\n");
l1s:
        do {
            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
        } while ($bp<$bm);

l1e:
        if ($bp === (25+\strlen($b))) {
            if (0 === \strlen($b = \fread($f, self::BUFFER_SIZE))) goto o0;
            $bp = 25;
            goto l1;
        }

        $bRem = \substr($b, $bp-25);
        $b = \fread($f, self::BUFFER_SIZE);
        $bp = 26+\strpos($b, "\n", 0);
        $bRem .= \substr($b, 0, $bp-26);

        ++$counts[$partialIds[\substr($bRem, 25, \strlen($bRem)-51)] + $dateIds[\substr($bRem, \strlen($bRem)-22, 7)]];

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

            $year = 2021;
o1:
            $id = $partialId + ((($year-21) % 100) * 372);

            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-01-01": '.$counts[$id-1];
                goto oy1;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-01-02": '.$counts[$id-1];
                goto oy2;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-01-03": '.$counts[$id-1];
                goto oy3;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-01-04": '.$counts[$id-1];
                goto oy4;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-01-05": '.$counts[$id-1];
                goto oy5;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-01-06": '.$counts[$id-1];
                goto oy6;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-01-07": '.$counts[$id-1];
                goto oy7;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-01-08": '.$counts[$id-1];
                goto oy8;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-01-09": '.$counts[$id-1];
                goto oy9;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-01-10": '.$counts[$id-1];
                goto oy10;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-01-11": '.$counts[$id-1];
                goto oy11;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-01-12": '.$counts[$id-1];
                goto oy12;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-01-13": '.$counts[$id-1];
                goto oy13;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-01-14": '.$counts[$id-1];
                goto oy14;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-01-15": '.$counts[$id-1];
                goto oy15;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-01-16": '.$counts[$id-1];
                goto oy16;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-01-17": '.$counts[$id-1];
                goto oy17;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-01-18": '.$counts[$id-1];
                goto oy18;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-01-19": '.$counts[$id-1];
                goto oy19;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-01-20": '.$counts[$id-1];
                goto oy20;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-01-21": '.$counts[$id-1];
                goto oy21;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-01-22": '.$counts[$id-1];
                goto oy22;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-01-23": '.$counts[$id-1];
                goto oy23;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-01-24": '.$counts[$id-1];
                goto oy24;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-01-25": '.$counts[$id-1];
                goto oy25;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-01-26": '.$counts[$id-1];
                goto oy26;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-01-27": '.$counts[$id-1];
                goto oy27;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-01-28": '.$counts[$id-1];
                goto oy28;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-01-29": '.$counts[$id-1];
                goto oy29;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-01-30": '.$counts[$id-1];
                goto oy30;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-01-31": '.$counts[$id-1];
                goto oy31;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-02-01": '.$counts[$id-1];
                goto oy32;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-02-02": '.$counts[$id-1];
                goto oy33;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-02-03": '.$counts[$id-1];
                goto oy34;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-02-04": '.$counts[$id-1];
                goto oy35;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-02-05": '.$counts[$id-1];
                goto oy36;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-02-06": '.$counts[$id-1];
                goto oy37;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-02-07": '.$counts[$id-1];
                goto oy38;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-02-08": '.$counts[$id-1];
                goto oy39;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-02-09": '.$counts[$id-1];
                goto oy40;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-02-10": '.$counts[$id-1];
                goto oy41;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-02-11": '.$counts[$id-1];
                goto oy42;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-02-12": '.$counts[$id-1];
                goto oy43;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-02-13": '.$counts[$id-1];
                goto oy44;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-02-14": '.$counts[$id-1];
                goto oy45;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-02-15": '.$counts[$id-1];
                goto oy46;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-02-16": '.$counts[$id-1];
                goto oy47;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-02-17": '.$counts[$id-1];
                goto oy48;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-02-18": '.$counts[$id-1];
                goto oy49;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-02-19": '.$counts[$id-1];
                goto oy50;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-02-20": '.$counts[$id-1];
                goto oy51;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-02-21": '.$counts[$id-1];
                goto oy52;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-02-22": '.$counts[$id-1];
                goto oy53;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-02-23": '.$counts[$id-1];
                goto oy54;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-02-24": '.$counts[$id-1];
                goto oy55;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-02-25": '.$counts[$id-1];
                goto oy56;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-02-26": '.$counts[$id-1];
                goto oy57;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-02-27": '.$counts[$id-1];
                goto oy58;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-02-28": '.$counts[$id-1];
                goto oy59;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-02-29": '.$counts[$id-1];
                goto oy60;
            }
            $id += 2;
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-03-01": '.$counts[$id-1];
                goto oy63;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-03-02": '.$counts[$id-1];
                goto oy64;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-03-03": '.$counts[$id-1];
                goto oy65;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-03-04": '.$counts[$id-1];
                goto oy66;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-03-05": '.$counts[$id-1];
                goto oy67;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-03-06": '.$counts[$id-1];
                goto oy68;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-03-07": '.$counts[$id-1];
                goto oy69;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-03-08": '.$counts[$id-1];
                goto oy70;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-03-09": '.$counts[$id-1];
                goto oy71;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-03-10": '.$counts[$id-1];
                goto oy72;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-03-11": '.$counts[$id-1];
                goto oy73;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-03-12": '.$counts[$id-1];
                goto oy74;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-03-13": '.$counts[$id-1];
                goto oy75;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-03-14": '.$counts[$id-1];
                goto oy76;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-03-15": '.$counts[$id-1];
                goto oy77;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-03-16": '.$counts[$id-1];
                goto oy78;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-03-17": '.$counts[$id-1];
                goto oy79;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-03-18": '.$counts[$id-1];
                goto oy80;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-03-19": '.$counts[$id-1];
                goto oy81;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-03-20": '.$counts[$id-1];
                goto oy82;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-03-21": '.$counts[$id-1];
                goto oy83;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-03-22": '.$counts[$id-1];
                goto oy84;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-03-23": '.$counts[$id-1];
                goto oy85;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-03-24": '.$counts[$id-1];
                goto oy86;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-03-25": '.$counts[$id-1];
                goto oy87;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-03-26": '.$counts[$id-1];
                goto oy88;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-03-27": '.$counts[$id-1];
                goto oy89;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-03-28": '.$counts[$id-1];
                goto oy90;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-03-29": '.$counts[$id-1];
                goto oy91;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-03-30": '.$counts[$id-1];
                goto oy92;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-03-31": '.$counts[$id-1];
                goto oy93;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-04-01": '.$counts[$id-1];
                goto oy94;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-04-02": '.$counts[$id-1];
                goto oy95;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-04-03": '.$counts[$id-1];
                goto oy96;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-04-04": '.$counts[$id-1];
                goto oy97;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-04-05": '.$counts[$id-1];
                goto oy98;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-04-06": '.$counts[$id-1];
                goto oy99;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-04-07": '.$counts[$id-1];
                goto oy100;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-04-08": '.$counts[$id-1];
                goto oy101;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-04-09": '.$counts[$id-1];
                goto oy102;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-04-10": '.$counts[$id-1];
                goto oy103;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-04-11": '.$counts[$id-1];
                goto oy104;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-04-12": '.$counts[$id-1];
                goto oy105;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-04-13": '.$counts[$id-1];
                goto oy106;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-04-14": '.$counts[$id-1];
                goto oy107;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-04-15": '.$counts[$id-1];
                goto oy108;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-04-16": '.$counts[$id-1];
                goto oy109;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-04-17": '.$counts[$id-1];
                goto oy110;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-04-18": '.$counts[$id-1];
                goto oy111;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-04-19": '.$counts[$id-1];
                goto oy112;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-04-20": '.$counts[$id-1];
                goto oy113;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-04-21": '.$counts[$id-1];
                goto oy114;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-04-22": '.$counts[$id-1];
                goto oy115;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-04-23": '.$counts[$id-1];
                goto oy116;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-04-24": '.$counts[$id-1];
                goto oy117;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-04-25": '.$counts[$id-1];
                goto oy118;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-04-26": '.$counts[$id-1];
                goto oy119;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-04-27": '.$counts[$id-1];
                goto oy120;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-04-28": '.$counts[$id-1];
                goto oy121;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-04-29": '.$counts[$id-1];
                goto oy122;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-04-30": '.$counts[$id-1];
                goto oy123;
            }
            ++$id;
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-05-01": '.$counts[$id-1];
                goto oy125;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-05-02": '.$counts[$id-1];
                goto oy126;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-05-03": '.$counts[$id-1];
                goto oy127;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-05-04": '.$counts[$id-1];
                goto oy128;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-05-05": '.$counts[$id-1];
                goto oy129;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-05-06": '.$counts[$id-1];
                goto oy130;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-05-07": '.$counts[$id-1];
                goto oy131;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-05-08": '.$counts[$id-1];
                goto oy132;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-05-09": '.$counts[$id-1];
                goto oy133;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-05-10": '.$counts[$id-1];
                goto oy134;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-05-11": '.$counts[$id-1];
                goto oy135;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-05-12": '.$counts[$id-1];
                goto oy136;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-05-13": '.$counts[$id-1];
                goto oy137;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-05-14": '.$counts[$id-1];
                goto oy138;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-05-15": '.$counts[$id-1];
                goto oy139;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-05-16": '.$counts[$id-1];
                goto oy140;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-05-17": '.$counts[$id-1];
                goto oy141;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-05-18": '.$counts[$id-1];
                goto oy142;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-05-19": '.$counts[$id-1];
                goto oy143;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-05-20": '.$counts[$id-1];
                goto oy144;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-05-21": '.$counts[$id-1];
                goto oy145;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-05-22": '.$counts[$id-1];
                goto oy146;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-05-23": '.$counts[$id-1];
                goto oy147;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-05-24": '.$counts[$id-1];
                goto oy148;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-05-25": '.$counts[$id-1];
                goto oy149;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-05-26": '.$counts[$id-1];
                goto oy150;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-05-27": '.$counts[$id-1];
                goto oy151;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-05-28": '.$counts[$id-1];
                goto oy152;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-05-29": '.$counts[$id-1];
                goto oy153;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-05-30": '.$counts[$id-1];
                goto oy154;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-05-31": '.$counts[$id-1];
                goto oy155;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-06-01": '.$counts[$id-1];
                goto oy156;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-06-02": '.$counts[$id-1];
                goto oy157;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-06-03": '.$counts[$id-1];
                goto oy158;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-06-04": '.$counts[$id-1];
                goto oy159;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-06-05": '.$counts[$id-1];
                goto oy160;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-06-06": '.$counts[$id-1];
                goto oy161;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-06-07": '.$counts[$id-1];
                goto oy162;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-06-08": '.$counts[$id-1];
                goto oy163;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-06-09": '.$counts[$id-1];
                goto oy164;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-06-10": '.$counts[$id-1];
                goto oy165;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-06-11": '.$counts[$id-1];
                goto oy166;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-06-12": '.$counts[$id-1];
                goto oy167;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-06-13": '.$counts[$id-1];
                goto oy168;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-06-14": '.$counts[$id-1];
                goto oy169;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-06-15": '.$counts[$id-1];
                goto oy170;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-06-16": '.$counts[$id-1];
                goto oy171;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-06-17": '.$counts[$id-1];
                goto oy172;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-06-18": '.$counts[$id-1];
                goto oy173;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-06-19": '.$counts[$id-1];
                goto oy174;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-06-20": '.$counts[$id-1];
                goto oy175;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-06-21": '.$counts[$id-1];
                goto oy176;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-06-22": '.$counts[$id-1];
                goto oy177;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-06-23": '.$counts[$id-1];
                goto oy178;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-06-24": '.$counts[$id-1];
                goto oy179;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-06-25": '.$counts[$id-1];
                goto oy180;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-06-26": '.$counts[$id-1];
                goto oy181;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-06-27": '.$counts[$id-1];
                goto oy182;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-06-28": '.$counts[$id-1];
                goto oy183;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-06-29": '.$counts[$id-1];
                goto oy184;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-06-30": '.$counts[$id-1];
                goto oy185;
            }
            ++$id;
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-07-01": '.$counts[$id-1];
                goto oy187;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-07-02": '.$counts[$id-1];
                goto oy188;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-07-03": '.$counts[$id-1];
                goto oy189;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-07-04": '.$counts[$id-1];
                goto oy190;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-07-05": '.$counts[$id-1];
                goto oy191;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-07-06": '.$counts[$id-1];
                goto oy192;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-07-07": '.$counts[$id-1];
                goto oy193;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-07-08": '.$counts[$id-1];
                goto oy194;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-07-09": '.$counts[$id-1];
                goto oy195;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-07-10": '.$counts[$id-1];
                goto oy196;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-07-11": '.$counts[$id-1];
                goto oy197;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-07-12": '.$counts[$id-1];
                goto oy198;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-07-13": '.$counts[$id-1];
                goto oy199;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-07-14": '.$counts[$id-1];
                goto oy200;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-07-15": '.$counts[$id-1];
                goto oy201;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-07-16": '.$counts[$id-1];
                goto oy202;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-07-17": '.$counts[$id-1];
                goto oy203;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-07-18": '.$counts[$id-1];
                goto oy204;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-07-19": '.$counts[$id-1];
                goto oy205;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-07-20": '.$counts[$id-1];
                goto oy206;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-07-21": '.$counts[$id-1];
                goto oy207;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-07-22": '.$counts[$id-1];
                goto oy208;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-07-23": '.$counts[$id-1];
                goto oy209;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-07-24": '.$counts[$id-1];
                goto oy210;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-07-25": '.$counts[$id-1];
                goto oy211;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-07-26": '.$counts[$id-1];
                goto oy212;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-07-27": '.$counts[$id-1];
                goto oy213;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-07-28": '.$counts[$id-1];
                goto oy214;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-07-29": '.$counts[$id-1];
                goto oy215;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-07-30": '.$counts[$id-1];
                goto oy216;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-07-31": '.$counts[$id-1];
                goto oy217;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-08-01": '.$counts[$id-1];
                goto oy218;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-08-02": '.$counts[$id-1];
                goto oy219;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-08-03": '.$counts[$id-1];
                goto oy220;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-08-04": '.$counts[$id-1];
                goto oy221;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-08-05": '.$counts[$id-1];
                goto oy222;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-08-06": '.$counts[$id-1];
                goto oy223;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-08-07": '.$counts[$id-1];
                goto oy224;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-08-08": '.$counts[$id-1];
                goto oy225;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-08-09": '.$counts[$id-1];
                goto oy226;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-08-10": '.$counts[$id-1];
                goto oy227;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-08-11": '.$counts[$id-1];
                goto oy228;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-08-12": '.$counts[$id-1];
                goto oy229;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-08-13": '.$counts[$id-1];
                goto oy230;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-08-14": '.$counts[$id-1];
                goto oy231;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-08-15": '.$counts[$id-1];
                goto oy232;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-08-16": '.$counts[$id-1];
                goto oy233;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-08-17": '.$counts[$id-1];
                goto oy234;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-08-18": '.$counts[$id-1];
                goto oy235;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-08-19": '.$counts[$id-1];
                goto oy236;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-08-20": '.$counts[$id-1];
                goto oy237;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-08-21": '.$counts[$id-1];
                goto oy238;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-08-22": '.$counts[$id-1];
                goto oy239;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-08-23": '.$counts[$id-1];
                goto oy240;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-08-24": '.$counts[$id-1];
                goto oy241;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-08-25": '.$counts[$id-1];
                goto oy242;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-08-26": '.$counts[$id-1];
                goto oy243;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-08-27": '.$counts[$id-1];
                goto oy244;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-08-28": '.$counts[$id-1];
                goto oy245;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-08-29": '.$counts[$id-1];
                goto oy246;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-08-30": '.$counts[$id-1];
                goto oy247;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-08-31": '.$counts[$id-1];
                goto oy248;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-09-01": '.$counts[$id-1];
                goto oy249;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-09-02": '.$counts[$id-1];
                goto oy250;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-09-03": '.$counts[$id-1];
                goto oy251;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-09-04": '.$counts[$id-1];
                goto oy252;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-09-05": '.$counts[$id-1];
                goto oy253;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-09-06": '.$counts[$id-1];
                goto oy254;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-09-07": '.$counts[$id-1];
                goto oy255;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-09-08": '.$counts[$id-1];
                goto oy256;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-09-09": '.$counts[$id-1];
                goto oy257;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-09-10": '.$counts[$id-1];
                goto oy258;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-09-11": '.$counts[$id-1];
                goto oy259;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-09-12": '.$counts[$id-1];
                goto oy260;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-09-13": '.$counts[$id-1];
                goto oy261;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-09-14": '.$counts[$id-1];
                goto oy262;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-09-15": '.$counts[$id-1];
                goto oy263;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-09-16": '.$counts[$id-1];
                goto oy264;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-09-17": '.$counts[$id-1];
                goto oy265;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-09-18": '.$counts[$id-1];
                goto oy266;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-09-19": '.$counts[$id-1];
                goto oy267;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-09-20": '.$counts[$id-1];
                goto oy268;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-09-21": '.$counts[$id-1];
                goto oy269;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-09-22": '.$counts[$id-1];
                goto oy270;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-09-23": '.$counts[$id-1];
                goto oy271;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-09-24": '.$counts[$id-1];
                goto oy272;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-09-25": '.$counts[$id-1];
                goto oy273;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-09-26": '.$counts[$id-1];
                goto oy274;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-09-27": '.$counts[$id-1];
                goto oy275;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-09-28": '.$counts[$id-1];
                goto oy276;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-09-29": '.$counts[$id-1];
                goto oy277;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-09-30": '.$counts[$id-1];
                goto oy278;
            }
            ++$id;
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-10-01": '.$counts[$id-1];
                goto oy280;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-10-02": '.$counts[$id-1];
                goto oy281;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-10-03": '.$counts[$id-1];
                goto oy282;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-10-04": '.$counts[$id-1];
                goto oy283;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-10-05": '.$counts[$id-1];
                goto oy284;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-10-06": '.$counts[$id-1];
                goto oy285;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-10-07": '.$counts[$id-1];
                goto oy286;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-10-08": '.$counts[$id-1];
                goto oy287;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-10-09": '.$counts[$id-1];
                goto oy288;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-10-10": '.$counts[$id-1];
                goto oy289;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-10-11": '.$counts[$id-1];
                goto oy290;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-10-12": '.$counts[$id-1];
                goto oy291;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-10-13": '.$counts[$id-1];
                goto oy292;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-10-14": '.$counts[$id-1];
                goto oy293;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-10-15": '.$counts[$id-1];
                goto oy294;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-10-16": '.$counts[$id-1];
                goto oy295;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-10-17": '.$counts[$id-1];
                goto oy296;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-10-18": '.$counts[$id-1];
                goto oy297;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-10-19": '.$counts[$id-1];
                goto oy298;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-10-20": '.$counts[$id-1];
                goto oy299;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-10-21": '.$counts[$id-1];
                goto oy300;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-10-22": '.$counts[$id-1];
                goto oy301;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-10-23": '.$counts[$id-1];
                goto oy302;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-10-24": '.$counts[$id-1];
                goto oy303;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-10-25": '.$counts[$id-1];
                goto oy304;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-10-26": '.$counts[$id-1];
                goto oy305;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-10-27": '.$counts[$id-1];
                goto oy306;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-10-28": '.$counts[$id-1];
                goto oy307;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-10-29": '.$counts[$id-1];
                goto oy308;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-10-30": '.$counts[$id-1];
                goto oy309;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-10-31": '.$counts[$id-1];
                goto oy310;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-11-01": '.$counts[$id-1];
                goto oy311;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-11-02": '.$counts[$id-1];
                goto oy312;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-11-03": '.$counts[$id-1];
                goto oy313;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-11-04": '.$counts[$id-1];
                goto oy314;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-11-05": '.$counts[$id-1];
                goto oy315;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-11-06": '.$counts[$id-1];
                goto oy316;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-11-07": '.$counts[$id-1];
                goto oy317;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-11-08": '.$counts[$id-1];
                goto oy318;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-11-09": '.$counts[$id-1];
                goto oy319;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-11-10": '.$counts[$id-1];
                goto oy320;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-11-11": '.$counts[$id-1];
                goto oy321;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-11-12": '.$counts[$id-1];
                goto oy322;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-11-13": '.$counts[$id-1];
                goto oy323;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-11-14": '.$counts[$id-1];
                goto oy324;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-11-15": '.$counts[$id-1];
                goto oy325;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-11-16": '.$counts[$id-1];
                goto oy326;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-11-17": '.$counts[$id-1];
                goto oy327;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-11-18": '.$counts[$id-1];
                goto oy328;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-11-19": '.$counts[$id-1];
                goto oy329;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-11-20": '.$counts[$id-1];
                goto oy330;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-11-21": '.$counts[$id-1];
                goto oy331;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-11-22": '.$counts[$id-1];
                goto oy332;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-11-23": '.$counts[$id-1];
                goto oy333;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-11-24": '.$counts[$id-1];
                goto oy334;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-11-25": '.$counts[$id-1];
                goto oy335;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-11-26": '.$counts[$id-1];
                goto oy336;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-11-27": '.$counts[$id-1];
                goto oy337;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-11-28": '.$counts[$id-1];
                goto oy338;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-11-29": '.$counts[$id-1];
                goto oy339;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-11-30": '.$counts[$id-1];
                goto oy340;
            }
            ++$id;
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-12-01": '.$counts[$id-1];
                goto oy342;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-12-02": '.$counts[$id-1];
                goto oy343;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-12-03": '.$counts[$id-1];
                goto oy344;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-12-04": '.$counts[$id-1];
                goto oy345;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-12-05": '.$counts[$id-1];
                goto oy346;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-12-06": '.$counts[$id-1];
                goto oy347;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-12-07": '.$counts[$id-1];
                goto oy348;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-12-08": '.$counts[$id-1];
                goto oy349;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-12-09": '.$counts[$id-1];
                goto oy350;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-12-10": '.$counts[$id-1];
                goto oy351;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-12-11": '.$counts[$id-1];
                goto oy352;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-12-12": '.$counts[$id-1];
                goto oy353;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-12-13": '.$counts[$id-1];
                goto oy354;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-12-14": '.$counts[$id-1];
                goto oy355;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-12-15": '.$counts[$id-1];
                goto oy356;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-12-16": '.$counts[$id-1];
                goto oy357;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-12-17": '.$counts[$id-1];
                goto oy358;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-12-18": '.$counts[$id-1];
                goto oy359;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-12-19": '.$counts[$id-1];
                goto oy360;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-12-20": '.$counts[$id-1];
                goto oy361;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-12-21": '.$counts[$id-1];
                goto oy362;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-12-22": '.$counts[$id-1];
                goto oy363;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-12-23": '.$counts[$id-1];
                goto oy364;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-12-24": '.$counts[$id-1];
                goto oy365;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-12-25": '.$counts[$id-1];
                goto oy366;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-12-26": '.$counts[$id-1];
                goto oy367;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-12-27": '.$counts[$id-1];
                goto oy368;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-12-28": '.$counts[$id-1];
                goto oy369;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-12-29": '.$counts[$id-1];
                goto oy370;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-12-30": '.$counts[$id-1];
                goto oy371;
            }
            if ($counts[$id++] > 0) {
                $j .= '        "'.$year.'-12-31": '.$counts[$id-1];
                goto oy372;
            }

            ++$year;
            if($year <= 2026) goto o1;
            continue;

o2:
            $id = $partialId + ((($year-21) % 100) * 372);

            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-01-01": '.$counts[$id-1];
            }
            oy1:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-01-02": '.$counts[$id-1];
            }
            oy2:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-01-03": '.$counts[$id-1];
            }
            oy3:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-01-04": '.$counts[$id-1];
            }
            oy4:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-01-05": '.$counts[$id-1];
            }
            oy5:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-01-06": '.$counts[$id-1];
            }
            oy6:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-01-07": '.$counts[$id-1];
            }
            oy7:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-01-08": '.$counts[$id-1];
            }
            oy8:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-01-09": '.$counts[$id-1];
            }
            oy9:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-01-10": '.$counts[$id-1];
            }
            oy10:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-01-11": '.$counts[$id-1];
            }
            oy11:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-01-12": '.$counts[$id-1];
            }
            oy12:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-01-13": '.$counts[$id-1];
            }
            oy13:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-01-14": '.$counts[$id-1];
            }
            oy14:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-01-15": '.$counts[$id-1];
            }
            oy15:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-01-16": '.$counts[$id-1];
            }
            oy16:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-01-17": '.$counts[$id-1];
            }
            oy17:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-01-18": '.$counts[$id-1];
            }
            oy18:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-01-19": '.$counts[$id-1];
            }
            oy19:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-01-20": '.$counts[$id-1];
            }
            oy20:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-01-21": '.$counts[$id-1];
            }
            oy21:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-01-22": '.$counts[$id-1];
            }
            oy22:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-01-23": '.$counts[$id-1];
            }
            oy23:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-01-24": '.$counts[$id-1];
            }
            oy24:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-01-25": '.$counts[$id-1];
            }
            oy25:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-01-26": '.$counts[$id-1];
            }
            oy26:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-01-27": '.$counts[$id-1];
            }
            oy27:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-01-28": '.$counts[$id-1];
            }
            oy28:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-01-29": '.$counts[$id-1];
            }
            oy29:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-01-30": '.$counts[$id-1];
            }
            oy30:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-01-31": '.$counts[$id-1];
            }
            oy31:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-02-01": '.$counts[$id-1];
            }
            oy32:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-02-02": '.$counts[$id-1];
            }
            oy33:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-02-03": '.$counts[$id-1];
            }
            oy34:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-02-04": '.$counts[$id-1];
            }
            oy35:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-02-05": '.$counts[$id-1];
            }
            oy36:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-02-06": '.$counts[$id-1];
            }
            oy37:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-02-07": '.$counts[$id-1];
            }
            oy38:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-02-08": '.$counts[$id-1];
            }
            oy39:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-02-09": '.$counts[$id-1];
            }
            oy40:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-02-10": '.$counts[$id-1];
            }
            oy41:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-02-11": '.$counts[$id-1];
            }
            oy42:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-02-12": '.$counts[$id-1];
            }
            oy43:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-02-13": '.$counts[$id-1];
            }
            oy44:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-02-14": '.$counts[$id-1];
            }
            oy45:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-02-15": '.$counts[$id-1];
            }
            oy46:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-02-16": '.$counts[$id-1];
            }
            oy47:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-02-17": '.$counts[$id-1];
            }
            oy48:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-02-18": '.$counts[$id-1];
            }
            oy49:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-02-19": '.$counts[$id-1];
            }
            oy50:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-02-20": '.$counts[$id-1];
            }
            oy51:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-02-21": '.$counts[$id-1];
            }
            oy52:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-02-22": '.$counts[$id-1];
            }
            oy53:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-02-23": '.$counts[$id-1];
            }
            oy54:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-02-24": '.$counts[$id-1];
            }
            oy55:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-02-25": '.$counts[$id-1];
            }
            oy56:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-02-26": '.$counts[$id-1];
            }
            oy57:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-02-27": '.$counts[$id-1];
            }
            oy58:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-02-28": '.$counts[$id-1];
            }
            oy59:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-02-29": '.$counts[$id-1];
            }
            oy60:
            $id += 2;
            oy62:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-03-01": '.$counts[$id-1];
            }
            oy63:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-03-02": '.$counts[$id-1];
            }
            oy64:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-03-03": '.$counts[$id-1];
            }
            oy65:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-03-04": '.$counts[$id-1];
            }
            oy66:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-03-05": '.$counts[$id-1];
            }
            oy67:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-03-06": '.$counts[$id-1];
            }
            oy68:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-03-07": '.$counts[$id-1];
            }
            oy69:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-03-08": '.$counts[$id-1];
            }
            oy70:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-03-09": '.$counts[$id-1];
            }
            oy71:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-03-10": '.$counts[$id-1];
            }
            oy72:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-03-11": '.$counts[$id-1];
            }
            oy73:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-03-12": '.$counts[$id-1];
            }
            oy74:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-03-13": '.$counts[$id-1];
            }
            oy75:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-03-14": '.$counts[$id-1];
            }
            oy76:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-03-15": '.$counts[$id-1];
            }
            oy77:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-03-16": '.$counts[$id-1];
            }
            oy78:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-03-17": '.$counts[$id-1];
            }
            oy79:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-03-18": '.$counts[$id-1];
            }
            oy80:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-03-19": '.$counts[$id-1];
            }
            oy81:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-03-20": '.$counts[$id-1];
            }
            oy82:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-03-21": '.$counts[$id-1];
            }
            oy83:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-03-22": '.$counts[$id-1];
            }
            oy84:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-03-23": '.$counts[$id-1];
            }
            oy85:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-03-24": '.$counts[$id-1];
            }
            oy86:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-03-25": '.$counts[$id-1];
            }
            oy87:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-03-26": '.$counts[$id-1];
            }
            oy88:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-03-27": '.$counts[$id-1];
            }
            oy89:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-03-28": '.$counts[$id-1];
            }
            oy90:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-03-29": '.$counts[$id-1];
            }
            oy91:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-03-30": '.$counts[$id-1];
            }
            oy92:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-03-31": '.$counts[$id-1];
            }
            oy93:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-04-01": '.$counts[$id-1];
            }
            oy94:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-04-02": '.$counts[$id-1];
            }
            oy95:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-04-03": '.$counts[$id-1];
            }
            oy96:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-04-04": '.$counts[$id-1];
            }
            oy97:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-04-05": '.$counts[$id-1];
            }
            oy98:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-04-06": '.$counts[$id-1];
            }
            oy99:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-04-07": '.$counts[$id-1];
            }
            oy100:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-04-08": '.$counts[$id-1];
            }
            oy101:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-04-09": '.$counts[$id-1];
            }
            oy102:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-04-10": '.$counts[$id-1];
            }
            oy103:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-04-11": '.$counts[$id-1];
            }
            oy104:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-04-12": '.$counts[$id-1];
            }
            oy105:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-04-13": '.$counts[$id-1];
            }
            oy106:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-04-14": '.$counts[$id-1];
            }
            oy107:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-04-15": '.$counts[$id-1];
            }
            oy108:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-04-16": '.$counts[$id-1];
            }
            oy109:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-04-17": '.$counts[$id-1];
            }
            oy110:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-04-18": '.$counts[$id-1];
            }
            oy111:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-04-19": '.$counts[$id-1];
            }
            oy112:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-04-20": '.$counts[$id-1];
            }
            oy113:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-04-21": '.$counts[$id-1];
            }
            oy114:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-04-22": '.$counts[$id-1];
            }
            oy115:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-04-23": '.$counts[$id-1];
            }
            oy116:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-04-24": '.$counts[$id-1];
            }
            oy117:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-04-25": '.$counts[$id-1];
            }
            oy118:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-04-26": '.$counts[$id-1];
            }
            oy119:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-04-27": '.$counts[$id-1];
            }
            oy120:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-04-28": '.$counts[$id-1];
            }
            oy121:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-04-29": '.$counts[$id-1];
            }
            oy122:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-04-30": '.$counts[$id-1];
            }
            oy123:
            ++$id;
            oy124:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-05-01": '.$counts[$id-1];
            }
            oy125:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-05-02": '.$counts[$id-1];
            }
            oy126:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-05-03": '.$counts[$id-1];
            }
            oy127:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-05-04": '.$counts[$id-1];
            }
            oy128:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-05-05": '.$counts[$id-1];
            }
            oy129:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-05-06": '.$counts[$id-1];
            }
            oy130:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-05-07": '.$counts[$id-1];
            }
            oy131:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-05-08": '.$counts[$id-1];
            }
            oy132:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-05-09": '.$counts[$id-1];
            }
            oy133:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-05-10": '.$counts[$id-1];
            }
            oy134:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-05-11": '.$counts[$id-1];
            }
            oy135:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-05-12": '.$counts[$id-1];
            }
            oy136:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-05-13": '.$counts[$id-1];
            }
            oy137:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-05-14": '.$counts[$id-1];
            }
            oy138:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-05-15": '.$counts[$id-1];
            }
            oy139:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-05-16": '.$counts[$id-1];
            }
            oy140:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-05-17": '.$counts[$id-1];
            }
            oy141:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-05-18": '.$counts[$id-1];
            }
            oy142:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-05-19": '.$counts[$id-1];
            }
            oy143:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-05-20": '.$counts[$id-1];
            }
            oy144:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-05-21": '.$counts[$id-1];
            }
            oy145:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-05-22": '.$counts[$id-1];
            }
            oy146:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-05-23": '.$counts[$id-1];
            }
            oy147:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-05-24": '.$counts[$id-1];
            }
            oy148:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-05-25": '.$counts[$id-1];
            }
            oy149:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-05-26": '.$counts[$id-1];
            }
            oy150:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-05-27": '.$counts[$id-1];
            }
            oy151:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-05-28": '.$counts[$id-1];
            }
            oy152:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-05-29": '.$counts[$id-1];
            }
            oy153:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-05-30": '.$counts[$id-1];
            }
            oy154:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-05-31": '.$counts[$id-1];
            }
            oy155:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-06-01": '.$counts[$id-1];
            }
            oy156:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-06-02": '.$counts[$id-1];
            }
            oy157:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-06-03": '.$counts[$id-1];
            }
            oy158:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-06-04": '.$counts[$id-1];
            }
            oy159:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-06-05": '.$counts[$id-1];
            }
            oy160:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-06-06": '.$counts[$id-1];
            }
            oy161:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-06-07": '.$counts[$id-1];
            }
            oy162:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-06-08": '.$counts[$id-1];
            }
            oy163:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-06-09": '.$counts[$id-1];
            }
            oy164:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-06-10": '.$counts[$id-1];
            }
            oy165:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-06-11": '.$counts[$id-1];
            }
            oy166:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-06-12": '.$counts[$id-1];
            }
            oy167:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-06-13": '.$counts[$id-1];
            }
            oy168:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-06-14": '.$counts[$id-1];
            }
            oy169:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-06-15": '.$counts[$id-1];
            }
            oy170:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-06-16": '.$counts[$id-1];
            }
            oy171:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-06-17": '.$counts[$id-1];
            }
            oy172:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-06-18": '.$counts[$id-1];
            }
            oy173:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-06-19": '.$counts[$id-1];
            }
            oy174:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-06-20": '.$counts[$id-1];
            }
            oy175:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-06-21": '.$counts[$id-1];
            }
            oy176:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-06-22": '.$counts[$id-1];
            }
            oy177:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-06-23": '.$counts[$id-1];
            }
            oy178:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-06-24": '.$counts[$id-1];
            }
            oy179:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-06-25": '.$counts[$id-1];
            }
            oy180:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-06-26": '.$counts[$id-1];
            }
            oy181:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-06-27": '.$counts[$id-1];
            }
            oy182:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-06-28": '.$counts[$id-1];
            }
            oy183:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-06-29": '.$counts[$id-1];
            }
            oy184:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-06-30": '.$counts[$id-1];
            }
            oy185:
            ++$id;
            oy186:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-07-01": '.$counts[$id-1];
            }
            oy187:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-07-02": '.$counts[$id-1];
            }
            oy188:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-07-03": '.$counts[$id-1];
            }
            oy189:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-07-04": '.$counts[$id-1];
            }
            oy190:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-07-05": '.$counts[$id-1];
            }
            oy191:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-07-06": '.$counts[$id-1];
            }
            oy192:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-07-07": '.$counts[$id-1];
            }
            oy193:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-07-08": '.$counts[$id-1];
            }
            oy194:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-07-09": '.$counts[$id-1];
            }
            oy195:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-07-10": '.$counts[$id-1];
            }
            oy196:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-07-11": '.$counts[$id-1];
            }
            oy197:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-07-12": '.$counts[$id-1];
            }
            oy198:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-07-13": '.$counts[$id-1];
            }
            oy199:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-07-14": '.$counts[$id-1];
            }
            oy200:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-07-15": '.$counts[$id-1];
            }
            oy201:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-07-16": '.$counts[$id-1];
            }
            oy202:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-07-17": '.$counts[$id-1];
            }
            oy203:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-07-18": '.$counts[$id-1];
            }
            oy204:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-07-19": '.$counts[$id-1];
            }
            oy205:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-07-20": '.$counts[$id-1];
            }
            oy206:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-07-21": '.$counts[$id-1];
            }
            oy207:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-07-22": '.$counts[$id-1];
            }
            oy208:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-07-23": '.$counts[$id-1];
            }
            oy209:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-07-24": '.$counts[$id-1];
            }
            oy210:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-07-25": '.$counts[$id-1];
            }
            oy211:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-07-26": '.$counts[$id-1];
            }
            oy212:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-07-27": '.$counts[$id-1];
            }
            oy213:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-07-28": '.$counts[$id-1];
            }
            oy214:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-07-29": '.$counts[$id-1];
            }
            oy215:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-07-30": '.$counts[$id-1];
            }
            oy216:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-07-31": '.$counts[$id-1];
            }
            oy217:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-08-01": '.$counts[$id-1];
            }
            oy218:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-08-02": '.$counts[$id-1];
            }
            oy219:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-08-03": '.$counts[$id-1];
            }
            oy220:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-08-04": '.$counts[$id-1];
            }
            oy221:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-08-05": '.$counts[$id-1];
            }
            oy222:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-08-06": '.$counts[$id-1];
            }
            oy223:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-08-07": '.$counts[$id-1];
            }
            oy224:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-08-08": '.$counts[$id-1];
            }
            oy225:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-08-09": '.$counts[$id-1];
            }
            oy226:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-08-10": '.$counts[$id-1];
            }
            oy227:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-08-11": '.$counts[$id-1];
            }
            oy228:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-08-12": '.$counts[$id-1];
            }
            oy229:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-08-13": '.$counts[$id-1];
            }
            oy230:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-08-14": '.$counts[$id-1];
            }
            oy231:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-08-15": '.$counts[$id-1];
            }
            oy232:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-08-16": '.$counts[$id-1];
            }
            oy233:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-08-17": '.$counts[$id-1];
            }
            oy234:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-08-18": '.$counts[$id-1];
            }
            oy235:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-08-19": '.$counts[$id-1];
            }
            oy236:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-08-20": '.$counts[$id-1];
            }
            oy237:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-08-21": '.$counts[$id-1];
            }
            oy238:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-08-22": '.$counts[$id-1];
            }
            oy239:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-08-23": '.$counts[$id-1];
            }
            oy240:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-08-24": '.$counts[$id-1];
            }
            oy241:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-08-25": '.$counts[$id-1];
            }
            oy242:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-08-26": '.$counts[$id-1];
            }
            oy243:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-08-27": '.$counts[$id-1];
            }
            oy244:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-08-28": '.$counts[$id-1];
            }
            oy245:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-08-29": '.$counts[$id-1];
            }
            oy246:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-08-30": '.$counts[$id-1];
            }
            oy247:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-08-31": '.$counts[$id-1];
            }
            oy248:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-09-01": '.$counts[$id-1];
            }
            oy249:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-09-02": '.$counts[$id-1];
            }
            oy250:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-09-03": '.$counts[$id-1];
            }
            oy251:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-09-04": '.$counts[$id-1];
            }
            oy252:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-09-05": '.$counts[$id-1];
            }
            oy253:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-09-06": '.$counts[$id-1];
            }
            oy254:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-09-07": '.$counts[$id-1];
            }
            oy255:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-09-08": '.$counts[$id-1];
            }
            oy256:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-09-09": '.$counts[$id-1];
            }
            oy257:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-09-10": '.$counts[$id-1];
            }
            oy258:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-09-11": '.$counts[$id-1];
            }
            oy259:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-09-12": '.$counts[$id-1];
            }
            oy260:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-09-13": '.$counts[$id-1];
            }
            oy261:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-09-14": '.$counts[$id-1];
            }
            oy262:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-09-15": '.$counts[$id-1];
            }
            oy263:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-09-16": '.$counts[$id-1];
            }
            oy264:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-09-17": '.$counts[$id-1];
            }
            oy265:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-09-18": '.$counts[$id-1];
            }
            oy266:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-09-19": '.$counts[$id-1];
            }
            oy267:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-09-20": '.$counts[$id-1];
            }
            oy268:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-09-21": '.$counts[$id-1];
            }
            oy269:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-09-22": '.$counts[$id-1];
            }
            oy270:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-09-23": '.$counts[$id-1];
            }
            oy271:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-09-24": '.$counts[$id-1];
            }
            oy272:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-09-25": '.$counts[$id-1];
            }
            oy273:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-09-26": '.$counts[$id-1];
            }
            oy274:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-09-27": '.$counts[$id-1];
            }
            oy275:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-09-28": '.$counts[$id-1];
            }
            oy276:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-09-29": '.$counts[$id-1];
            }
            oy277:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-09-30": '.$counts[$id-1];
            }
            oy278:
            ++$id;
            oy279:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-10-01": '.$counts[$id-1];
            }
            oy280:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-10-02": '.$counts[$id-1];
            }
            oy281:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-10-03": '.$counts[$id-1];
            }
            oy282:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-10-04": '.$counts[$id-1];
            }
            oy283:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-10-05": '.$counts[$id-1];
            }
            oy284:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-10-06": '.$counts[$id-1];
            }
            oy285:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-10-07": '.$counts[$id-1];
            }
            oy286:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-10-08": '.$counts[$id-1];
            }
            oy287:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-10-09": '.$counts[$id-1];
            }
            oy288:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-10-10": '.$counts[$id-1];
            }
            oy289:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-10-11": '.$counts[$id-1];
            }
            oy290:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-10-12": '.$counts[$id-1];
            }
            oy291:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-10-13": '.$counts[$id-1];
            }
            oy292:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-10-14": '.$counts[$id-1];
            }
            oy293:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-10-15": '.$counts[$id-1];
            }
            oy294:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-10-16": '.$counts[$id-1];
            }
            oy295:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-10-17": '.$counts[$id-1];
            }
            oy296:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-10-18": '.$counts[$id-1];
            }
            oy297:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-10-19": '.$counts[$id-1];
            }
            oy298:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-10-20": '.$counts[$id-1];
            }
            oy299:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-10-21": '.$counts[$id-1];
            }
            oy300:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-10-22": '.$counts[$id-1];
            }
            oy301:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-10-23": '.$counts[$id-1];
            }
            oy302:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-10-24": '.$counts[$id-1];
            }
            oy303:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-10-25": '.$counts[$id-1];
            }
            oy304:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-10-26": '.$counts[$id-1];
            }
            oy305:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-10-27": '.$counts[$id-1];
            }
            oy306:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-10-28": '.$counts[$id-1];
            }
            oy307:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-10-29": '.$counts[$id-1];
            }
            oy308:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-10-30": '.$counts[$id-1];
            }
            oy309:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-10-31": '.$counts[$id-1];
            }
            oy310:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-11-01": '.$counts[$id-1];
            }
            oy311:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-11-02": '.$counts[$id-1];
            }
            oy312:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-11-03": '.$counts[$id-1];
            }
            oy313:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-11-04": '.$counts[$id-1];
            }
            oy314:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-11-05": '.$counts[$id-1];
            }
            oy315:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-11-06": '.$counts[$id-1];
            }
            oy316:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-11-07": '.$counts[$id-1];
            }
            oy317:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-11-08": '.$counts[$id-1];
            }
            oy318:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-11-09": '.$counts[$id-1];
            }
            oy319:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-11-10": '.$counts[$id-1];
            }
            oy320:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-11-11": '.$counts[$id-1];
            }
            oy321:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-11-12": '.$counts[$id-1];
            }
            oy322:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-11-13": '.$counts[$id-1];
            }
            oy323:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-11-14": '.$counts[$id-1];
            }
            oy324:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-11-15": '.$counts[$id-1];
            }
            oy325:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-11-16": '.$counts[$id-1];
            }
            oy326:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-11-17": '.$counts[$id-1];
            }
            oy327:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-11-18": '.$counts[$id-1];
            }
            oy328:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-11-19": '.$counts[$id-1];
            }
            oy329:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-11-20": '.$counts[$id-1];
            }
            oy330:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-11-21": '.$counts[$id-1];
            }
            oy331:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-11-22": '.$counts[$id-1];
            }
            oy332:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-11-23": '.$counts[$id-1];
            }
            oy333:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-11-24": '.$counts[$id-1];
            }
            oy334:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-11-25": '.$counts[$id-1];
            }
            oy335:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-11-26": '.$counts[$id-1];
            }
            oy336:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-11-27": '.$counts[$id-1];
            }
            oy337:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-11-28": '.$counts[$id-1];
            }
            oy338:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-11-29": '.$counts[$id-1];
            }
            oy339:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-11-30": '.$counts[$id-1];
            }
            oy340:
            ++$id;
            oy341:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-12-01": '.$counts[$id-1];
            }
            oy342:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-12-02": '.$counts[$id-1];
            }
            oy343:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-12-03": '.$counts[$id-1];
            }
            oy344:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-12-04": '.$counts[$id-1];
            }
            oy345:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-12-05": '.$counts[$id-1];
            }
            oy346:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-12-06": '.$counts[$id-1];
            }
            oy347:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-12-07": '.$counts[$id-1];
            }
            oy348:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-12-08": '.$counts[$id-1];
            }
            oy349:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-12-09": '.$counts[$id-1];
            }
            oy350:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-12-10": '.$counts[$id-1];
            }
            oy351:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-12-11": '.$counts[$id-1];
            }
            oy352:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-12-12": '.$counts[$id-1];
            }
            oy353:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-12-13": '.$counts[$id-1];
            }
            oy354:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-12-14": '.$counts[$id-1];
            }
            oy355:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-12-15": '.$counts[$id-1];
            }
            oy356:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-12-16": '.$counts[$id-1];
            }
            oy357:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-12-17": '.$counts[$id-1];
            }
            oy358:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-12-18": '.$counts[$id-1];
            }
            oy359:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-12-19": '.$counts[$id-1];
            }
            oy360:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-12-20": '.$counts[$id-1];
            }
            oy361:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-12-21": '.$counts[$id-1];
            }
            oy362:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-12-22": '.$counts[$id-1];
            }
            oy363:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-12-23": '.$counts[$id-1];
            }
            oy364:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-12-24": '.$counts[$id-1];
            }
            oy365:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-12-25": '.$counts[$id-1];
            }
            oy366:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-12-26": '.$counts[$id-1];
            }
            oy367:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-12-27": '.$counts[$id-1];
            }
            oy368:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-12-28": '.$counts[$id-1];
            }
            oy369:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-12-29": '.$counts[$id-1];
            }
            oy370:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-12-30": '.$counts[$id-1];
            }
            oy371:
            if ($counts[$id++] > 0) {
                $j .= ",\n        \"".$year.'-12-31": '.$counts[$id-1];
            }

            oy372:
            ++$year;
            if($year <= 2026) goto o2;
            
            $j .= "\n    }";
            \fwrite($fo, $j);
            $fu = false;
        }

        \fwrite($fo, "\n}");
        \fclose($fo);
        \exit();
    }
}