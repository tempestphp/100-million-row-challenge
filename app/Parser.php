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

            $id += 2232;
        }
        $counts = \array_fill(0, $id, 0); self::$keepAlive[] = &$counts;

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
        foreach($sequence as $id => $sequenceNo) {
            if (!isset($sequenceNo)) continue;

            if (false === $fu)
                $j = ",\n".'    "\\/blog\\/'.$uriIds[$id]."\": {\n";
            else
                $j = '    "\\/blog\\/'.$uriIds[$id]."\": {\n";

            $year = 2021;
o1:
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-01-01": '.$count;
                goto oy1;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-01-02": '.$count;
                goto oy2;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-01-03": '.$count;
                goto oy3;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-01-04": '.$count;
                goto oy4;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-01-05": '.$count;
                goto oy5;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-01-06": '.$count;
                goto oy6;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-01-07": '.$count;
                goto oy7;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-01-08": '.$count;
                goto oy8;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-01-09": '.$count;
                goto oy9;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-01-10": '.$count;
                goto oy10;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-01-11": '.$count;
                goto oy11;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-01-12": '.$count;
                goto oy12;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-01-13": '.$count;
                goto oy13;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-01-14": '.$count;
                goto oy14;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-01-15": '.$count;
                goto oy15;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-01-16": '.$count;
                goto oy16;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-01-17": '.$count;
                goto oy17;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-01-18": '.$count;
                goto oy18;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-01-19": '.$count;
                goto oy19;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-01-20": '.$count;
                goto oy20;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-01-21": '.$count;
                goto oy21;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-01-22": '.$count;
                goto oy22;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-01-23": '.$count;
                goto oy23;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-01-24": '.$count;
                goto oy24;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-01-25": '.$count;
                goto oy25;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-01-26": '.$count;
                goto oy26;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-01-27": '.$count;
                goto oy27;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-01-28": '.$count;
                goto oy28;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-01-29": '.$count;
                goto oy29;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-01-30": '.$count;
                goto oy30;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-01-31": '.$count;
                goto oy31;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-02-01": '.$count;
                goto oy32;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-02-02": '.$count;
                goto oy33;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-02-03": '.$count;
                goto oy34;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-02-04": '.$count;
                goto oy35;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-02-05": '.$count;
                goto oy36;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-02-06": '.$count;
                goto oy37;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-02-07": '.$count;
                goto oy38;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-02-08": '.$count;
                goto oy39;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-02-09": '.$count;
                goto oy40;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-02-10": '.$count;
                goto oy41;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-02-11": '.$count;
                goto oy42;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-02-12": '.$count;
                goto oy43;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-02-13": '.$count;
                goto oy44;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-02-14": '.$count;
                goto oy45;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-02-15": '.$count;
                goto oy46;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-02-16": '.$count;
                goto oy47;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-02-17": '.$count;
                goto oy48;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-02-18": '.$count;
                goto oy49;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-02-19": '.$count;
                goto oy50;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-02-20": '.$count;
                goto oy51;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-02-21": '.$count;
                goto oy52;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-02-22": '.$count;
                goto oy53;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-02-23": '.$count;
                goto oy54;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-02-24": '.$count;
                goto oy55;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-02-25": '.$count;
                goto oy56;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-02-26": '.$count;
                goto oy57;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-02-27": '.$count;
                goto oy58;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-02-28": '.$count;
                goto oy59;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-02-29": '.$count;
                goto oy60;
            }
            $id += 2;
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-03-01": '.$count;
                goto oy63;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-03-02": '.$count;
                goto oy64;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-03-03": '.$count;
                goto oy65;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-03-04": '.$count;
                goto oy66;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-03-05": '.$count;
                goto oy67;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-03-06": '.$count;
                goto oy68;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-03-07": '.$count;
                goto oy69;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-03-08": '.$count;
                goto oy70;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-03-09": '.$count;
                goto oy71;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-03-10": '.$count;
                goto oy72;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-03-11": '.$count;
                goto oy73;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-03-12": '.$count;
                goto oy74;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-03-13": '.$count;
                goto oy75;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-03-14": '.$count;
                goto oy76;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-03-15": '.$count;
                goto oy77;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-03-16": '.$count;
                goto oy78;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-03-17": '.$count;
                goto oy79;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-03-18": '.$count;
                goto oy80;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-03-19": '.$count;
                goto oy81;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-03-20": '.$count;
                goto oy82;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-03-21": '.$count;
                goto oy83;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-03-22": '.$count;
                goto oy84;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-03-23": '.$count;
                goto oy85;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-03-24": '.$count;
                goto oy86;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-03-25": '.$count;
                goto oy87;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-03-26": '.$count;
                goto oy88;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-03-27": '.$count;
                goto oy89;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-03-28": '.$count;
                goto oy90;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-03-29": '.$count;
                goto oy91;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-03-30": '.$count;
                goto oy92;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-03-31": '.$count;
                goto oy93;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-04-01": '.$count;
                goto oy94;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-04-02": '.$count;
                goto oy95;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-04-03": '.$count;
                goto oy96;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-04-04": '.$count;
                goto oy97;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-04-05": '.$count;
                goto oy98;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-04-06": '.$count;
                goto oy99;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-04-07": '.$count;
                goto oy100;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-04-08": '.$count;
                goto oy101;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-04-09": '.$count;
                goto oy102;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-04-10": '.$count;
                goto oy103;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-04-11": '.$count;
                goto oy104;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-04-12": '.$count;
                goto oy105;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-04-13": '.$count;
                goto oy106;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-04-14": '.$count;
                goto oy107;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-04-15": '.$count;
                goto oy108;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-04-16": '.$count;
                goto oy109;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-04-17": '.$count;
                goto oy110;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-04-18": '.$count;
                goto oy111;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-04-19": '.$count;
                goto oy112;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-04-20": '.$count;
                goto oy113;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-04-21": '.$count;
                goto oy114;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-04-22": '.$count;
                goto oy115;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-04-23": '.$count;
                goto oy116;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-04-24": '.$count;
                goto oy117;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-04-25": '.$count;
                goto oy118;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-04-26": '.$count;
                goto oy119;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-04-27": '.$count;
                goto oy120;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-04-28": '.$count;
                goto oy121;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-04-29": '.$count;
                goto oy122;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-04-30": '.$count;
                goto oy123;
            }
            ++$id;
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-05-01": '.$count;
                goto oy125;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-05-02": '.$count;
                goto oy126;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-05-03": '.$count;
                goto oy127;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-05-04": '.$count;
                goto oy128;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-05-05": '.$count;
                goto oy129;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-05-06": '.$count;
                goto oy130;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-05-07": '.$count;
                goto oy131;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-05-08": '.$count;
                goto oy132;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-05-09": '.$count;
                goto oy133;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-05-10": '.$count;
                goto oy134;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-05-11": '.$count;
                goto oy135;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-05-12": '.$count;
                goto oy136;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-05-13": '.$count;
                goto oy137;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-05-14": '.$count;
                goto oy138;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-05-15": '.$count;
                goto oy139;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-05-16": '.$count;
                goto oy140;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-05-17": '.$count;
                goto oy141;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-05-18": '.$count;
                goto oy142;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-05-19": '.$count;
                goto oy143;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-05-20": '.$count;
                goto oy144;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-05-21": '.$count;
                goto oy145;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-05-22": '.$count;
                goto oy146;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-05-23": '.$count;
                goto oy147;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-05-24": '.$count;
                goto oy148;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-05-25": '.$count;
                goto oy149;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-05-26": '.$count;
                goto oy150;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-05-27": '.$count;
                goto oy151;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-05-28": '.$count;
                goto oy152;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-05-29": '.$count;
                goto oy153;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-05-30": '.$count;
                goto oy154;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-05-31": '.$count;
                goto oy155;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-06-01": '.$count;
                goto oy156;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-06-02": '.$count;
                goto oy157;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-06-03": '.$count;
                goto oy158;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-06-04": '.$count;
                goto oy159;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-06-05": '.$count;
                goto oy160;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-06-06": '.$count;
                goto oy161;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-06-07": '.$count;
                goto oy162;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-06-08": '.$count;
                goto oy163;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-06-09": '.$count;
                goto oy164;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-06-10": '.$count;
                goto oy165;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-06-11": '.$count;
                goto oy166;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-06-12": '.$count;
                goto oy167;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-06-13": '.$count;
                goto oy168;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-06-14": '.$count;
                goto oy169;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-06-15": '.$count;
                goto oy170;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-06-16": '.$count;
                goto oy171;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-06-17": '.$count;
                goto oy172;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-06-18": '.$count;
                goto oy173;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-06-19": '.$count;
                goto oy174;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-06-20": '.$count;
                goto oy175;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-06-21": '.$count;
                goto oy176;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-06-22": '.$count;
                goto oy177;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-06-23": '.$count;
                goto oy178;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-06-24": '.$count;
                goto oy179;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-06-25": '.$count;
                goto oy180;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-06-26": '.$count;
                goto oy181;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-06-27": '.$count;
                goto oy182;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-06-28": '.$count;
                goto oy183;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-06-29": '.$count;
                goto oy184;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-06-30": '.$count;
                goto oy185;
            }
            ++$id;
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-07-01": '.$count;
                goto oy187;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-07-02": '.$count;
                goto oy188;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-07-03": '.$count;
                goto oy189;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-07-04": '.$count;
                goto oy190;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-07-05": '.$count;
                goto oy191;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-07-06": '.$count;
                goto oy192;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-07-07": '.$count;
                goto oy193;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-07-08": '.$count;
                goto oy194;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-07-09": '.$count;
                goto oy195;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-07-10": '.$count;
                goto oy196;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-07-11": '.$count;
                goto oy197;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-07-12": '.$count;
                goto oy198;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-07-13": '.$count;
                goto oy199;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-07-14": '.$count;
                goto oy200;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-07-15": '.$count;
                goto oy201;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-07-16": '.$count;
                goto oy202;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-07-17": '.$count;
                goto oy203;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-07-18": '.$count;
                goto oy204;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-07-19": '.$count;
                goto oy205;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-07-20": '.$count;
                goto oy206;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-07-21": '.$count;
                goto oy207;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-07-22": '.$count;
                goto oy208;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-07-23": '.$count;
                goto oy209;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-07-24": '.$count;
                goto oy210;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-07-25": '.$count;
                goto oy211;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-07-26": '.$count;
                goto oy212;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-07-27": '.$count;
                goto oy213;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-07-28": '.$count;
                goto oy214;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-07-29": '.$count;
                goto oy215;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-07-30": '.$count;
                goto oy216;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-07-31": '.$count;
                goto oy217;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-08-01": '.$count;
                goto oy218;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-08-02": '.$count;
                goto oy219;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-08-03": '.$count;
                goto oy220;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-08-04": '.$count;
                goto oy221;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-08-05": '.$count;
                goto oy222;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-08-06": '.$count;
                goto oy223;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-08-07": '.$count;
                goto oy224;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-08-08": '.$count;
                goto oy225;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-08-09": '.$count;
                goto oy226;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-08-10": '.$count;
                goto oy227;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-08-11": '.$count;
                goto oy228;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-08-12": '.$count;
                goto oy229;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-08-13": '.$count;
                goto oy230;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-08-14": '.$count;
                goto oy231;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-08-15": '.$count;
                goto oy232;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-08-16": '.$count;
                goto oy233;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-08-17": '.$count;
                goto oy234;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-08-18": '.$count;
                goto oy235;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-08-19": '.$count;
                goto oy236;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-08-20": '.$count;
                goto oy237;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-08-21": '.$count;
                goto oy238;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-08-22": '.$count;
                goto oy239;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-08-23": '.$count;
                goto oy240;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-08-24": '.$count;
                goto oy241;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-08-25": '.$count;
                goto oy242;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-08-26": '.$count;
                goto oy243;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-08-27": '.$count;
                goto oy244;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-08-28": '.$count;
                goto oy245;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-08-29": '.$count;
                goto oy246;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-08-30": '.$count;
                goto oy247;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-08-31": '.$count;
                goto oy248;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-09-01": '.$count;
                goto oy249;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-09-02": '.$count;
                goto oy250;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-09-03": '.$count;
                goto oy251;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-09-04": '.$count;
                goto oy252;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-09-05": '.$count;
                goto oy253;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-09-06": '.$count;
                goto oy254;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-09-07": '.$count;
                goto oy255;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-09-08": '.$count;
                goto oy256;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-09-09": '.$count;
                goto oy257;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-09-10": '.$count;
                goto oy258;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-09-11": '.$count;
                goto oy259;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-09-12": '.$count;
                goto oy260;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-09-13": '.$count;
                goto oy261;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-09-14": '.$count;
                goto oy262;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-09-15": '.$count;
                goto oy263;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-09-16": '.$count;
                goto oy264;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-09-17": '.$count;
                goto oy265;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-09-18": '.$count;
                goto oy266;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-09-19": '.$count;
                goto oy267;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-09-20": '.$count;
                goto oy268;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-09-21": '.$count;
                goto oy269;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-09-22": '.$count;
                goto oy270;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-09-23": '.$count;
                goto oy271;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-09-24": '.$count;
                goto oy272;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-09-25": '.$count;
                goto oy273;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-09-26": '.$count;
                goto oy274;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-09-27": '.$count;
                goto oy275;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-09-28": '.$count;
                goto oy276;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-09-29": '.$count;
                goto oy277;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-09-30": '.$count;
                goto oy278;
            }
            ++$id;
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-10-01": '.$count;
                goto oy280;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-10-02": '.$count;
                goto oy281;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-10-03": '.$count;
                goto oy282;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-10-04": '.$count;
                goto oy283;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-10-05": '.$count;
                goto oy284;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-10-06": '.$count;
                goto oy285;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-10-07": '.$count;
                goto oy286;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-10-08": '.$count;
                goto oy287;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-10-09": '.$count;
                goto oy288;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-10-10": '.$count;
                goto oy289;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-10-11": '.$count;
                goto oy290;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-10-12": '.$count;
                goto oy291;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-10-13": '.$count;
                goto oy292;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-10-14": '.$count;
                goto oy293;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-10-15": '.$count;
                goto oy294;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-10-16": '.$count;
                goto oy295;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-10-17": '.$count;
                goto oy296;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-10-18": '.$count;
                goto oy297;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-10-19": '.$count;
                goto oy298;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-10-20": '.$count;
                goto oy299;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-10-21": '.$count;
                goto oy300;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-10-22": '.$count;
                goto oy301;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-10-23": '.$count;
                goto oy302;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-10-24": '.$count;
                goto oy303;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-10-25": '.$count;
                goto oy304;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-10-26": '.$count;
                goto oy305;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-10-27": '.$count;
                goto oy306;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-10-28": '.$count;
                goto oy307;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-10-29": '.$count;
                goto oy308;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-10-30": '.$count;
                goto oy309;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-10-31": '.$count;
                goto oy310;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-11-01": '.$count;
                goto oy311;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-11-02": '.$count;
                goto oy312;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-11-03": '.$count;
                goto oy313;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-11-04": '.$count;
                goto oy314;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-11-05": '.$count;
                goto oy315;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-11-06": '.$count;
                goto oy316;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-11-07": '.$count;
                goto oy317;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-11-08": '.$count;
                goto oy318;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-11-09": '.$count;
                goto oy319;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-11-10": '.$count;
                goto oy320;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-11-11": '.$count;
                goto oy321;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-11-12": '.$count;
                goto oy322;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-11-13": '.$count;
                goto oy323;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-11-14": '.$count;
                goto oy324;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-11-15": '.$count;
                goto oy325;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-11-16": '.$count;
                goto oy326;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-11-17": '.$count;
                goto oy327;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-11-18": '.$count;
                goto oy328;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-11-19": '.$count;
                goto oy329;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-11-20": '.$count;
                goto oy330;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-11-21": '.$count;
                goto oy331;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-11-22": '.$count;
                goto oy332;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-11-23": '.$count;
                goto oy333;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-11-24": '.$count;
                goto oy334;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-11-25": '.$count;
                goto oy335;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-11-26": '.$count;
                goto oy336;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-11-27": '.$count;
                goto oy337;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-11-28": '.$count;
                goto oy338;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-11-29": '.$count;
                goto oy339;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-11-30": '.$count;
                goto oy340;
            }
            ++$id;
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-12-01": '.$count;
                goto oy342;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-12-02": '.$count;
                goto oy343;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-12-03": '.$count;
                goto oy344;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-12-04": '.$count;
                goto oy345;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-12-05": '.$count;
                goto oy346;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-12-06": '.$count;
                goto oy347;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-12-07": '.$count;
                goto oy348;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-12-08": '.$count;
                goto oy349;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-12-09": '.$count;
                goto oy350;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-12-10": '.$count;
                goto oy351;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-12-11": '.$count;
                goto oy352;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-12-12": '.$count;
                goto oy353;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-12-13": '.$count;
                goto oy354;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-12-14": '.$count;
                goto oy355;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-12-15": '.$count;
                goto oy356;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-12-16": '.$count;
                goto oy357;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-12-17": '.$count;
                goto oy358;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-12-18": '.$count;
                goto oy359;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-12-19": '.$count;
                goto oy360;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-12-20": '.$count;
                goto oy361;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-12-21": '.$count;
                goto oy362;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-12-22": '.$count;
                goto oy363;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-12-23": '.$count;
                goto oy364;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-12-24": '.$count;
                goto oy365;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-12-25": '.$count;
                goto oy366;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-12-26": '.$count;
                goto oy367;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-12-27": '.$count;
                goto oy368;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-12-28": '.$count;
                goto oy369;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-12-29": '.$count;
                goto oy370;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-12-30": '.$count;
                goto oy371;
            }
            if ($count = $counts[$id++]) {
                $j .= '        "'.$year.'-12-31": '.$count;
                goto oy372;
            }

            if(++$year <= 2026) goto o1;
            continue;

o2:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-01-01": '.$count;
            }
            oy1:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-01-02": '.$count;
            }
            oy2:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-01-03": '.$count;
            }
            oy3:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-01-04": '.$count;
            }
            oy4:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-01-05": '.$count;
            }
            oy5:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-01-06": '.$count;
            }
            oy6:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-01-07": '.$count;
            }
            oy7:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-01-08": '.$count;
            }
            oy8:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-01-09": '.$count;
            }
            oy9:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-01-10": '.$count;
            }
            oy10:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-01-11": '.$count;
            }
            oy11:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-01-12": '.$count;
            }
            oy12:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-01-13": '.$count;
            }
            oy13:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-01-14": '.$count;
            }
            oy14:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-01-15": '.$count;
            }
            oy15:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-01-16": '.$count;
            }
            oy16:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-01-17": '.$count;
            }
            oy17:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-01-18": '.$count;
            }
            oy18:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-01-19": '.$count;
            }
            oy19:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-01-20": '.$count;
            }
            oy20:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-01-21": '.$count;
            }
            oy21:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-01-22": '.$count;
            }
            oy22:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-01-23": '.$count;
            }
            oy23:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-01-24": '.$count;
            }
            oy24:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-01-25": '.$count;
            }
            oy25:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-01-26": '.$count;
            }
            oy26:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-01-27": '.$count;
            }
            oy27:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-01-28": '.$count;
            }
            oy28:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-01-29": '.$count;
            }
            oy29:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-01-30": '.$count;
            }
            oy30:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-01-31": '.$count;
            }
            oy31:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-02-01": '.$count;
            }
            oy32:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-02-02": '.$count;
            }
            oy33:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-02-03": '.$count;
            }
            oy34:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-02-04": '.$count;
            }
            oy35:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-02-05": '.$count;
            }
            oy36:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-02-06": '.$count;
            }
            oy37:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-02-07": '.$count;
            }
            oy38:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-02-08": '.$count;
            }
            oy39:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-02-09": '.$count;
            }
            oy40:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-02-10": '.$count;
            }
            oy41:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-02-11": '.$count;
            }
            oy42:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-02-12": '.$count;
            }
            oy43:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-02-13": '.$count;
            }
            oy44:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-02-14": '.$count;
            }
            oy45:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-02-15": '.$count;
            }
            oy46:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-02-16": '.$count;
            }
            oy47:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-02-17": '.$count;
            }
            oy48:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-02-18": '.$count;
            }
            oy49:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-02-19": '.$count;
            }
            oy50:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-02-20": '.$count;
            }
            oy51:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-02-21": '.$count;
            }
            oy52:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-02-22": '.$count;
            }
            oy53:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-02-23": '.$count;
            }
            oy54:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-02-24": '.$count;
            }
            oy55:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-02-25": '.$count;
            }
            oy56:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-02-26": '.$count;
            }
            oy57:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-02-27": '.$count;
            }
            oy58:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-02-28": '.$count;
            }
            oy59:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-02-29": '.$count;
            }
            oy60:
            $id += 2;
            oy62:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-03-01": '.$count;
            }
            oy63:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-03-02": '.$count;
            }
            oy64:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-03-03": '.$count;
            }
            oy65:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-03-04": '.$count;
            }
            oy66:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-03-05": '.$count;
            }
            oy67:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-03-06": '.$count;
            }
            oy68:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-03-07": '.$count;
            }
            oy69:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-03-08": '.$count;
            }
            oy70:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-03-09": '.$count;
            }
            oy71:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-03-10": '.$count;
            }
            oy72:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-03-11": '.$count;
            }
            oy73:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-03-12": '.$count;
            }
            oy74:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-03-13": '.$count;
            }
            oy75:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-03-14": '.$count;
            }
            oy76:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-03-15": '.$count;
            }
            oy77:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-03-16": '.$count;
            }
            oy78:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-03-17": '.$count;
            }
            oy79:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-03-18": '.$count;
            }
            oy80:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-03-19": '.$count;
            }
            oy81:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-03-20": '.$count;
            }
            oy82:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-03-21": '.$count;
            }
            oy83:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-03-22": '.$count;
            }
            oy84:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-03-23": '.$count;
            }
            oy85:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-03-24": '.$count;
            }
            oy86:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-03-25": '.$count;
            }
            oy87:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-03-26": '.$count;
            }
            oy88:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-03-27": '.$count;
            }
            oy89:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-03-28": '.$count;
            }
            oy90:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-03-29": '.$count;
            }
            oy91:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-03-30": '.$count;
            }
            oy92:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-03-31": '.$count;
            }
            oy93:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-04-01": '.$count;
            }
            oy94:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-04-02": '.$count;
            }
            oy95:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-04-03": '.$count;
            }
            oy96:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-04-04": '.$count;
            }
            oy97:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-04-05": '.$count;
            }
            oy98:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-04-06": '.$count;
            }
            oy99:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-04-07": '.$count;
            }
            oy100:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-04-08": '.$count;
            }
            oy101:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-04-09": '.$count;
            }
            oy102:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-04-10": '.$count;
            }
            oy103:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-04-11": '.$count;
            }
            oy104:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-04-12": '.$count;
            }
            oy105:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-04-13": '.$count;
            }
            oy106:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-04-14": '.$count;
            }
            oy107:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-04-15": '.$count;
            }
            oy108:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-04-16": '.$count;
            }
            oy109:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-04-17": '.$count;
            }
            oy110:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-04-18": '.$count;
            }
            oy111:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-04-19": '.$count;
            }
            oy112:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-04-20": '.$count;
            }
            oy113:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-04-21": '.$count;
            }
            oy114:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-04-22": '.$count;
            }
            oy115:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-04-23": '.$count;
            }
            oy116:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-04-24": '.$count;
            }
            oy117:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-04-25": '.$count;
            }
            oy118:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-04-26": '.$count;
            }
            oy119:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-04-27": '.$count;
            }
            oy120:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-04-28": '.$count;
            }
            oy121:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-04-29": '.$count;
            }
            oy122:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-04-30": '.$count;
            }
            oy123:
            ++$id;
            oy124:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-05-01": '.$count;
            }
            oy125:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-05-02": '.$count;
            }
            oy126:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-05-03": '.$count;
            }
            oy127:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-05-04": '.$count;
            }
            oy128:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-05-05": '.$count;
            }
            oy129:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-05-06": '.$count;
            }
            oy130:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-05-07": '.$count;
            }
            oy131:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-05-08": '.$count;
            }
            oy132:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-05-09": '.$count;
            }
            oy133:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-05-10": '.$count;
            }
            oy134:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-05-11": '.$count;
            }
            oy135:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-05-12": '.$count;
            }
            oy136:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-05-13": '.$count;
            }
            oy137:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-05-14": '.$count;
            }
            oy138:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-05-15": '.$count;
            }
            oy139:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-05-16": '.$count;
            }
            oy140:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-05-17": '.$count;
            }
            oy141:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-05-18": '.$count;
            }
            oy142:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-05-19": '.$count;
            }
            oy143:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-05-20": '.$count;
            }
            oy144:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-05-21": '.$count;
            }
            oy145:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-05-22": '.$count;
            }
            oy146:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-05-23": '.$count;
            }
            oy147:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-05-24": '.$count;
            }
            oy148:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-05-25": '.$count;
            }
            oy149:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-05-26": '.$count;
            }
            oy150:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-05-27": '.$count;
            }
            oy151:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-05-28": '.$count;
            }
            oy152:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-05-29": '.$count;
            }
            oy153:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-05-30": '.$count;
            }
            oy154:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-05-31": '.$count;
            }
            oy155:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-06-01": '.$count;
            }
            oy156:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-06-02": '.$count;
            }
            oy157:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-06-03": '.$count;
            }
            oy158:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-06-04": '.$count;
            }
            oy159:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-06-05": '.$count;
            }
            oy160:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-06-06": '.$count;
            }
            oy161:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-06-07": '.$count;
            }
            oy162:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-06-08": '.$count;
            }
            oy163:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-06-09": '.$count;
            }
            oy164:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-06-10": '.$count;
            }
            oy165:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-06-11": '.$count;
            }
            oy166:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-06-12": '.$count;
            }
            oy167:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-06-13": '.$count;
            }
            oy168:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-06-14": '.$count;
            }
            oy169:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-06-15": '.$count;
            }
            oy170:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-06-16": '.$count;
            }
            oy171:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-06-17": '.$count;
            }
            oy172:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-06-18": '.$count;
            }
            oy173:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-06-19": '.$count;
            }
            oy174:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-06-20": '.$count;
            }
            oy175:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-06-21": '.$count;
            }
            oy176:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-06-22": '.$count;
            }
            oy177:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-06-23": '.$count;
            }
            oy178:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-06-24": '.$count;
            }
            oy179:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-06-25": '.$count;
            }
            oy180:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-06-26": '.$count;
            }
            oy181:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-06-27": '.$count;
            }
            oy182:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-06-28": '.$count;
            }
            oy183:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-06-29": '.$count;
            }
            oy184:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-06-30": '.$count;
            }
            oy185:
            ++$id;
            oy186:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-07-01": '.$count;
            }
            oy187:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-07-02": '.$count;
            }
            oy188:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-07-03": '.$count;
            }
            oy189:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-07-04": '.$count;
            }
            oy190:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-07-05": '.$count;
            }
            oy191:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-07-06": '.$count;
            }
            oy192:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-07-07": '.$count;
            }
            oy193:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-07-08": '.$count;
            }
            oy194:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-07-09": '.$count;
            }
            oy195:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-07-10": '.$count;
            }
            oy196:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-07-11": '.$count;
            }
            oy197:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-07-12": '.$count;
            }
            oy198:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-07-13": '.$count;
            }
            oy199:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-07-14": '.$count;
            }
            oy200:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-07-15": '.$count;
            }
            oy201:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-07-16": '.$count;
            }
            oy202:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-07-17": '.$count;
            }
            oy203:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-07-18": '.$count;
            }
            oy204:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-07-19": '.$count;
            }
            oy205:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-07-20": '.$count;
            }
            oy206:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-07-21": '.$count;
            }
            oy207:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-07-22": '.$count;
            }
            oy208:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-07-23": '.$count;
            }
            oy209:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-07-24": '.$count;
            }
            oy210:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-07-25": '.$count;
            }
            oy211:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-07-26": '.$count;
            }
            oy212:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-07-27": '.$count;
            }
            oy213:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-07-28": '.$count;
            }
            oy214:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-07-29": '.$count;
            }
            oy215:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-07-30": '.$count;
            }
            oy216:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-07-31": '.$count;
            }
            oy217:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-08-01": '.$count;
            }
            oy218:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-08-02": '.$count;
            }
            oy219:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-08-03": '.$count;
            }
            oy220:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-08-04": '.$count;
            }
            oy221:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-08-05": '.$count;
            }
            oy222:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-08-06": '.$count;
            }
            oy223:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-08-07": '.$count;
            }
            oy224:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-08-08": '.$count;
            }
            oy225:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-08-09": '.$count;
            }
            oy226:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-08-10": '.$count;
            }
            oy227:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-08-11": '.$count;
            }
            oy228:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-08-12": '.$count;
            }
            oy229:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-08-13": '.$count;
            }
            oy230:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-08-14": '.$count;
            }
            oy231:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-08-15": '.$count;
            }
            oy232:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-08-16": '.$count;
            }
            oy233:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-08-17": '.$count;
            }
            oy234:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-08-18": '.$count;
            }
            oy235:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-08-19": '.$count;
            }
            oy236:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-08-20": '.$count;
            }
            oy237:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-08-21": '.$count;
            }
            oy238:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-08-22": '.$count;
            }
            oy239:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-08-23": '.$count;
            }
            oy240:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-08-24": '.$count;
            }
            oy241:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-08-25": '.$count;
            }
            oy242:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-08-26": '.$count;
            }
            oy243:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-08-27": '.$count;
            }
            oy244:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-08-28": '.$count;
            }
            oy245:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-08-29": '.$count;
            }
            oy246:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-08-30": '.$count;
            }
            oy247:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-08-31": '.$count;
            }
            oy248:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-09-01": '.$count;
            }
            oy249:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-09-02": '.$count;
            }
            oy250:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-09-03": '.$count;
            }
            oy251:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-09-04": '.$count;
            }
            oy252:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-09-05": '.$count;
            }
            oy253:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-09-06": '.$count;
            }
            oy254:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-09-07": '.$count;
            }
            oy255:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-09-08": '.$count;
            }
            oy256:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-09-09": '.$count;
            }
            oy257:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-09-10": '.$count;
            }
            oy258:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-09-11": '.$count;
            }
            oy259:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-09-12": '.$count;
            }
            oy260:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-09-13": '.$count;
            }
            oy261:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-09-14": '.$count;
            }
            oy262:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-09-15": '.$count;
            }
            oy263:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-09-16": '.$count;
            }
            oy264:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-09-17": '.$count;
            }
            oy265:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-09-18": '.$count;
            }
            oy266:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-09-19": '.$count;
            }
            oy267:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-09-20": '.$count;
            }
            oy268:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-09-21": '.$count;
            }
            oy269:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-09-22": '.$count;
            }
            oy270:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-09-23": '.$count;
            }
            oy271:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-09-24": '.$count;
            }
            oy272:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-09-25": '.$count;
            }
            oy273:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-09-26": '.$count;
            }
            oy274:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-09-27": '.$count;
            }
            oy275:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-09-28": '.$count;
            }
            oy276:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-09-29": '.$count;
            }
            oy277:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-09-30": '.$count;
            }
            oy278:
            ++$id;
            oy279:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-10-01": '.$count;
            }
            oy280:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-10-02": '.$count;
            }
            oy281:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-10-03": '.$count;
            }
            oy282:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-10-04": '.$count;
            }
            oy283:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-10-05": '.$count;
            }
            oy284:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-10-06": '.$count;
            }
            oy285:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-10-07": '.$count;
            }
            oy286:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-10-08": '.$count;
            }
            oy287:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-10-09": '.$count;
            }
            oy288:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-10-10": '.$count;
            }
            oy289:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-10-11": '.$count;
            }
            oy290:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-10-12": '.$count;
            }
            oy291:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-10-13": '.$count;
            }
            oy292:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-10-14": '.$count;
            }
            oy293:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-10-15": '.$count;
            }
            oy294:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-10-16": '.$count;
            }
            oy295:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-10-17": '.$count;
            }
            oy296:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-10-18": '.$count;
            }
            oy297:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-10-19": '.$count;
            }
            oy298:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-10-20": '.$count;
            }
            oy299:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-10-21": '.$count;
            }
            oy300:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-10-22": '.$count;
            }
            oy301:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-10-23": '.$count;
            }
            oy302:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-10-24": '.$count;
            }
            oy303:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-10-25": '.$count;
            }
            oy304:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-10-26": '.$count;
            }
            oy305:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-10-27": '.$count;
            }
            oy306:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-10-28": '.$count;
            }
            oy307:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-10-29": '.$count;
            }
            oy308:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-10-30": '.$count;
            }
            oy309:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-10-31": '.$count;
            }
            oy310:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-11-01": '.$count;
            }
            oy311:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-11-02": '.$count;
            }
            oy312:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-11-03": '.$count;
            }
            oy313:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-11-04": '.$count;
            }
            oy314:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-11-05": '.$count;
            }
            oy315:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-11-06": '.$count;
            }
            oy316:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-11-07": '.$count;
            }
            oy317:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-11-08": '.$count;
            }
            oy318:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-11-09": '.$count;
            }
            oy319:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-11-10": '.$count;
            }
            oy320:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-11-11": '.$count;
            }
            oy321:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-11-12": '.$count;
            }
            oy322:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-11-13": '.$count;
            }
            oy323:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-11-14": '.$count;
            }
            oy324:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-11-15": '.$count;
            }
            oy325:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-11-16": '.$count;
            }
            oy326:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-11-17": '.$count;
            }
            oy327:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-11-18": '.$count;
            }
            oy328:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-11-19": '.$count;
            }
            oy329:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-11-20": '.$count;
            }
            oy330:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-11-21": '.$count;
            }
            oy331:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-11-22": '.$count;
            }
            oy332:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-11-23": '.$count;
            }
            oy333:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-11-24": '.$count;
            }
            oy334:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-11-25": '.$count;
            }
            oy335:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-11-26": '.$count;
            }
            oy336:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-11-27": '.$count;
            }
            oy337:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-11-28": '.$count;
            }
            oy338:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-11-29": '.$count;
            }
            oy339:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-11-30": '.$count;
            }
            oy340:
            ++$id;
            oy341:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-12-01": '.$count;
            }
            oy342:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-12-02": '.$count;
            }
            oy343:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-12-03": '.$count;
            }
            oy344:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-12-04": '.$count;
            }
            oy345:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-12-05": '.$count;
            }
            oy346:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-12-06": '.$count;
            }
            oy347:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-12-07": '.$count;
            }
            oy348:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-12-08": '.$count;
            }
            oy349:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-12-09": '.$count;
            }
            oy350:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-12-10": '.$count;
            }
            oy351:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-12-11": '.$count;
            }
            oy352:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-12-12": '.$count;
            }
            oy353:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-12-13": '.$count;
            }
            oy354:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-12-14": '.$count;
            }
            oy355:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-12-15": '.$count;
            }
            oy356:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-12-16": '.$count;
            }
            oy357:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-12-17": '.$count;
            }
            oy358:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-12-18": '.$count;
            }
            oy359:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-12-19": '.$count;
            }
            oy360:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-12-20": '.$count;
            }
            oy361:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-12-21": '.$count;
            }
            oy362:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-12-22": '.$count;
            }
            oy363:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-12-23": '.$count;
            }
            oy364:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-12-24": '.$count;
            }
            oy365:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-12-25": '.$count;
            }
            oy366:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-12-26": '.$count;
            }
            oy367:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-12-27": '.$count;
            }
            oy368:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-12-28": '.$count;
            }
            oy369:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-12-29": '.$count;
            }
            oy370:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-12-30": '.$count;
            }
            oy371:
            if ($count = $counts[$id++]) {
                $j .= ",\n        \"".$year.'-12-31": '.$count;
            }

            oy372:
            if(++$year <= 2026) goto o2;
            
            $j .= "\n    }";
            \fwrite($fo, $j);
            $fu = false;
        }

        \fwrite($fo, "\n}");
        \fclose($fo);
        \exit();
    }
}