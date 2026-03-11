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
            $id = $partialId + ((($year-21) % 100) * 372);
o1:
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-01-01": '.$id;
                goto oy1;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-01-02": '.$id;
                goto oy2;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-01-03": '.$id;
                goto oy3;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-01-04": '.$id;
                goto oy4;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-01-05": '.$id;
                goto oy5;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-01-06": '.$id;
                goto oy6;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-01-07": '.$id;
                goto oy7;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-01-08": '.$id;
                goto oy8;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-01-09": '.$id;
                goto oy9;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-01-10": '.$id;
                goto oy10;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-01-11": '.$id;
                goto oy11;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-01-12": '.$id;
                goto oy12;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-01-13": '.$id;
                goto oy13;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-01-14": '.$id;
                goto oy14;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-01-15": '.$id;
                goto oy15;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-01-16": '.$id;
                goto oy16;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-01-17": '.$id;
                goto oy17;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-01-18": '.$id;
                goto oy18;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-01-19": '.$id;
                goto oy19;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-01-20": '.$id;
                goto oy20;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-01-21": '.$id;
                goto oy21;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-01-22": '.$id;
                goto oy22;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-01-23": '.$id;
                goto oy23;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-01-24": '.$id;
                goto oy24;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-01-25": '.$id;
                goto oy25;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-01-26": '.$id;
                goto oy26;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-01-27": '.$id;
                goto oy27;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-01-28": '.$id;
                goto oy28;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-01-29": '.$id;
                goto oy29;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-01-30": '.$id;
                goto oy30;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-01-31": '.$id;
                goto oy31;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-02-01": '.$id;
                goto oy32;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-02-02": '.$id;
                goto oy33;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-02-03": '.$id;
                goto oy34;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-02-04": '.$id;
                goto oy35;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-02-05": '.$id;
                goto oy36;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-02-06": '.$id;
                goto oy37;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-02-07": '.$id;
                goto oy38;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-02-08": '.$id;
                goto oy39;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-02-09": '.$id;
                goto oy40;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-02-10": '.$id;
                goto oy41;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-02-11": '.$id;
                goto oy42;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-02-12": '.$id;
                goto oy43;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-02-13": '.$id;
                goto oy44;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-02-14": '.$id;
                goto oy45;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-02-15": '.$id;
                goto oy46;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-02-16": '.$id;
                goto oy47;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-02-17": '.$id;
                goto oy48;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-02-18": '.$id;
                goto oy49;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-02-19": '.$id;
                goto oy50;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-02-20": '.$id;
                goto oy51;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-02-21": '.$id;
                goto oy52;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-02-22": '.$id;
                goto oy53;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-02-23": '.$id;
                goto oy54;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-02-24": '.$id;
                goto oy55;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-02-25": '.$id;
                goto oy56;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-02-26": '.$id;
                goto oy57;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-02-27": '.$id;
                goto oy58;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-02-28": '.$id;
                goto oy59;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-02-29": '.$id;
                goto oy60;
            }
            $id += 2;
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-03-01": '.$id;
                goto oy63;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-03-02": '.$id;
                goto oy64;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-03-03": '.$id;
                goto oy65;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-03-04": '.$id;
                goto oy66;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-03-05": '.$id;
                goto oy67;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-03-06": '.$id;
                goto oy68;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-03-07": '.$id;
                goto oy69;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-03-08": '.$id;
                goto oy70;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-03-09": '.$id;
                goto oy71;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-03-10": '.$id;
                goto oy72;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-03-11": '.$id;
                goto oy73;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-03-12": '.$id;
                goto oy74;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-03-13": '.$id;
                goto oy75;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-03-14": '.$id;
                goto oy76;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-03-15": '.$id;
                goto oy77;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-03-16": '.$id;
                goto oy78;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-03-17": '.$id;
                goto oy79;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-03-18": '.$id;
                goto oy80;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-03-19": '.$id;
                goto oy81;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-03-20": '.$id;
                goto oy82;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-03-21": '.$id;
                goto oy83;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-03-22": '.$id;
                goto oy84;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-03-23": '.$id;
                goto oy85;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-03-24": '.$id;
                goto oy86;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-03-25": '.$id;
                goto oy87;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-03-26": '.$id;
                goto oy88;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-03-27": '.$id;
                goto oy89;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-03-28": '.$id;
                goto oy90;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-03-29": '.$id;
                goto oy91;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-03-30": '.$id;
                goto oy92;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-03-31": '.$id;
                goto oy93;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-04-01": '.$id;
                goto oy94;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-04-02": '.$id;
                goto oy95;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-04-03": '.$id;
                goto oy96;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-04-04": '.$id;
                goto oy97;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-04-05": '.$id;
                goto oy98;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-04-06": '.$id;
                goto oy99;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-04-07": '.$id;
                goto oy100;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-04-08": '.$id;
                goto oy101;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-04-09": '.$id;
                goto oy102;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-04-10": '.$id;
                goto oy103;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-04-11": '.$id;
                goto oy104;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-04-12": '.$id;
                goto oy105;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-04-13": '.$id;
                goto oy106;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-04-14": '.$id;
                goto oy107;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-04-15": '.$id;
                goto oy108;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-04-16": '.$id;
                goto oy109;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-04-17": '.$id;
                goto oy110;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-04-18": '.$id;
                goto oy111;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-04-19": '.$id;
                goto oy112;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-04-20": '.$id;
                goto oy113;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-04-21": '.$id;
                goto oy114;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-04-22": '.$id;
                goto oy115;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-04-23": '.$id;
                goto oy116;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-04-24": '.$id;
                goto oy117;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-04-25": '.$id;
                goto oy118;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-04-26": '.$id;
                goto oy119;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-04-27": '.$id;
                goto oy120;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-04-28": '.$id;
                goto oy121;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-04-29": '.$id;
                goto oy122;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-04-30": '.$id;
                goto oy123;
            }
            ++$id;
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-05-01": '.$id;
                goto oy125;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-05-02": '.$id;
                goto oy126;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-05-03": '.$id;
                goto oy127;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-05-04": '.$id;
                goto oy128;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-05-05": '.$id;
                goto oy129;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-05-06": '.$id;
                goto oy130;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-05-07": '.$id;
                goto oy131;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-05-08": '.$id;
                goto oy132;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-05-09": '.$id;
                goto oy133;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-05-10": '.$id;
                goto oy134;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-05-11": '.$id;
                goto oy135;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-05-12": '.$id;
                goto oy136;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-05-13": '.$id;
                goto oy137;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-05-14": '.$id;
                goto oy138;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-05-15": '.$id;
                goto oy139;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-05-16": '.$id;
                goto oy140;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-05-17": '.$id;
                goto oy141;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-05-18": '.$id;
                goto oy142;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-05-19": '.$id;
                goto oy143;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-05-20": '.$id;
                goto oy144;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-05-21": '.$id;
                goto oy145;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-05-22": '.$id;
                goto oy146;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-05-23": '.$id;
                goto oy147;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-05-24": '.$id;
                goto oy148;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-05-25": '.$id;
                goto oy149;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-05-26": '.$id;
                goto oy150;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-05-27": '.$id;
                goto oy151;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-05-28": '.$id;
                goto oy152;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-05-29": '.$id;
                goto oy153;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-05-30": '.$id;
                goto oy154;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-05-31": '.$id;
                goto oy155;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-06-01": '.$id;
                goto oy156;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-06-02": '.$id;
                goto oy157;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-06-03": '.$id;
                goto oy158;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-06-04": '.$id;
                goto oy159;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-06-05": '.$id;
                goto oy160;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-06-06": '.$id;
                goto oy161;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-06-07": '.$id;
                goto oy162;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-06-08": '.$id;
                goto oy163;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-06-09": '.$id;
                goto oy164;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-06-10": '.$id;
                goto oy165;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-06-11": '.$id;
                goto oy166;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-06-12": '.$id;
                goto oy167;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-06-13": '.$id;
                goto oy168;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-06-14": '.$id;
                goto oy169;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-06-15": '.$id;
                goto oy170;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-06-16": '.$id;
                goto oy171;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-06-17": '.$id;
                goto oy172;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-06-18": '.$id;
                goto oy173;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-06-19": '.$id;
                goto oy174;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-06-20": '.$id;
                goto oy175;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-06-21": '.$id;
                goto oy176;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-06-22": '.$id;
                goto oy177;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-06-23": '.$id;
                goto oy178;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-06-24": '.$id;
                goto oy179;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-06-25": '.$id;
                goto oy180;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-06-26": '.$id;
                goto oy181;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-06-27": '.$id;
                goto oy182;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-06-28": '.$id;
                goto oy183;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-06-29": '.$id;
                goto oy184;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-06-30": '.$id;
                goto oy185;
            }
            ++$id;
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-07-01": '.$id;
                goto oy187;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-07-02": '.$id;
                goto oy188;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-07-03": '.$id;
                goto oy189;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-07-04": '.$id;
                goto oy190;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-07-05": '.$id;
                goto oy191;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-07-06": '.$id;
                goto oy192;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-07-07": '.$id;
                goto oy193;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-07-08": '.$id;
                goto oy194;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-07-09": '.$id;
                goto oy195;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-07-10": '.$id;
                goto oy196;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-07-11": '.$id;
                goto oy197;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-07-12": '.$id;
                goto oy198;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-07-13": '.$id;
                goto oy199;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-07-14": '.$id;
                goto oy200;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-07-15": '.$id;
                goto oy201;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-07-16": '.$id;
                goto oy202;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-07-17": '.$id;
                goto oy203;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-07-18": '.$id;
                goto oy204;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-07-19": '.$id;
                goto oy205;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-07-20": '.$id;
                goto oy206;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-07-21": '.$id;
                goto oy207;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-07-22": '.$id;
                goto oy208;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-07-23": '.$id;
                goto oy209;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-07-24": '.$id;
                goto oy210;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-07-25": '.$id;
                goto oy211;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-07-26": '.$id;
                goto oy212;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-07-27": '.$id;
                goto oy213;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-07-28": '.$id;
                goto oy214;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-07-29": '.$id;
                goto oy215;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-07-30": '.$id;
                goto oy216;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-07-31": '.$id;
                goto oy217;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-08-01": '.$id;
                goto oy218;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-08-02": '.$id;
                goto oy219;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-08-03": '.$id;
                goto oy220;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-08-04": '.$id;
                goto oy221;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-08-05": '.$id;
                goto oy222;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-08-06": '.$id;
                goto oy223;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-08-07": '.$id;
                goto oy224;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-08-08": '.$id;
                goto oy225;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-08-09": '.$id;
                goto oy226;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-08-10": '.$id;
                goto oy227;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-08-11": '.$id;
                goto oy228;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-08-12": '.$id;
                goto oy229;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-08-13": '.$id;
                goto oy230;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-08-14": '.$id;
                goto oy231;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-08-15": '.$id;
                goto oy232;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-08-16": '.$id;
                goto oy233;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-08-17": '.$id;
                goto oy234;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-08-18": '.$id;
                goto oy235;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-08-19": '.$id;
                goto oy236;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-08-20": '.$id;
                goto oy237;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-08-21": '.$id;
                goto oy238;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-08-22": '.$id;
                goto oy239;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-08-23": '.$id;
                goto oy240;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-08-24": '.$id;
                goto oy241;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-08-25": '.$id;
                goto oy242;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-08-26": '.$id;
                goto oy243;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-08-27": '.$id;
                goto oy244;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-08-28": '.$id;
                goto oy245;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-08-29": '.$id;
                goto oy246;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-08-30": '.$id;
                goto oy247;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-08-31": '.$id;
                goto oy248;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-09-01": '.$id;
                goto oy249;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-09-02": '.$id;
                goto oy250;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-09-03": '.$id;
                goto oy251;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-09-04": '.$id;
                goto oy252;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-09-05": '.$id;
                goto oy253;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-09-06": '.$id;
                goto oy254;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-09-07": '.$id;
                goto oy255;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-09-08": '.$id;
                goto oy256;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-09-09": '.$id;
                goto oy257;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-09-10": '.$id;
                goto oy258;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-09-11": '.$id;
                goto oy259;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-09-12": '.$id;
                goto oy260;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-09-13": '.$id;
                goto oy261;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-09-14": '.$id;
                goto oy262;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-09-15": '.$id;
                goto oy263;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-09-16": '.$id;
                goto oy264;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-09-17": '.$id;
                goto oy265;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-09-18": '.$id;
                goto oy266;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-09-19": '.$id;
                goto oy267;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-09-20": '.$id;
                goto oy268;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-09-21": '.$id;
                goto oy269;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-09-22": '.$id;
                goto oy270;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-09-23": '.$id;
                goto oy271;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-09-24": '.$id;
                goto oy272;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-09-25": '.$id;
                goto oy273;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-09-26": '.$id;
                goto oy274;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-09-27": '.$id;
                goto oy275;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-09-28": '.$id;
                goto oy276;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-09-29": '.$id;
                goto oy277;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-09-30": '.$id;
                goto oy278;
            }
            ++$id;
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-10-01": '.$id;
                goto oy280;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-10-02": '.$id;
                goto oy281;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-10-03": '.$id;
                goto oy282;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-10-04": '.$id;
                goto oy283;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-10-05": '.$id;
                goto oy284;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-10-06": '.$id;
                goto oy285;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-10-07": '.$id;
                goto oy286;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-10-08": '.$id;
                goto oy287;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-10-09": '.$id;
                goto oy288;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-10-10": '.$id;
                goto oy289;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-10-11": '.$id;
                goto oy290;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-10-12": '.$id;
                goto oy291;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-10-13": '.$id;
                goto oy292;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-10-14": '.$id;
                goto oy293;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-10-15": '.$id;
                goto oy294;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-10-16": '.$id;
                goto oy295;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-10-17": '.$id;
                goto oy296;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-10-18": '.$id;
                goto oy297;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-10-19": '.$id;
                goto oy298;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-10-20": '.$id;
                goto oy299;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-10-21": '.$id;
                goto oy300;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-10-22": '.$id;
                goto oy301;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-10-23": '.$id;
                goto oy302;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-10-24": '.$id;
                goto oy303;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-10-25": '.$id;
                goto oy304;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-10-26": '.$id;
                goto oy305;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-10-27": '.$id;
                goto oy306;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-10-28": '.$id;
                goto oy307;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-10-29": '.$id;
                goto oy308;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-10-30": '.$id;
                goto oy309;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-10-31": '.$id;
                goto oy310;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-11-01": '.$id;
                goto oy311;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-11-02": '.$id;
                goto oy312;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-11-03": '.$id;
                goto oy313;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-11-04": '.$id;
                goto oy314;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-11-05": '.$id;
                goto oy315;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-11-06": '.$id;
                goto oy316;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-11-07": '.$id;
                goto oy317;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-11-08": '.$id;
                goto oy318;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-11-09": '.$id;
                goto oy319;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-11-10": '.$id;
                goto oy320;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-11-11": '.$id;
                goto oy321;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-11-12": '.$id;
                goto oy322;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-11-13": '.$id;
                goto oy323;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-11-14": '.$id;
                goto oy324;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-11-15": '.$id;
                goto oy325;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-11-16": '.$id;
                goto oy326;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-11-17": '.$id;
                goto oy327;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-11-18": '.$id;
                goto oy328;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-11-19": '.$id;
                goto oy329;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-11-20": '.$id;
                goto oy330;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-11-21": '.$id;
                goto oy331;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-11-22": '.$id;
                goto oy332;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-11-23": '.$id;
                goto oy333;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-11-24": '.$id;
                goto oy334;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-11-25": '.$id;
                goto oy335;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-11-26": '.$id;
                goto oy336;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-11-27": '.$id;
                goto oy337;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-11-28": '.$id;
                goto oy338;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-11-29": '.$id;
                goto oy339;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-11-30": '.$id;
                goto oy340;
            }
            ++$id;
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-12-01": '.$id;
                goto oy342;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-12-02": '.$id;
                goto oy343;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-12-03": '.$id;
                goto oy344;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-12-04": '.$id;
                goto oy345;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-12-05": '.$id;
                goto oy346;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-12-06": '.$id;
                goto oy347;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-12-07": '.$id;
                goto oy348;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-12-08": '.$id;
                goto oy349;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-12-09": '.$id;
                goto oy350;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-12-10": '.$id;
                goto oy351;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-12-11": '.$id;
                goto oy352;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-12-12": '.$id;
                goto oy353;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-12-13": '.$id;
                goto oy354;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-12-14": '.$id;
                goto oy355;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-12-15": '.$id;
                goto oy356;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-12-16": '.$id;
                goto oy357;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-12-17": '.$id;
                goto oy358;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-12-18": '.$id;
                goto oy359;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-12-19": '.$id;
                goto oy360;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-12-20": '.$id;
                goto oy361;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-12-21": '.$id;
                goto oy362;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-12-22": '.$id;
                goto oy363;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-12-23": '.$id;
                goto oy364;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-12-24": '.$id;
                goto oy365;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-12-25": '.$id;
                goto oy366;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-12-26": '.$id;
                goto oy367;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-12-27": '.$id;
                goto oy368;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-12-28": '.$id;
                goto oy369;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-12-29": '.$id;
                goto oy370;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-12-30": '.$id;
                goto oy371;
            }
            if ($id = $count[$id++]) {
                $j .= '        "'.$year.'-12-31": '.$id;
                goto oy372;
            }

            ++$year;
            if($year <= 2026) goto o1;
            continue;

o2:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-01-01": '.$id;
            }
            oy1:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-01-02": '.$id;
            }
            oy2:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-01-03": '.$id;
            }
            oy3:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-01-04": '.$id;
            }
            oy4:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-01-05": '.$id;
            }
            oy5:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-01-06": '.$id;
            }
            oy6:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-01-07": '.$id;
            }
            oy7:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-01-08": '.$id;
            }
            oy8:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-01-09": '.$id;
            }
            oy9:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-01-10": '.$id;
            }
            oy10:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-01-11": '.$id;
            }
            oy11:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-01-12": '.$id;
            }
            oy12:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-01-13": '.$id;
            }
            oy13:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-01-14": '.$id;
            }
            oy14:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-01-15": '.$id;
            }
            oy15:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-01-16": '.$id;
            }
            oy16:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-01-17": '.$id;
            }
            oy17:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-01-18": '.$id;
            }
            oy18:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-01-19": '.$id;
            }
            oy19:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-01-20": '.$id;
            }
            oy20:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-01-21": '.$id;
            }
            oy21:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-01-22": '.$id;
            }
            oy22:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-01-23": '.$id;
            }
            oy23:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-01-24": '.$id;
            }
            oy24:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-01-25": '.$id;
            }
            oy25:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-01-26": '.$id;
            }
            oy26:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-01-27": '.$id;
            }
            oy27:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-01-28": '.$id;
            }
            oy28:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-01-29": '.$id;
            }
            oy29:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-01-30": '.$id;
            }
            oy30:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-01-31": '.$id;
            }
            oy31:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-02-01": '.$id;
            }
            oy32:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-02-02": '.$id;
            }
            oy33:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-02-03": '.$id;
            }
            oy34:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-02-04": '.$id;
            }
            oy35:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-02-05": '.$id;
            }
            oy36:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-02-06": '.$id;
            }
            oy37:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-02-07": '.$id;
            }
            oy38:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-02-08": '.$id;
            }
            oy39:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-02-09": '.$id;
            }
            oy40:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-02-10": '.$id;
            }
            oy41:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-02-11": '.$id;
            }
            oy42:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-02-12": '.$id;
            }
            oy43:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-02-13": '.$id;
            }
            oy44:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-02-14": '.$id;
            }
            oy45:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-02-15": '.$id;
            }
            oy46:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-02-16": '.$id;
            }
            oy47:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-02-17": '.$id;
            }
            oy48:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-02-18": '.$id;
            }
            oy49:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-02-19": '.$id;
            }
            oy50:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-02-20": '.$id;
            }
            oy51:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-02-21": '.$id;
            }
            oy52:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-02-22": '.$id;
            }
            oy53:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-02-23": '.$id;
            }
            oy54:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-02-24": '.$id;
            }
            oy55:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-02-25": '.$id;
            }
            oy56:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-02-26": '.$id;
            }
            oy57:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-02-27": '.$id;
            }
            oy58:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-02-28": '.$id;
            }
            oy59:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-02-29": '.$id;
            }
            oy60:
            $id += 2;
            oy62:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-03-01": '.$id;
            }
            oy63:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-03-02": '.$id;
            }
            oy64:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-03-03": '.$id;
            }
            oy65:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-03-04": '.$id;
            }
            oy66:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-03-05": '.$id;
            }
            oy67:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-03-06": '.$id;
            }
            oy68:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-03-07": '.$id;
            }
            oy69:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-03-08": '.$id;
            }
            oy70:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-03-09": '.$id;
            }
            oy71:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-03-10": '.$id;
            }
            oy72:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-03-11": '.$id;
            }
            oy73:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-03-12": '.$id;
            }
            oy74:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-03-13": '.$id;
            }
            oy75:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-03-14": '.$id;
            }
            oy76:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-03-15": '.$id;
            }
            oy77:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-03-16": '.$id;
            }
            oy78:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-03-17": '.$id;
            }
            oy79:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-03-18": '.$id;
            }
            oy80:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-03-19": '.$id;
            }
            oy81:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-03-20": '.$id;
            }
            oy82:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-03-21": '.$id;
            }
            oy83:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-03-22": '.$id;
            }
            oy84:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-03-23": '.$id;
            }
            oy85:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-03-24": '.$id;
            }
            oy86:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-03-25": '.$id;
            }
            oy87:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-03-26": '.$id;
            }
            oy88:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-03-27": '.$id;
            }
            oy89:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-03-28": '.$id;
            }
            oy90:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-03-29": '.$id;
            }
            oy91:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-03-30": '.$id;
            }
            oy92:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-03-31": '.$id;
            }
            oy93:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-04-01": '.$id;
            }
            oy94:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-04-02": '.$id;
            }
            oy95:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-04-03": '.$id;
            }
            oy96:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-04-04": '.$id;
            }
            oy97:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-04-05": '.$id;
            }
            oy98:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-04-06": '.$id;
            }
            oy99:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-04-07": '.$id;
            }
            oy100:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-04-08": '.$id;
            }
            oy101:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-04-09": '.$id;
            }
            oy102:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-04-10": '.$id;
            }
            oy103:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-04-11": '.$id;
            }
            oy104:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-04-12": '.$id;
            }
            oy105:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-04-13": '.$id;
            }
            oy106:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-04-14": '.$id;
            }
            oy107:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-04-15": '.$id;
            }
            oy108:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-04-16": '.$id;
            }
            oy109:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-04-17": '.$id;
            }
            oy110:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-04-18": '.$id;
            }
            oy111:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-04-19": '.$id;
            }
            oy112:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-04-20": '.$id;
            }
            oy113:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-04-21": '.$id;
            }
            oy114:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-04-22": '.$id;
            }
            oy115:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-04-23": '.$id;
            }
            oy116:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-04-24": '.$id;
            }
            oy117:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-04-25": '.$id;
            }
            oy118:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-04-26": '.$id;
            }
            oy119:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-04-27": '.$id;
            }
            oy120:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-04-28": '.$id;
            }
            oy121:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-04-29": '.$id;
            }
            oy122:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-04-30": '.$id;
            }
            oy123:
            ++$id;
            oy124:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-05-01": '.$id;
            }
            oy125:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-05-02": '.$id;
            }
            oy126:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-05-03": '.$id;
            }
            oy127:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-05-04": '.$id;
            }
            oy128:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-05-05": '.$id;
            }
            oy129:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-05-06": '.$id;
            }
            oy130:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-05-07": '.$id;
            }
            oy131:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-05-08": '.$id;
            }
            oy132:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-05-09": '.$id;
            }
            oy133:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-05-10": '.$id;
            }
            oy134:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-05-11": '.$id;
            }
            oy135:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-05-12": '.$id;
            }
            oy136:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-05-13": '.$id;
            }
            oy137:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-05-14": '.$id;
            }
            oy138:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-05-15": '.$id;
            }
            oy139:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-05-16": '.$id;
            }
            oy140:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-05-17": '.$id;
            }
            oy141:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-05-18": '.$id;
            }
            oy142:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-05-19": '.$id;
            }
            oy143:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-05-20": '.$id;
            }
            oy144:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-05-21": '.$id;
            }
            oy145:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-05-22": '.$id;
            }
            oy146:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-05-23": '.$id;
            }
            oy147:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-05-24": '.$id;
            }
            oy148:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-05-25": '.$id;
            }
            oy149:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-05-26": '.$id;
            }
            oy150:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-05-27": '.$id;
            }
            oy151:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-05-28": '.$id;
            }
            oy152:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-05-29": '.$id;
            }
            oy153:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-05-30": '.$id;
            }
            oy154:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-05-31": '.$id;
            }
            oy155:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-06-01": '.$id;
            }
            oy156:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-06-02": '.$id;
            }
            oy157:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-06-03": '.$id;
            }
            oy158:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-06-04": '.$id;
            }
            oy159:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-06-05": '.$id;
            }
            oy160:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-06-06": '.$id;
            }
            oy161:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-06-07": '.$id;
            }
            oy162:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-06-08": '.$id;
            }
            oy163:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-06-09": '.$id;
            }
            oy164:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-06-10": '.$id;
            }
            oy165:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-06-11": '.$id;
            }
            oy166:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-06-12": '.$id;
            }
            oy167:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-06-13": '.$id;
            }
            oy168:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-06-14": '.$id;
            }
            oy169:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-06-15": '.$id;
            }
            oy170:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-06-16": '.$id;
            }
            oy171:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-06-17": '.$id;
            }
            oy172:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-06-18": '.$id;
            }
            oy173:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-06-19": '.$id;
            }
            oy174:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-06-20": '.$id;
            }
            oy175:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-06-21": '.$id;
            }
            oy176:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-06-22": '.$id;
            }
            oy177:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-06-23": '.$id;
            }
            oy178:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-06-24": '.$id;
            }
            oy179:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-06-25": '.$id;
            }
            oy180:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-06-26": '.$id;
            }
            oy181:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-06-27": '.$id;
            }
            oy182:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-06-28": '.$id;
            }
            oy183:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-06-29": '.$id;
            }
            oy184:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-06-30": '.$id;
            }
            oy185:
            ++$id;
            oy186:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-07-01": '.$id;
            }
            oy187:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-07-02": '.$id;
            }
            oy188:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-07-03": '.$id;
            }
            oy189:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-07-04": '.$id;
            }
            oy190:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-07-05": '.$id;
            }
            oy191:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-07-06": '.$id;
            }
            oy192:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-07-07": '.$id;
            }
            oy193:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-07-08": '.$id;
            }
            oy194:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-07-09": '.$id;
            }
            oy195:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-07-10": '.$id;
            }
            oy196:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-07-11": '.$id;
            }
            oy197:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-07-12": '.$id;
            }
            oy198:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-07-13": '.$id;
            }
            oy199:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-07-14": '.$id;
            }
            oy200:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-07-15": '.$id;
            }
            oy201:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-07-16": '.$id;
            }
            oy202:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-07-17": '.$id;
            }
            oy203:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-07-18": '.$id;
            }
            oy204:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-07-19": '.$id;
            }
            oy205:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-07-20": '.$id;
            }
            oy206:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-07-21": '.$id;
            }
            oy207:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-07-22": '.$id;
            }
            oy208:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-07-23": '.$id;
            }
            oy209:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-07-24": '.$id;
            }
            oy210:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-07-25": '.$id;
            }
            oy211:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-07-26": '.$id;
            }
            oy212:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-07-27": '.$id;
            }
            oy213:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-07-28": '.$id;
            }
            oy214:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-07-29": '.$id;
            }
            oy215:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-07-30": '.$id;
            }
            oy216:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-07-31": '.$id;
            }
            oy217:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-08-01": '.$id;
            }
            oy218:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-08-02": '.$id;
            }
            oy219:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-08-03": '.$id;
            }
            oy220:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-08-04": '.$id;
            }
            oy221:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-08-05": '.$id;
            }
            oy222:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-08-06": '.$id;
            }
            oy223:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-08-07": '.$id;
            }
            oy224:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-08-08": '.$id;
            }
            oy225:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-08-09": '.$id;
            }
            oy226:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-08-10": '.$id;
            }
            oy227:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-08-11": '.$id;
            }
            oy228:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-08-12": '.$id;
            }
            oy229:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-08-13": '.$id;
            }
            oy230:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-08-14": '.$id;
            }
            oy231:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-08-15": '.$id;
            }
            oy232:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-08-16": '.$id;
            }
            oy233:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-08-17": '.$id;
            }
            oy234:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-08-18": '.$id;
            }
            oy235:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-08-19": '.$id;
            }
            oy236:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-08-20": '.$id;
            }
            oy237:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-08-21": '.$id;
            }
            oy238:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-08-22": '.$id;
            }
            oy239:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-08-23": '.$id;
            }
            oy240:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-08-24": '.$id;
            }
            oy241:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-08-25": '.$id;
            }
            oy242:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-08-26": '.$id;
            }
            oy243:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-08-27": '.$id;
            }
            oy244:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-08-28": '.$id;
            }
            oy245:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-08-29": '.$id;
            }
            oy246:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-08-30": '.$id;
            }
            oy247:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-08-31": '.$id;
            }
            oy248:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-09-01": '.$id;
            }
            oy249:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-09-02": '.$id;
            }
            oy250:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-09-03": '.$id;
            }
            oy251:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-09-04": '.$id;
            }
            oy252:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-09-05": '.$id;
            }
            oy253:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-09-06": '.$id;
            }
            oy254:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-09-07": '.$id;
            }
            oy255:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-09-08": '.$id;
            }
            oy256:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-09-09": '.$id;
            }
            oy257:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-09-10": '.$id;
            }
            oy258:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-09-11": '.$id;
            }
            oy259:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-09-12": '.$id;
            }
            oy260:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-09-13": '.$id;
            }
            oy261:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-09-14": '.$id;
            }
            oy262:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-09-15": '.$id;
            }
            oy263:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-09-16": '.$id;
            }
            oy264:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-09-17": '.$id;
            }
            oy265:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-09-18": '.$id;
            }
            oy266:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-09-19": '.$id;
            }
            oy267:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-09-20": '.$id;
            }
            oy268:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-09-21": '.$id;
            }
            oy269:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-09-22": '.$id;
            }
            oy270:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-09-23": '.$id;
            }
            oy271:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-09-24": '.$id;
            }
            oy272:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-09-25": '.$id;
            }
            oy273:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-09-26": '.$id;
            }
            oy274:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-09-27": '.$id;
            }
            oy275:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-09-28": '.$id;
            }
            oy276:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-09-29": '.$id;
            }
            oy277:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-09-30": '.$id;
            }
            oy278:
            ++$id;
            oy279:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-10-01": '.$id;
            }
            oy280:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-10-02": '.$id;
            }
            oy281:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-10-03": '.$id;
            }
            oy282:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-10-04": '.$id;
            }
            oy283:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-10-05": '.$id;
            }
            oy284:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-10-06": '.$id;
            }
            oy285:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-10-07": '.$id;
            }
            oy286:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-10-08": '.$id;
            }
            oy287:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-10-09": '.$id;
            }
            oy288:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-10-10": '.$id;
            }
            oy289:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-10-11": '.$id;
            }
            oy290:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-10-12": '.$id;
            }
            oy291:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-10-13": '.$id;
            }
            oy292:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-10-14": '.$id;
            }
            oy293:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-10-15": '.$id;
            }
            oy294:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-10-16": '.$id;
            }
            oy295:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-10-17": '.$id;
            }
            oy296:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-10-18": '.$id;
            }
            oy297:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-10-19": '.$id;
            }
            oy298:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-10-20": '.$id;
            }
            oy299:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-10-21": '.$id;
            }
            oy300:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-10-22": '.$id;
            }
            oy301:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-10-23": '.$id;
            }
            oy302:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-10-24": '.$id;
            }
            oy303:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-10-25": '.$id;
            }
            oy304:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-10-26": '.$id;
            }
            oy305:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-10-27": '.$id;
            }
            oy306:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-10-28": '.$id;
            }
            oy307:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-10-29": '.$id;
            }
            oy308:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-10-30": '.$id;
            }
            oy309:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-10-31": '.$id;
            }
            oy310:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-11-01": '.$id;
            }
            oy311:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-11-02": '.$id;
            }
            oy312:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-11-03": '.$id;
            }
            oy313:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-11-04": '.$id;
            }
            oy314:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-11-05": '.$id;
            }
            oy315:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-11-06": '.$id;
            }
            oy316:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-11-07": '.$id;
            }
            oy317:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-11-08": '.$id;
            }
            oy318:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-11-09": '.$id;
            }
            oy319:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-11-10": '.$id;
            }
            oy320:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-11-11": '.$id;
            }
            oy321:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-11-12": '.$id;
            }
            oy322:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-11-13": '.$id;
            }
            oy323:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-11-14": '.$id;
            }
            oy324:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-11-15": '.$id;
            }
            oy325:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-11-16": '.$id;
            }
            oy326:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-11-17": '.$id;
            }
            oy327:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-11-18": '.$id;
            }
            oy328:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-11-19": '.$id;
            }
            oy329:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-11-20": '.$id;
            }
            oy330:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-11-21": '.$id;
            }
            oy331:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-11-22": '.$id;
            }
            oy332:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-11-23": '.$id;
            }
            oy333:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-11-24": '.$id;
            }
            oy334:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-11-25": '.$id;
            }
            oy335:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-11-26": '.$id;
            }
            oy336:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-11-27": '.$id;
            }
            oy337:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-11-28": '.$id;
            }
            oy338:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-11-29": '.$id;
            }
            oy339:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-11-30": '.$id;
            }
            oy340:
            ++$id;
            oy341:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-12-01": '.$id;
            }
            oy342:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-12-02": '.$id;
            }
            oy343:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-12-03": '.$id;
            }
            oy344:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-12-04": '.$id;
            }
            oy345:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-12-05": '.$id;
            }
            oy346:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-12-06": '.$id;
            }
            oy347:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-12-07": '.$id;
            }
            oy348:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-12-08": '.$id;
            }
            oy349:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-12-09": '.$id;
            }
            oy350:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-12-10": '.$id;
            }
            oy351:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-12-11": '.$id;
            }
            oy352:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-12-12": '.$id;
            }
            oy353:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-12-13": '.$id;
            }
            oy354:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-12-14": '.$id;
            }
            oy355:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-12-15": '.$id;
            }
            oy356:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-12-16": '.$id;
            }
            oy357:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-12-17": '.$id;
            }
            oy358:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-12-18": '.$id;
            }
            oy359:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-12-19": '.$id;
            }
            oy360:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-12-20": '.$id;
            }
            oy361:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-12-21": '.$id;
            }
            oy362:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-12-22": '.$id;
            }
            oy363:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-12-23": '.$id;
            }
            oy364:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-12-24": '.$id;
            }
            oy365:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-12-25": '.$id;
            }
            oy366:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-12-26": '.$id;
            }
            oy367:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-12-27": '.$id;
            }
            oy368:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-12-28": '.$id;
            }
            oy369:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-12-29": '.$id;
            }
            oy370:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-12-30": '.$id;
            }
            oy371:
            if ($id = $count[$id++]) {
                $j .= ",\n        \"".$year.'-12-31": '.$id;
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