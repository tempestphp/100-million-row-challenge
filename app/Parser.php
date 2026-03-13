<?php

namespace App;

use function array_fill;
use function fclose;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function str_replace;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use const SEEK_CUR;
use const SEEK_END;

final class Parser
{
    public static function parse($inputPath, $outputPath)
    {
        self::lusail($inputPath, $outputPath);
    }

    /**
     * Lusail - The World Cup Final. Qatar 2022.
     * Argentina 3 (4) - France 3 (2)
     */
    public static function lusail($inputPath, $outputPath)
    {
        gc_disable();

        // Scaloni sets up the fixture schedule
        $scaloni = [];
        $fixtures = [];
        $matches = 0;
        for ($y = 1; $y <= 6; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => $y === 4 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $mStr = ($m < 10 ? '0' : '') . $m;
                $ymStr = $y . '-' . $mStr . '-';
                for ($d = 1; $d <= $maxD; $d++) {
                    $key = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $scaloni[$key] = $matches;
                    $fixtures[$matches] = '202' . $key;
                    $matches++;
                }
            }
        }

        // Dibu opens the goal and reads the header
        $dibu = fopen($inputPath, 'rb');
        stream_set_read_buffer($dibu, 0);
        $header = fread($dibu, 181000);

        $pitch = [];
        $squad = 0;
        $pos = 0;
        $headerEnd = strrpos($header, "\n") ?: 0;
        $called = [];

        while ($pos < $headerEnd && $squad < 268) {
            $nl = strpos($header, "\n", $pos + 52);
            if ($nl === false) {
                break;
            }

            $player = substr($header, $pos + 25, $nl - $pos - 51);
            if (!isset($called[$player])) {
                $pitch[$squad] = $player;
                $called[$player] = $squad * $matches;
                $squad++;
            }

            $pos = $nl + 1;
        }
        unset($header, $called);

        // Cuti Romero marks the minimum unique distance to identify each player
        $shirt = 'https://stitcher.io/blog/';
        $cuti = 1;
        while (true) {
            $marks = [];
            $s = 0;
            while ($s < $squad) {
                $number = substr($shirt . $pitch[$s], -$cuti);
                if (isset($marks[$number])) {
                    $cuti++;
                    continue 2;
                }
                $marks[$number] = true;
                $s++;
            }
            break;
        }

        // Messi crafts the play: packed lookup lineLen << shift | baseIndex
        $messi = 20;
        $dePaul = (1 << $messi) - 1;
        $mbappe = 0;
        $enzo = [];
        for ($s = 0; $s < $squad; $s++) {
            $molina = strlen($pitch[$s]) + 52;
            if ($molina > $mbappe) {
                $mbappe = $molina;
            }
            $enzo[substr($shirt . $pitch[$s], -$cuti)] = ($molina << $messi) | ($s * $matches);
        }
        $macAllister = 26 + $cuti;
        $alvarez = 22;
        $montiel = 7;
        $otamendi = ($mbappe * 10) + $macAllister;

        $goals = $squad * $matches;

        // Dibu measures the pitch
        fseek($dibu, 0, SEEK_END);
        $remaining = ftell($dibu);
        fseek($dibu, 0);

        // Kickoff - reverse-scan hot loop
        $scoreboard = array_fill(0, $goals, 0);

        while ($remaining > 0) {
            $toRead = $remaining > 1_048_576 ? 1_048_576 : $remaining;
            $play = fread($dibu, $toRead);
            $length = strlen($play);
            $remaining -= $length;

            $whistle = strrpos($play, "\n");
            if ($whistle === false) {
                break;
            }

            $offside = $length - $whistle - 1;
            if ($offside > 0) {
                fseek($dibu, -$offside, SEEK_CUR);
                $remaining += $offside;
            }

            $i = $whistle;

            // 10 unrolled - like the 10 on the field
            while ($i > $otamendi) {
                $v = $enzo[substr($play, $i - $macAllister, $cuti)];
                $scoreboard[($v & $dePaul) + $scaloni[substr($play, $i - $alvarez, $montiel)]]++;
                $i -= $v >> $messi;

                $v = $enzo[substr($play, $i - $macAllister, $cuti)];
                $scoreboard[($v & $dePaul) + $scaloni[substr($play, $i - $alvarez, $montiel)]]++;
                $i -= $v >> $messi;

                $v = $enzo[substr($play, $i - $macAllister, $cuti)];
                $scoreboard[($v & $dePaul) + $scaloni[substr($play, $i - $alvarez, $montiel)]]++;
                $i -= $v >> $messi;

                $v = $enzo[substr($play, $i - $macAllister, $cuti)];
                $scoreboard[($v & $dePaul) + $scaloni[substr($play, $i - $alvarez, $montiel)]]++;
                $i -= $v >> $messi;

                $v = $enzo[substr($play, $i - $macAllister, $cuti)];
                $scoreboard[($v & $dePaul) + $scaloni[substr($play, $i - $alvarez, $montiel)]]++;
                $i -= $v >> $messi;

                $v = $enzo[substr($play, $i - $macAllister, $cuti)];
                $scoreboard[($v & $dePaul) + $scaloni[substr($play, $i - $alvarez, $montiel)]]++;
                $i -= $v >> $messi;

                $v = $enzo[substr($play, $i - $macAllister, $cuti)];
                $scoreboard[($v & $dePaul) + $scaloni[substr($play, $i - $alvarez, $montiel)]]++;
                $i -= $v >> $messi;

                $v = $enzo[substr($play, $i - $macAllister, $cuti)];
                $scoreboard[($v & $dePaul) + $scaloni[substr($play, $i - $alvarez, $montiel)]]++;
                $i -= $v >> $messi;

                $v = $enzo[substr($play, $i - $macAllister, $cuti)];
                $scoreboard[($v & $dePaul) + $scaloni[substr($play, $i - $alvarez, $montiel)]]++;
                $i -= $v >> $messi;

                $v = $enzo[substr($play, $i - $macAllister, $cuti)];
                $scoreboard[($v & $dePaul) + $scaloni[substr($play, $i - $alvarez, $montiel)]]++;
                $i -= $v >> $messi;
            }

            // Stoppage time
            while ($i >= $macAllister) {
                $v = $enzo[substr($play, $i - $macAllister, $cuti)];
                $scoreboard[($v & $dePaul) + $scaloni[substr($play, $i - $alvarez, $montiel)]]++;
                $i -= $v >> $messi;
            }
        }

        fclose($dibu);

        // Montiel slots it home and we write the JSON - WORLD CHAMPIONS
        $trophy = fopen($outputPath, 'wb');
        stream_set_write_buffer($trophy, 1_048_576);
        fwrite($trophy, '{');

        $dates = [];
        for ($d = 0; $d < $matches; $d++) {
            $dates[$d] = '        "' . $fixtures[$d] . '": ';
        }

        $starters = [];
        for ($s = 0; $s < $squad; $s++) {
            $starters[$s] = '"\/blog\/' . str_replace('/', '\/', $pitch[$s]) . '": {';
        }

        $lap = "\n    ";
        $base = 0;
        for ($s = 0; $s < $squad; $s++) {
            $d = 0;
            $idx = $base;
            while ($d < $matches && $scoreboard[$idx] === 0) {
                $d++;
                $idx++;
            }

            if ($d === $matches) {
                $base += $matches;
                continue;
            }

            $buf = $lap . $starters[$s] . "\n" . $dates[$d] . $scoreboard[$idx];
            $lap = ",\n    ";
            $d++;
            while ($d < $matches) {
                $idx++;
                if ($scoreboard[$idx] !== 0) {
                    $buf .= ",\n" . $dates[$d] . $scoreboard[$idx];
                }
                $d++;
            }

            $buf .= "\n    }";
            fwrite($trophy, $buf);
            $base += $matches;
        }

        fwrite($trophy, "\n}");
        fclose($trophy);
    }
}
