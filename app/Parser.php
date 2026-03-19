<?php

namespace App;

ini_set('memory_limit', '-1');

use function gc_disable;
use function fopen;
use function stream_set_read_buffer;
use function feof;
use function fread;
use function strpos;
use function substr;
use function fclose;
use function str_replace;
use function fwrite;

final class Parser
{
    CONST CHUNK_SIZE = 1048576; // 1 MB

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $output = [];
        $dates = [];
        for ($y = 2020; $y <= 2026; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $days = match ($m) {
                    2 => ($y === 2020) || ($y === 2024) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                for ($d = 1; $d <= $days; $d++) {
                    $mStr   = $m < 10 ? "0{$m}" : $m;
                    $dStr   = $d < 10 ? "0{$d}" : $d;
                    $date = $y . '-' . $mStr . '-' . $dStr;
                    $dates[$date] = 0;

                }
            }
        }
        unset($y, $m, $d, $date);

        $fileHandle = fopen($inputPath, 'rb');

        stream_set_read_buffer($fileHandle, 0);

        $leftover = '';

        while(!feof($fileHandle)) {
            $chunk = $leftover . fread($fileHandle, self::CHUNK_SIZE);
            $chunkLen = strlen($chunk);
            $pos = 0;

            while($pos < $chunkLen) {
                $commaPos = strpos($chunk, ',', $pos);
                if($commaPos === false) {
                    break;
                }
                // newline should always be 26 characters after the comma
                if($commaPos + 26 >= $chunkLen) {
                    break;
                }


                $path = substr($chunk, $pos + 19, $commaPos - ($pos + 19));
                $date = substr($chunk, $commaPos + 1, 10);

                if(!isset($output[$path])) {
                    $output[$path] = $dates;
                }

                $output[$path][$date]++;

                $pos =  $commaPos + 27;

            }

            $leftover = substr($chunk, $pos);
        }

        fclose($fileHandle);
        unset($fileHandle, $dates, $chunk, $chunkLen, $pos, $commaPos, $path, $date, $leftover);


        $json ="{\n";
        $s = "";
        foreach($output as $path =>$dates) {
            $json .= $s . '    "' . str_replace('/', '\\/', $path) . '"' . ": {\n";
            $sd = "";
            foreach($dates as $date => $count) {
                if($count === 0) continue;
                $json .= $sd . '        "' . $date . '": ' . $count;
                $sd = ",\n";
            }
            $json .= "\n    }";
            $s = ",\n";
        }
        $json .= "\n}";

        $fileHandle = fopen($outputPath, 'wb');
        fwrite($fileHandle, $json);
        fclose($fileHandle);
        
    }
}