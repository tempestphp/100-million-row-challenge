<?php

namespace App;

use Exception;
use SplFileObject;
use App\Commands\Visit;

final class Parser
{
    static $READ_CHUNK = 500_000;
    static $CORES = 8;

    static public function partParse(string $inputPath, int $start, int $length, $dates, $paths, $fullCount) {
        $left = "";
        $read = 0;

        $output = \str_repeat(\chr(0), $fullCount);

        $next = [];
        for($i=0; $i!=100;$i++) {
            $next[\chr($i)] = \chr($i+1);
        }

        $file = \fopen($inputPath, "r");
        \stream_set_read_buffer($file, 0);
        \fseek($file, $start);

        $order = [];
        $chunks = 0;
        while (!\feof($file) && $read < $length) {
            $lenAsked = $read + Parser::$READ_CHUNK >= $length ? $length - $read : Parser::$READ_CHUNK;
            $buffer = \fread($file, $lenAsked);

            if(\substr($buffer, -1) != \PHP_EOL) {
                $extra = \fgets($file);
                $lenAsked += \strlen($extra);
                $buffer .= $extra;
            }

            $lenAsked -= 10;
            $lenAskedBatch = $lenAsked - 2500;

            $nextPos = -1;
            $pos = -1;
            if($start == 0 && $chunks++ < 1) {
                while($nextPos < $lenAskedBatch) {
                    $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                    $pathId = $paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                    $index = $dates[\substr($buffer, $pos - 22, 7)]+$pathId;
                    $output[$index] = $next[$output[$index]];

                    $order[$pathId] = true;

                    $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                    $pathId = $paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                    $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$pathId;
                    $output[$index] = $next[$output[$index]];

                    $order[$pathId] = true;

                    $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                    $pathId = $paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                    $index = $dates[\substr($buffer, $pos - 22, 7)]+$pathId;
                    $output[$index] = $next[$output[$index]];

                    $order[$pathId] = true;

                    $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                    $pathId = $paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                    $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$pathId;
                    $output[$index] = $next[$output[$index]];

                    $order[$pathId] = true;

                    $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                    $pathId = $paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                    $index = $dates[\substr($buffer, $pos - 22, 7)]+$pathId;
                    $output[$index] = $next[$output[$index]];

                    $order[$pathId] = true;

                    $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                    $pathId = $paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                    $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$pathId;
                    $output[$index] = $next[$output[$index]];

                    $order[$pathId] = true;

                    $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                    $pathId = $paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                    $index = $dates[\substr($buffer, $pos - 22, 7)]+$pathId;
                    $output[$index] = $next[$output[$index]];

                    $order[$pathId] = true;

                    $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                    $pathId = $paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                    $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$pathId;
                    $output[$index] = $next[$output[$index]];

                    $order[$pathId] = true;

                    $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                    $pathId = $paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                    $index = $dates[\substr($buffer, $pos - 22, 7)]+$pathId;
                    $output[$index] = $next[$output[$index]];

                    $order[$pathId] = true;

                    $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                    $pathId = $paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                    $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$pathId;
                    $output[$index] = $next[$output[$index]];

                    $order[$pathId] = true;

                    $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                    $pathId = $paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                    $index = $dates[\substr($buffer, $pos - 22, 7)]+$pathId;
                    $output[$index] = $next[$output[$index]];

                    $order[$pathId] = true;

                    $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                    $pathId = $paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                    $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$pathId;
                    $output[$index] = $next[$output[$index]];

                    $order[$pathId] = true;

                    $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                    $pathId = $paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                    $index = $dates[\substr($buffer, $pos - 22, 7)]+$pathId;
                    $output[$index] = $next[$output[$index]];

                    $order[$pathId] = true;

                    $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                    $pathId = $paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                    $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$pathId;
                    $output[$index] = $next[$output[$index]];

                    $order[$pathId] = true;

                    $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                    $pathId = $paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                    $index = $dates[\substr($buffer, $pos - 22, 7)]+$pathId;
                    $output[$index] = $next[$output[$index]];

                    $order[$pathId] = true;

                    $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                    $pathId = $paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                    $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$pathId;
                    $output[$index] = $next[$output[$index]];

                    $order[$pathId] = true;
                }

                while($nextPos < $lenAsked) {
                    $pos = $nextPos;
                    $nextPos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                    $pathId = $paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                    $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$pathId;
                    $output[$index] = $next[$output[$index]];
                    
                    $order[$pathId] = true;
                }
            }
            else {
                while($nextPos < $lenAskedBatch) {
                    $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                    $index = $dates[\substr($buffer, $pos - 22, 7)]+$paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                    $output[$index] = $next[$output[$index]];

                    $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                    $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                    $output[$index] = $next[$output[$index]];

                    $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                    $index = $dates[\substr($buffer, $pos - 22, 7)]+$paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                    $output[$index] = $next[$output[$index]];

                    $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                    $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                    $output[$index] = $next[$output[$index]];

                    $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                    $index = $dates[\substr($buffer, $pos - 22, 7)]+$paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                    $output[$index] = $next[$output[$index]];

                    $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                    $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                    $output[$index] = $next[$output[$index]];

                    $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                    $index = $dates[\substr($buffer, $pos - 22, 7)]+$paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                    $output[$index] = $next[$output[$index]];

                    $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                    $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                    $output[$index] = $next[$output[$index]];

                    $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                    $index = $dates[\substr($buffer, $pos - 22, 7)]+$paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                    $output[$index] = $next[$output[$index]];

                    $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                    $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                    $output[$index] = $next[$output[$index]];

                    $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                    $index = $dates[\substr($buffer, $pos - 22, 7)]+$paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                    $output[$index] = $next[$output[$index]];

                    $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                    $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                    $output[$index] = $next[$output[$index]];

                    $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                    $index = $dates[\substr($buffer, $pos - 22, 7)]+$paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                    $output[$index] = $next[$output[$index]];

                    $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                    $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                    $output[$index] = $next[$output[$index]];

                    $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                    $index = $dates[\substr($buffer, $pos - 22, 7)]+$paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                    $output[$index] = $next[$output[$index]];

                    $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                    $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                    $output[$index] = $next[$output[$index]];

                    $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                    $index = $dates[\substr($buffer, $pos - 22, 7)]+$paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                    $output[$index] = $next[$output[$index]];

                    $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                    $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                    $output[$index] = $next[$output[$index]];

                    $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                    $index = $dates[\substr($buffer, $pos - 22, 7)]+$paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                    $output[$index] = $next[$output[$index]];

                    $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                    $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                    $output[$index] = $next[$output[$index]];

                    $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                    $index = $dates[\substr($buffer, $pos - 22, 7)]+$paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                    $output[$index] = $next[$output[$index]];

                    $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                    $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                    $output[$index] = $next[$output[$index]];

                    $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                    $index = $dates[\substr($buffer, $pos - 22, 7)]+$paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                    $output[$index] = $next[$output[$index]];

                    $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                    $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                    $output[$index] = $next[$output[$index]];

                    $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                    $index = $dates[\substr($buffer, $pos - 22, 7)]+$paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                    $output[$index] = $next[$output[$index]];

                    $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                    $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                    $output[$index] = $next[$output[$index]];

                    $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                    $index = $dates[\substr($buffer, $pos - 22, 7)]+$paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                    $output[$index] = $next[$output[$index]];

                    $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                    $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                    $output[$index] = $next[$output[$index]];

                    $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                    $index = $dates[\substr($buffer, $pos - 22, 7)]+$paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                    $output[$index] = $next[$output[$index]];

                    $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                    $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                    $output[$index] = $next[$output[$index]];

                    $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                    $index = $dates[\substr($buffer, $pos - 22, 7)]+$paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                    $output[$index] = $next[$output[$index]];

                    $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                    $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                    $output[$index] = $next[$output[$index]];
                }

                while($nextPos < $lenAsked) {
                    $pos = $nextPos;
                    $nextPos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                    $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                    $output[$index] = $next[$output[$index]];
                }
            }

            $read += $lenAsked+10;
        }

        return $output.\pack("v*", ...\array_keys($order));
    }

    static public function partParallel(string $inputPath, int $start, int $length, $dates, $paths, $fullCount) {

        list($readChannel, $writeChannel) = \stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        \stream_set_chunk_size($readChannel, $fullCount*2);
        \stream_set_chunk_size($writeChannel, $fullCount*2);

        $pid = \pcntl_fork();

        if ($pid == 0) {
            \fclose($readChannel);
            $output = Parser::partParse($inputPath, $start, $length, $dates, $paths, $fullCount);
            \fwrite($writeChannel, $output);
            \fflush($writeChannel);
            exit();
        }

        \fclose($writeChannel);
        return $readChannel;
    }

    static public function partReadParallelFirst($thread, $fullCount) { 
        $output = [];
        while(!\feof($thread)) {
            $output[] = \fread($thread, $fullCount);
        }
        $output = \implode("", $output);

        return [\unpack("C*", \substr($output, 0, $fullCount)), \unpack("v*", \substr($output, $fullCount))];
    }

    static public function partReadParallel($thread, $fullCount) {
        $output = [];
        while(!\feof($thread)) {
            $output[] = \fread($thread, $fullCount);
        }

        return \unpack("C*", implode("", $output));
    }

    public function parse(string $inputPath, string $outputPath): void
    {
        \gc_disable();

        // Prepare arrays
        $m2d = [0, 32, 30, 32, 31, 32, 31, 32, 32, 31, 32, 31, 32];
        $numbers = ["", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"];

        $paths = [];
        $pathCount = 0;
        foreach(Visit::all() as $page) {
            $uri = \substr($page->uri, 29);
            $paths[$uri] = $pathCount++;
        }

        $dates = [];
        $dateCount = 0;
        for($y=0; $y!=6; $y++) {
            for($m=1; $m!=13; $m++) {
                $max = $m2d[$m];
                for($d=1; $d!=$max; $d++) {
                    $date = $y."-".$numbers[$m]."-".$numbers[$d];
                    $dates[$date] = $pathCount*$dateCount++;
                }
            }
        }
        for($m=1; $m!=3; $m++) {
            $max = $m2d[$m];
            for($d=1; $d!=$max; $d++) {
                $date = "6-".$numbers[$m]."-".$numbers[$d];
                $dates[$date] = $pathCount*$dateCount++;
            }
        }

        $fullCount = $pathCount*$dateCount;

        // Determine ranges
        $ranges = [];
        $start = 0;
        $file = \fopen($inputPath, "r");
        \stream_set_read_buffer($file, 0);
        $filesize = \filesize($inputPath);
        $length = \ceil($filesize/Parser::$CORES);
        for($i=0; $i!=Parser::$CORES; $i++) {
            \fseek($file, $length*$i+$length);
            \fgets($file);
            $end = \ftell($file);
            $ranges[$i] = [$start, $end];
            $start = $end;
        }
        $ranges[$i-1][1] = $filesize;
        \fclose($file);

        // Start threads
        $threads = [];
        for($i=0; $i!=Parser::$CORES; $i++) {
            $threads[$i] = Parser::partParallel($inputPath, $ranges[$i][0], $ranges[$i][1]-$ranges[$i][0], $dates, $paths, $fullCount);
        }

        // Precompute while waiting
        $datesJson = [];
        foreach($dates as $date => $dateI) {
            $datesJson[$dateI] = ",\n        \"202".$date.'": ';
        }

        $pathsJson = [];
        foreach(Visit::all() as $page) {
            $uri = \substr($page->uri, 25);
            $short = \substr($page->uri, 29);
            $pathsJson[$paths[$short]] = "\n    },\n    \"\\/blog\\/".$uri.'": {';
        }

        $output = \array_fill(0, $fullCount, 0);

        // Read threads
        $first = $threads[0];
        $read = []; $write = []; $except = [];
        while(\count($threads) != 0) {
            $read = $threads;
            \stream_select($read, $write, $except, 5);
            foreach($read as $i => $thread) {
                if($thread == $first) {
                    list($data, $sortedPaths) = Parser::partReadParallelFirst($thread, $fullCount);
                    $pathsJson[$sortedPaths[1]] = substr($pathsJson[$sortedPaths[1]], 7);
                }
                else {
                    $data = Parser::partReadParallel($thread, $fullCount);
                }

                for($j=0; $j!=$fullCount; $j++) {
                    $output[$j] += $data[$j+1];
                }
                unset($threads[$i]);
            }
        }

        // Merge
        $buffer = "{";
        $max = $pathCount+1;
        for($i=1; $i!=$max; $i++) {
            $pathI = $sortedPaths[$i];
            $buffer .= $pathsJson[$pathI];  
            for($j=$pathI; $j<$fullCount; $j+=$pathCount) {
                if($output[$j] != 0) {
                    $buffer .= \substr($datesJson[$j-$pathI].$output[$j], 1);
                    $j+=$pathCount;
                    break;
                }
            }

            for(; $j<$fullCount; $j+=$pathCount) {
                if($output[$j] != 0) {
                    $buffer .= $datesJson[$j-$pathI].$output[$j];
                }
            }
        }
        $buffer .= "\n    }\n}";
        \file_put_contents($outputPath, $buffer);
    }
}