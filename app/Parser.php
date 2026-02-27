<?php

declare(strict_types=0);

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $children = 8;
        $fstat = stat($inputPath);
        $size = $fstat['size'];
        $childSize = round($size / ($children));

        $workers = [];
        for($childNo = 1; $childNo <= $children; $childNo++) {
            $socks = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
            $pid = pcntl_fork();

            if ($pid === -1) {
                exit('could not fork');
            }

            if ($pid === 0) {
                // close parent sock
                fclose($socks[0]);
                // child
                $start = ($childNo - 1) * $childSize;
                $this->processFile($start, $childSize, $socks[1], $inputPath);
                fflush($socks[1]);
                fclose($socks[1]);
                exit(0);
            }

            // master
            fclose($socks[1]);
            $workers[] = ['pid' => $pid, 'sock' => $socks[0]];
        }

        $out = [];
        foreach ($workers as $worker) {
            $sock = $worker['sock'];
            while (($line = fgets($sock)) !== false) {
                $posU = strpos($line, "\t");
                $posD = strpos($line, 'D', $posU);
                $url = substr($line, 0, $posU);
                $date = substr($line, $posU + 1, $posD - $posU - 1);
                $count = (int)substr($line, $posD + 1);

                $out[$url][$date] = ($out[$url][$date] ?? 0 ) + $count;
            }
        }

        foreach ($out as &$data) {
            ksort($data);
        }
        unset($data);

        $outFd = fopen($outputPath, 'w');
        fwrite($outFd, json_encode($out, JSON_PRETTY_PRINT));
        fflush($outFd);
        fclose($outFd);
    }

    private function processFile(int $start, int $size, mixed $outSock, string $fileIn): void
    {
        $inFd = fopen($fileIn, 'r');
        $seekResult = fseek($inFd, $start);
        if ($seekResult === -1) {
            exit('could not seek');
        }

        if ($start !== 0) {
            // skip an incomplete line
            fgets($inFd);
        }

        $arrByUrlAndDate = [];
        $currPos = ftell($inFd);
        $limit = $start + $size;
        while(($line = fgets($inFd)) !== false) {
            $commaPos = strpos($line, ',');
            $url = substr($line, 19, $commaPos - 19);
            $date = substr($line, $commaPos + 1, 10);
            $arrByUrlAndDate[$url][$date] = ($arrByUrlAndDate[$url][$date] ?? 0) + 1;
            $currPos += strlen($line);
            if ($currPos > $limit) {
                break;
            }
        }
        fclose($inFd);

        foreach ($arrByUrlAndDate as $url => $data) {
            foreach ($data as $date => $count) {
                fwrite($outSock, "{$url}\t{$date}D{$count}\n");
            }
        }
    }

}