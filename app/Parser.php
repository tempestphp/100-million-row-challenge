<?php

namespace App;

use Exception;



final class Parser
{
    private const THREAD_COUNT = 8;
    private const READ_CHUNK_SZ = 0x10000;
    private const NUM_DATES = 2232;

    private function findSplitPoints(string $inputPath): array
    {
        $res = [0];
        $size = filesize($inputPath);
        $handle = fopen($inputPath, 'rb');
        for ($i=1; $i<self::THREAD_COUNT; $i++) {
            fseek($handle, (int)(($i*$size)/self::THREAD_COUNT));
            fgets($handle); // read until newline
            $res[] = ftell($handle);
        }
        fclose($handle);
        $res[] = $size;
        return $res;
    }

    private function parseRange(string $inputPath, int $start, int $end, array $datelutlut, int $tid, $sock): void
    {
        $map = [];
        $file = fopen($inputPath, 'rb');
        fseek($file, $start);
        $block = '';
        $len_remaining = $end - $start;
        //$lines_read = 0;
        for (;;) {
            if ($len_remaining > 0) {
                $newdata = fread($file, min($len_remaining, self::READ_CHUNK_SZ));
            } else {
                $newdata = '';
            }
            $readlen = strlen($newdata);
            $len_remaining -= $readlen;
            $block .= $newdata;
            $blen = strlen($block) - 200; // 200 >= max line length, stop early to avoid starting to parse a line that straddles the boundary
            if ($blen <= 0) {
                $blen += 200; // we're on the last block, no early-stop needed
                if ($blen == 0) break; // eof
            };
            $idx = 0;
            while ($idx < $blen) { // this is the hot loop
                $comma = strpos($block, ',', $idx + 25);
                $path = substr($block, $idx + 25, ($comma - $idx) - 25);
                //$date = ($block[$comma + 4]-1)*372 +
                //        (substr($block, $comma + 6, 2)-1)*31 +
                //        (substr($block, $comma + 9, 2)-1);
                $date = $datelutlut[substr($block, $comma + 4, 7)];

                //print($path.":".$date."\n");
                if (!isset($map[$path])) {
                    $map[$path] = array_fill(0, self::NUM_DATES, 0);
                }
                $map[$path][$date]++;
                $idx = $comma + 27;
                //$lines_read++;
            }
            $block = substr($block, $idx); // remainder
            //print($lines_read."\n");
        }
        fclose($file);

        //print("done reading (thread $tid)\n");

        $data = igbinary_serialize($map);
        $len = strlen($data);
        $header = pack('V', $len);
        fwrite($sock, $header);
        $written = 0;
        while ($written < $len) {
            $w = fwrite($sock, substr($data, $written, 65536));
            $written += $w;
        }
        fclose($sock);
        //print(strlen(serialize($map))."\n");
        //print("hello\n");
        //return $map;
    }

    public function parse(string $inputPath, string $outputPath): void
    {
        $datelut = [];
        for ($y = 1; $y <= 6; $y++) {
            for ($mm = 1; $mm <= 12; $mm++) {
                for ($dd = 1; $dd <= 31; $dd++) {
                    $datelut[] = sprintf('%d-%02d-%02d', $y, $mm, $dd);
                }
            }
        }
        $datelutlut = array_flip($datelut);

        //print("done building date table\n");

        $splitpoints = $this->findSplitPoints($inputPath);

        // spin up threads
        $pids = [];
        $sockets = [];
        for ($i=0; $i<self::THREAD_COUNT; $i++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            $pid = pcntl_fork();
            if ($pid == -1) {
                throw new \RuntimeException("Could not fork process");
            } elseif ($pid == 0) {
                fclose($pair[0]);
                $this->parseRange($inputPath, $splitpoints[$i], $splitpoints[$i+1], $datelutlut, $i, $pair[1]);
                exit(0);
            }
            $pids[] = $pid;
            fclose($pair[1]);
            $sockets[] = $pair[0];
        }

        // join the threads, collect results
        // NOTE: must read from socket BEFORE waitpid, otherwise deadlock
        // if child's data exceeds socket buffer and blocks on fwrite
        $maps = [];
        $slugs = []; // all slugs, in first-seen order
        $seenSlugs = [];
        foreach ($pids as $tid=>$pid) {
            //print("getting $tid\n");
            $header = fread($sockets[$tid], 4);
            $len = unpack('V', $header)[1];
            $data = '';
            while (strlen($data) < $len) {
                $data .= fread($sockets[$tid], $len - strlen($data));
            }
            fclose($sockets[$tid]);
            $map = igbinary_unserialize($data);
            pcntl_waitpid($pid, $status);
            if ($status !== 0) {
                throw new \RuntimeException("child exited with nonzero status");
            }
            foreach($map as $key=>$_) {
                if (!isset($seenSlugs[$key])) {
                    $seenSlugs[$key] = true;
                    $slugs[] = $key;
                }
            }
            $maps[] = $map;
        }

        $outHandle = fopen($outputPath, 'wb');
        fwrite($outHandle, "{\n");
        //$zeroes = array_fill(0, self::NUM_DATES, 0);
        foreach ($slugs as $slug) {
            $counts = $maps[0][$slug] ?? array_fill(0, self::NUM_DATES, 0);
            for ($i=1; $i<self::THREAD_COUNT; $i++) {
                if (isset($maps[$i][$slug])) {
                    $tmp = $maps[$i][$slug];
                    for ($j=0; $j<self::NUM_DATES; $j++) {
                        $counts[$j] += $tmp[$j];
                    }
                }
            }
            $datefmt = [];
            for ($i=0; $i<self::NUM_DATES; $i++) {
                $count = $counts[$i];
                if ($count > 0) {
                    $datefmt[] = $datelut[$i].'": '.$count;
                }
            }
            $joined = implode(",\n        \"202", $datefmt);
            fwrite($outHandle, "    \"\\/blog\\/$slug\": {\n        \"202$joined\n    },\n");
        }
        fseek($outHandle, -2, SEEK_CUR); // unwind last comma+newline
        fwrite($outHandle, "\n}");
        fclose($outHandle);
    }
}
