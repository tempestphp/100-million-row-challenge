<?php
namespace App;

use Exception;

final class Parser
{

    public function parse(string $inputPath, string $outputPath): void
    {

        //$inFile = "/mnt/d/php/million-row-challenge/100-million-row-challenge/data/test-data.csv";
        // $opFile = "/mnt/d/php/million-row-challenge/100-million-row-challenge/data/test-data-op.json";
        $inFile = $inputPath;
        $opFile = $outputPath;

        //$file = new \SplFileObject($inFile, 'r');
        $ret = [];

        $handle = fopen($inFile, 'r');

        if (! $handle) {
            throw new Exception("Failed to open file");
        }

        try {
            $chunkSize = 1024 * 1024 * 512;
            stream_set_read_buffer($handle, 1024 * 1024 * 512);

            $stitcherIoLength = strlen("https://stitcher.io");
            $leftOver = "";

            while (! feof($handle)) {
                $lines = fread($handle, $chunkSize);
                $lines = $leftOver . $lines;
                $lines = explode("\n", $lines);
                $leftOver = array_pop($lines);

                foreach ($lines as $line) {
                    if (empty($line)) {
                        continue;
                    }

                    $data = explode(",", $line, 3);
                    $data[0] = substr($data[0], $stitcherIoLength);
                    $data[1] = substr($data[1], 0, 10);

                    if (! isset($ret[$data[0]][$data[1]])) {
                        $ret[$data[0]][$data[1]] = 1;
                    } else {
                        $ret[$data[0]][$data[1]]++;
                    }

                }
            }

            if ($leftOver != "") {
                $data = explode(",", $leftOver, 3);
                $data[0] = substr($data[0], $stitcherIoLength);
                $data[1] = substr($data[1], 0, 10);

                if (! isset($ret[$data[0]][$data[1]])) {
                    $ret[$data[0]][$data[1]] = 1;
                } else {
                    $ret[$data[0]][$data[1]]++;
                }

            }
        } finally {
            fclose($handle);
        }

        foreach ($ret as &$dates) {
            ksort($dates);
        }

        file_put_contents($opFile, json_encode($ret, JSON_PRETTY_PRINT));
    }
}
