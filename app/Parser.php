<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $handle = fopen($inputPath, 'r');
        $pathMap = [];
        $dateMap = [];
        $pathNames = [];
        $dateNames = [];
        $counts = [];
        $nextPathId = 0;
        $nextDateId = 0;
        $chunkSize = 16 * 1024 * 1024;

        while (!feof($handle)) {
            $raw = fread($handle, $chunkSize);
            if ($raw === false || $raw === '') {
                break;
            }

            $rawLen = strlen($raw);
            $lastNl = strrpos($raw, "\n");
            if ($lastNl === false) {
                fseek($handle, -$rawLen, SEEK_CUR);
                $chunkSize *= 2;
                continue;
            }

            // Seek back so next fread starts after the last complete line
            $remainder = $rawLen - $lastNl - 1;
            if ($remainder > 0) {
                fseek($handle, -$remainder, SEEK_CUR);
            }

            $pos = 0;
            while ($pos < $lastNl) {
                $nlPos = strpos($raw, "\n", $pos);
                $commaPos = $nlPos - 26;
                $pathStart = $pos + 19;
                $pathLen = $commaPos - $pathStart;

                $path = substr($raw, $pathStart, $pathLen);
                if (isset($pathMap[$path])) {
                    $pid = $pathMap[$path];
                } else {
                    $pid = $nextPathId++;
                    $pathMap[$path] = $pid;
                    $pathNames[$pid] = $path;
                }

                // Date: 10-char substr, small map
                $date = substr($raw, $commaPos + 1, 10);
                if (isset($dateMap[$date])) {
                    $did = $dateMap[$date];
                } else {
                    $did = $nextDateId++;
                    $dateMap[$date] = $did;
                    $dateNames[$did] = $date;
                }

                if (isset($counts[$pid][$did])) {
                    $counts[$pid][$did]++;
                } else {
                    $counts[$pid][$did] = 1;
                }

                $pos = $nlPos + 1;
            }
        }

        // Handle trailing line without \n
        $trailing = stream_get_contents($handle);
        if ($trailing !== false && $trailing !== '') {
            $len = strlen($trailing);
            $commaPos = $len - 25;
            if ($commaPos > 19) {
                $path = substr($trailing, 19, $commaPos - 19);
                $date = substr($trailing, $commaPos + 1, 10);
                if (isset($pathMap[$path])) {
                    $pid = $pathMap[$path];
                } else {
                    $pid = $nextPathId++;
                    $pathMap[$path] = $pid;
                    $pathNames[$pid] = $path;
                }
                if (isset($dateMap[$date])) {
                    $did = $dateMap[$date];
                } else {
                    $did = $nextDateId++;
                    $dateMap[$date] = $did;
                    $dateNames[$did] = $date;
                }
                if (isset($counts[$pid][$did])) {
                    $counts[$pid][$did]++;
                } else {
                    $counts[$pid][$did] = 1;
                }
            }
        }

        fclose($handle);

        // Build output
        $data = [];

        foreach ($counts as $pid => $dates) {
            $pathDates = [];
            foreach ($dates as $did => $count) {
                $pathDates[$dateNames[$did]] = $count;
            }
            ksort($pathDates);
            $data[$pathNames[$pid]] = $pathDates;
        }

        file_put_contents($outputPath, json_encode($data, JSON_PRETTY_PRINT));

        gc_enable();
    }
}
