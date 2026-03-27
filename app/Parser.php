<?php

namespace App;

use Exception;

final class Parser
{

    public function parse(string $inputPath, string $outputPath): void
    {
        $visitMap = [];
        $visitIndexMap = [];
        $dateMap = [];
        $dateIndexMap = [];
        $data = [];
        $nextVisitMapIndex = $nextDateMapIndex = 0;
        $urlPrefix = 'https://stitcher.io';
        $urlPrefixLength = strlen($urlPrefix);

        $input = fopen($inputPath, 'r');

        while (($line = fgets($input)) !== false) {
            $commaPos = strpos($line, ',');
            $visit = substr($line, $urlPrefixLength, $commaPos - $urlPrefixLength);
            $visit = str_replace('/', '\/', $visit);
            $date = substr($line, $commaPos + 1, 10);

            $dateMapIndex = $dateMap[$date] ??= ++$nextDateMapIndex;

            $dateIndexMap[$dateMapIndex] = $date;

            $visitMapIndex = $visitMap[$visit] ??= ++$nextVisitMapIndex;
            $visitIndexMap[$visitMapIndex] = $visit;

            $data[$visitMapIndex][$dateMapIndex] ??= 0;
            $data[$visitMapIndex][$dateMapIndex] += 1;
        }

        fclose($input);

        $output = fopen($outputPath, 'w');

        $index = 0;
        $total = count($data);

        $buffer = '{' . PHP_EOL;

        foreach ($data as $visitMapIndex => $dateMap) {
            $buffer .= '    "' . $visitIndexMap[$visitMapIndex] . '": {' . PHP_EOL;

            $countsByDate = [];

            foreach ($dateMap as $dateIndex => $count) {
                $countsByDate[$dateIndexMap[$dateIndex]] = $count;
            }

            ksort($countsByDate);

            $lastIndex = array_key_last($countsByDate);

            foreach ($countsByDate as $date => $count) {
                $buffer .= '        "' . $date . '": ' . $count;
                $buffer .= ($date !== $lastIndex ? ',' : '') . PHP_EOL;
            }

            if ($index++ % 1000 === 0) {
                fwrite($output, $buffer);
                $buffer = '';
            }

            $buffer .= '    }' . ($index < $total ? ',' : '')  . PHP_EOL;
        }

        fwrite($output, $buffer . '}');
        fclose($output);
    }
}
