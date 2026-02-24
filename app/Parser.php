<?php

namespace App;

use SplFixedArray;

enum ParserState
{
    case START_SKIP;
    case PATH;
    case YEAR;
    case MONTH;
    case DAY;
    case END_SKIP;
}

final class Parser
{
    private const int BUFFER_SIZE = 32 * 1024 * 1024;
    private const int START_YEAR = 2021;
    private const int DATA_POINTS = 6 * 12 * 31;
    private const int UNIQUE_PATHS = 300;
    private const int URL_SKIP = 24;
    private const int TZ_SKIP = 14;

    public function parse(string $inputPath, string $outputPath): void
    {
        $pathToIndex = [];
        $pathIndex = 0;
        $data = new SplFixedArray(self::UNIQUE_PATHS);

        $state = ParserState::START_SKIP;
        $skipLeft = self::URL_SKIP;
        $path = '';
        $year = 0;
        $month = 0;
        $day = 0;

        $handle = fopen($inputPath, 'rb');

        while (!feof($handle)) {
            $chunk = fread($handle, self::BUFFER_SIZE);
            $end = strlen($chunk);
            $i = 0;
            while ($i < $end) {
                switch ($state) {
                    case ParserState::START_SKIP:
                        if ($i + $skipLeft <= $end) {
                            $i += $skipLeft;
                            $path = '';
                            $year = 0;
                            $month = 0;
                            $day = 0;
                            $state = ParserState::PATH;
                        } else {
                            $skipLeft -= $end - $i;
                            $i = $end;
                        }
                        break;
                    case ParserState::PATH:
                        while ($i < $end) {
                            if ($chunk[$i] == ',') {
                                $state = ParserState::YEAR;
                                break;
                            }
                            $path .= $chunk[$i];
                            $i++;
                        }
                        break;
                    case ParserState::YEAR:
                        while ($i < $end) {
                            if ($chunk[$i] == '-') {
                                $state = ParserState::MONTH;
                                break;
                            }
                            $year = 10 * $year + ord($chunk[$i]) - ord('0');
                            $i++;
                        }
                        break;
                    case ParserState::MONTH:
                        while ($i < $end) {
                            if ($chunk[$i] == '-') {
                                $state = ParserState::DAY;
                                break;
                            }
                            $month = 10 * $month + ord($chunk[$i]) - ord('0');
                            $i++;
                        }
                        break;
                    case ParserState::DAY:
                        while ($i < $end) {
                            if ($chunk[$i] == 'T') {
                                $state = ParserState::END_SKIP;
                                $skipLeft = self::TZ_SKIP;
                                $curPathIndex = $pathToIndex[$path] ?? null;
                                if ($curPathIndex === null) {
                                    $curPathIndex = $pathIndex;
                                    $pathToIndex[$path] = $pathIndex;
                                    $pathIndex++;
                                    $data[$curPathIndex] = new SplFixedArray(self::DATA_POINTS);
                                }
                                $year -= self::START_YEAR;
                                $month--;
                                $day--;
                                $dataPointIndex = $year * 372 + $month * 31 + $day;
                                $data[$curPathIndex][$dataPointIndex] = ($data[$curPathIndex][$dataPointIndex] ?? 0) + 1;
                                break;
                            }
                            $day = 10 * $day + ord($chunk[$i]) - ord('0');
                            $i++;
                        }
                        break;
                    case ParserState::END_SKIP:
                        if ($i + $skipLeft <= $end) {
                            $i += $skipLeft;
                            $state = ParserState::START_SKIP;
                            $skipLeft = self::URL_SKIP;
                        } else {
                            $skipLeft -= $end - $i;
                            $i = $end;
                        }
                        break;
                }
                $i++;
            }
        }
        fclose($handle);

        $handle = fopen($outputPath, 'w');
        ob_start();
        echo "{\n";
        $firstPath = true;
        foreach ($pathToIndex as $path => $index) {
            if ($firstPath) {
                echo '    "\/blog\/' . $path . '": {' . "\n";
                $firstPath = false;
            } else {
                echo ",\n" . '    "\/blog\/' . $path . '": {' . "\n";
            }
            $firstData = true;
            for ($i = 0; $i < self::DATA_POINTS; $i++) {
                if (!$data[$index][$i]) {
                    continue;
                }
                $y = (self::START_YEAR + intdiv($i, (12 * 31)));
                $m = ((intdiv($i, 31) % 12) + 1);
                $d = (($i % 31) + 1);
                if ($firstData) {
                    echo sprintf('        "%04d-%02d-%02d": %d', $y, $m, $d, $data[$index][$i]);
                    $firstData = false;

                    continue;
                }
                echo ",\n" . sprintf('        "%04d-%02d-%02d": %d', $y, $m, $d, $data[$index][$i]);
            }
            echo "\n    }";
            fwrite($handle, ob_get_clean());
            ob_start();
        }
        echo "\n}";
        fwrite($handle, ob_get_clean());
        fclose($handle);
    }
}
