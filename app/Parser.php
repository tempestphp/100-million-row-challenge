<?php

namespace App;

use App\Commands\Visit;

final class Parser
{
    private const int DATA_POINTS = 6 * 12 * 31;

    private const int BUFFER_SIZE = 16 * 1024;

    private const int OUTPUT_BUFFER = 256 * 1024;

    private const int ADDITIONAL_READ_BYTES = 200;

    private const int URL_FIXED_LENGTH = 25;

    private const int SLUG_TO_COMMA_SEARCH_OFFSET = 5;

    private const int COMMA_TO_NEWLINE_OFFSET = 27;

    private const int WORKERS = 12;

    // avg line is > 75 bytes and with 5000 lines
    // chance that some links will be missing 1/73829502088041 (if I used correct formula)
    private const int SIZE_TO_COMPLETE_LINKS_ORDER = 76 * 5000;

    private function getOrder($handle, int $linksCount): array
    {
        $order = [];

        $o = 0;
        fseek($handle, 0);
        $data = fread($handle, self::SIZE_TO_COMPLETE_LINKS_ORDER);
        $endData = strrpos($data, "\n");

        while ($o < $endData && count($order) < $linksCount) {
            $nextComma = strpos($data, ',', $o + self::SLUG_TO_COMMA_SEARCH_OFFSET);

            $link = substr($data, $o + self::URL_FIXED_LENGTH, $nextComma - $o - self::URL_FIXED_LENGTH);
            $order[$link] = 0;

            $o = $nextComma + self::COMMA_TO_NEWLINE_OFFSET;
        }

        return array_keys($order);
    }

    private function worker(\Socket $socket, string $inputPath, int $start, int $end, array $substringToIndex): void
    {
        $result = $this->process($inputPath, $start, $end, $substringToIndex);
        $data = igbinary_serialize($result);
        socket_write($socket, $data, strlen($data));
        socket_close($socket);
    }

    private function process(string $inputPath, int $start, int $end, array $substringToIndex): array
    {
        $res = array_fill(0, count($substringToIndex), 0);

        $handle = fopen($inputPath, 'r');
        stream_set_read_buffer($handle, 0);

        while ($start < $end) {
            fseek($handle, $start);
            $data = fread($handle, min(self::BUFFER_SIZE, $end - $start));
            $endData = strrpos($data, "\n");
            if ($endData === false) {
                break;
            }
            $endData--;
            $o = 0;
            while ($o < $endData) {
                $nextComma = strpos($data, ',', $o + self::SLUG_TO_COMMA_SEARCH_OFFSET);

                $substring = substr($data, $o + self::URL_FIXED_LENGTH, $nextComma + 11 - $o - self::URL_FIXED_LENGTH);
                $res[$substringToIndex[$substring]]++;

                $o = $nextComma + self::COMMA_TO_NEWLINE_OFFSET;
            }

            $start += $endData + 2;
        }

        fclose($handle);

        return $res;
    }

    private function merge(array $results): array
    {
        $res = $results[0];
        for ($i = 1; $i < count($results); $i++) {
            foreach ($results[$i] as $j => $v) {
                $res[$j] += $v;
            }
        }

        return $res;
    }

    private function writeResult(string $outputPath, array $res, array $order, array $links, array $dates): void
    {
        $handle = fopen($outputPath, 'w');
        ob_start();
        $link = array_shift($order);
        $linkId = $links[$link];

        echo "{\n";
        echo '    "\/blog\/' . $link . '": {' . "\n";
        $j = $linkId * self::DATA_POINTS;
        $jl = $j + self::DATA_POINTS;
        while ($j < $jl) {
            $cnt = $res[$j];
            if ($cnt == 0) {
                $j++;

                continue;
            }
            echo sprintf('        "%s": %d', $dates[$j % self::DATA_POINTS], $cnt);
            $j++;
            break;
        }
        while ($j < $jl) {
            $cnt = $res[$j];
            if ($cnt == 0) {
                $j++;

                continue;
            }
            echo sprintf(",\n        \"%s\": %d", $dates[$j % self::DATA_POINTS], $cnt);
            $j++;
        }
        echo "\n    }";

        foreach ($order as $link) {
            $linkId = $links[$link];
            echo ",\n" . '    "\/blog\/' . str_replace('/', '\/', $link) . '": {' . "\n";
            $j = $linkId * self::DATA_POINTS;
            $jl = $j + self::DATA_POINTS;
            while ($j < $jl) {
                $cnt = $res[$j];
                if ($cnt == 0) {
                    $j++;

                    continue;
                }
                echo sprintf('        "%s": %d', $dates[$j % self::DATA_POINTS], $cnt);
                $j++;
                break;
            }
            while ($j < $jl) {
                $cnt = $res[$j];
                if ($cnt == 0) {
                    $j++;

                    continue;
                }
                echo sprintf(",\n        \"%s\": %d", $dates[$j % self::DATA_POINTS], $cnt);
                $j++;
            }

            echo "\n    }";
            if (ob_get_length() > self::OUTPUT_BUFFER) {
                fwrite($handle, ob_get_clean());
                ob_start();
            }
        }
        echo "\n}";
        fwrite($handle, ob_get_clean());
        fclose($handle);
    }

    public function parse(string $inputPath, string $outputPath): void
    {
        $dates = array_fill(0, self::DATA_POINTS, null);
        $i = 0;
        for ($y = 2021; $y <= 2026; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                for ($d = 1; $d <= 31; $d++) {
                    $dates[$i] = sprintf('%d-%02d-%02d', $y, $m, $d);
                    $i++;
                }
            }
        }
        $linkToIndex = [];
        $indexToLink = [];
        foreach (Visit::all() as $i => $v) {
            $link = substr($v->uri, self::URL_FIXED_LENGTH);
            $linkToIndex[$link] = $i;
            $indexToLink[] = $link;
        }

        $substringToIndex = [];
        $i = 0;
        foreach ($indexToLink as $link) {
            foreach ($dates as $date) {
                $substringToIndex[$link . ',' . $date] = $i;
                $i++;
            }
        }

        $size = filesize($inputPath);
        $chunk = intdiv($size, self::WORKERS) + 1;
        $start = 0;

        $sockets = [];

        $handle = fopen($inputPath, 'r');

        for ($i = 0; $i < self::WORKERS; $i++) {
            $socketPair = [];
            socket_create_pair(AF_UNIX, SOCK_STREAM, 0, $socketPair);

            $end = min($start + $chunk, $size);
            if ($i != self::WORKERS - 1) {
                fseek($handle, $end);
                $data = fread($handle, self::ADDITIONAL_READ_BYTES);
                $end += strpos($data, "\n") + 1;
            } else {
                $end = $size;
            }

            $pid = pcntl_fork();
            if ($pid == 0) {
                socket_close($socketPair[1]);
                $this->worker($socketPair[0], $inputPath, $start, $end, $substringToIndex);
                exit(0);
            }
            socket_close($socketPair[0]);
            $sockets[] = $socketPair[1];

            $start = $end;
        }

        $order = $this->getOrder($handle, count($linkToIndex));
        fclose($handle);

        $results = array_fill(0, self::WORKERS, '');

        $remaining = $sockets;
        while (count($remaining) > 0) {
            $read = $remaining;
            $write = null;
            $expect = null;
            socket_select($read, $write, $expect, null);
            foreach ($read as $socket) {
                $chunk = socket_read($socket, 10 * 1024 * 1024, PHP_BINARY_READ);
                $index = array_search($socket, $sockets);
                if ($chunk === '' || $chunk === false) {
                    socket_close($socket);
                    $results[$index] = igbinary_unserialize($results[$index]);
                    unset($remaining[array_search($socket, $remaining)]);
                    break;
                }
                $results[$index] .= $chunk;
            }
        }

        $res = $this->merge($results);
        $this->writeResult($outputPath, $res, $order, $linkToIndex, $dates);
    }
}
