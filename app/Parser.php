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

    private function worker(\Socket $socket, string $inputPath, int $start, int $end, int $resultsArraySize): void
    {
        $result = $this->process($inputPath, $start, $end, $resultsArraySize);
        $data = igbinary_serialize($result);
        socket_write($socket, $data, strlen($data));
        socket_close($socket);
    }

    private function process(string $inputPath, int $start, int $end, int $resultArraySize): array
    {
        $res = array_fill(0, $resultArraySize, 0);

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
                $c = strpos($data, ',', $o + self::SLUG_TO_COMMA_SEARCH_OFFSET);

                $res[
                    self::DATA_POINTS * match ($data[$c - 22]) {
                        'w' => 0,
                        's' => match ($data[$c - 18]) {
                            's' => match ($data[$c - 13]) {
                                'i' => match ($data[$c - 1]) {
                                    '1' => 1,
                                    '2' => 2,
                                },
                                'a' => 81,
                                'g' => 256,
                                'r' => 257,
                            },
                            'd' => 8,
                            '-' => match ($data[$c - 13]) {
                                'e' => 25,
                                'h' => 116,
                            },
                            't' => 126,
                            'i' => 246,
                        },
                        '/' => match ($data[$c - 18]) {
                            'g' => match ($data[$c - 13]) {
                                'g' => 3,
                                't' => match ($data[$c - 5]) {
                                    'p' => match ($data[$c - 1]) {
                                        '4' => 5,
                                        '5' => 7,
                                    },
                                    'b' => 149,
                                    'r' => 173,
                                    't' => 220,
                                },
                                'n' => match ($data[$c - 5]) {
                                    's' => 20,
                                    'n' => 80,
                                    'i' => 203,
                                },
                                's' => 215,
                                'o' => 224,
                                'l' => 228,
                                'a' => 239,
                            },
                            'u' => 31,
                            'r' => match ($data[$c - 13]) {
                                '-' => match ($data[$c - 5]) {
                                    'e' => 57,
                                    'a' => 231,
                                },
                                'o' => 58,
                            },
                            '-' => match ($data[$c - 13]) {
                                't' => 97,
                                'm' => 101,
                                'a' => 181,
                            },
                            'n' => 132,
                            'e' => match ($data[$c - 13]) {
                                '-' => 150,
                                'v' => 234,
                            },
                            't' => 183,
                            'o' => 242,
                            'i' => 265,
                        },
                        't' => match ($data[$c - 18]) {
                            'c' => 4,
                            's' => 19,
                            'n' => 52,
                            '-' => 91,
                            'p' => 93,
                            'g' => 110,
                            'r' => match ($data[$c - 5]) {
                                'n' => 196,
                                'o' => 199,
                                '-' => match ($data[$c - 2]) {
                                    't' => 212,
                                    '2' => 261,
                                },
                            },
                            't' => 217,
                        },
                        'i' => match ($data[$c - 18]) {
                            'e' => match ($data[$c - 13]) {
                                'l' => 6,
                                '/' => match ($data[$c - 5]) {
                                    'p' => 51,
                                    'n' => 123,
                                    'o' => 167,
                                },
                            },
                            'c' => 67,
                            's' => match ($data[$c - 13]) {
                                'o' => 79,
                                '-' => match ($data[$c - 1]) {
                                    '2' => 156,
                                    '3' => 193,
                                    '4' => 216,
                                    '5' => 241,
                                },
                            },
                            'l' => match ($data[$c - 13]) {
                                'o' => match ($data[$c - 5]) {
                                    's' => 105,
                                    'h' => 177,
                                    'l' => 208,
                                },
                                'r' => 111,
                                'h' => match ($data[$c - 5]) {
                                    'g' => 118,
                                    'w' => 141,
                                    '-' => 245,
                                },
                                't' => 160,
                            },
                            'y' => 122,
                            'p' => 227,
                            't' => 237,
                            'o' => match ($data[$c - 13]) {
                                't' => 248,
                                'w' => 267,
                            },
                        },
                        'o' => match ($data[$c - 18]) {
                            'o' => match ($data[$c - 13]) {
                                'i' => match ($data[$c - 1]) {
                                    '1' => 9,
                                    '2' => 17,
                                },
                                'a' => 27,
                                'e' => match ($data[$c - 5]) {
                                    '-' => 34,
                                    'f' => 86,
                                    'o' => 140,
                                },
                                'm' => 47,
                                's' => 64,
                                'p' => match ($data[$c - 5]) {
                                    'c' => 108,
                                    's' => 112,
                                    'l' => 233,
                                },
                                'b' => 195,
                                '-' => 211,
                            },
                            'y' => 23,
                            'a' => 36,
                            '-' => match ($data[$c - 13]) {
                                'e' => 62,
                                's' => 185,
                                'c' => 262,
                            },
                            'n' => 78,
                            'p' => match ($data[$c - 13]) {
                                'f' => 85,
                                'i' => 192,
                            },
                            'e' => match ($data[$c - 13]) {
                                '-' => 94,
                                't' => 138,
                            },
                            't' => 106,
                            'r' => 107,
                            's' => 179,
                            'i' => 189,
                        },
                        'j' => 10,
                        'u' => match ($data[$c - 18]) {
                            'i' => 11,
                            'p' => 32,
                            'o' => 46,
                            'g' => match ($data[$c - 13]) {
                                'h' => 55,
                                '-' => 144,
                            },
                            'm' => 73,
                        },
                        '.' => match ($data[$c - 13]) {
                            'p' => match ($data[$c - 5]) {
                                'f' => 12,
                                't' => 188,
                            },
                            'n' => match ($data[$c - 2]) {
                                '7' => match ($data[$c - 1]) {
                                    '3' => 44,
                                    '4' => 74,
                                },
                                '8' => match ($data[$c - 1]) {
                                    '1' => 152,
                                    '2' => 190,
                                    '3' => 213,
                                    '4' => 240,
                                    '5' => 259,
                                },
                            },
                            'm' => 145,
                            'i' => 252,
                            'v' => 254,
                        },
                        'c' => match ($data[$c - 18]) {
                            'r' => 13,
                            '.' => match ($data[$c - 5]) {
                                'e' => 121,
                                'h' => 202,
                            },
                            'o' => 180,
                            '-' => 232,
                        },
                        'a' => match ($data[$c - 18]) {
                            '-' => match ($data[$c - 13]) {
                                's' => 14,
                                'e' => 53,
                            },
                            'i' => 49,
                            'a' => 63,
                            'c' => 99,
                            'v' => 143,
                            't' => 197,
                            'r' => 198,
                        },
                        'r' => match ($data[$c - 18]) {
                            'e' => 15,
                            'm' => match ($data[$c - 2]) {
                                '1' => 41,
                                'f' => 153,
                            },
                            '-' => 42,
                            'l' => 72,
                            'c' => match ($data[$c - 13]) {
                                'n' => 109,
                                'r' => 178,
                            },
                            '/' => match ($data[$c - 5]) {
                                'p' => 114,
                                'l' => match ($data[$c - 2]) {
                                    'h' => 128,
                                    'n' => 200,
                                },
                                '-' => 223,
                                'i' => 263,
                            },
                            'n' => match ($data[$c - 13]) {
                                'l' => 119,
                                'f' => 238,
                            },
                        },
                        'm' => match ($data[$c - 18]) {
                            'e' => 16,
                            'p' => 26,
                        },
                        'g' => match ($data[$c - 18]) {
                            'p' => match ($data[$c - 5]) {
                                'm' => 18,
                                'g' => 171,
                            },
                            'o' => 35,
                            'b' => 48,
                            'n' => match ($data[$c - 13]) {
                                'i' => 50,
                                '-' => 170,
                            },
                            'e' => 66,
                            'd' => match ($data[$c - 13]) {
                                't' => 82,
                                'g' => 255,
                            },
                            '-' => match ($data[$c - 13]) {
                                'v' => 124,
                                'g' => 258,
                            },
                            'g' => 201,
                            'm' => 205,
                            'r' => match ($data[$c - 13]) {
                                't' => 218,
                                'i' => 229,
                            },
                            'i' => match ($data[$c - 13]) {
                                'g' => 236,
                                'i' => 251,
                            },
                            'a' => 253,
                        },
                        'n' => match ($data[$c - 18]) {
                            'n' => 21,
                            't' => 29,
                            'e' => 136,
                            'c' => 206,
                        },
                        '-' => match ($data[$c - 18]) {
                            'l' => match ($data[$c - 13]) {
                                'a' => 22,
                                's' => 40,
                            },
                            'g' => 43,
                            'a' => 59,
                            '-' => 69,
                            'r' => match ($data[$c - 13]) {
                                'a' => 71,
                                '-' => 125,
                                'o' => 129,
                            },
                            'p' => 92,
                            'e' => match ($data[$c - 13]) {
                                '-' => 96,
                                'd' => 207,
                            },
                            'u' => match ($data[$c - 13]) {
                                'n' => 131,
                                'q' => 134,
                            },
                            'h' => 155,
                            'i' => 172,
                            'm' => 182,
                            's' => 243,
                        },
                        'h' => match ($data[$c - 18]) {
                            'f' => 24,
                            '4' => 87,
                            'i' => match ($data[$c - 13]) {
                                'y' => 95,
                                'o' => match ($data[$c - 5]) {
                                    '-' => 158,
                                    'e' => 168,
                                    'h' => 226,
                                },
                            },
                            '-' => match ($data[$c - 13]) {
                                's' => 113,
                                '-' => 159,
                                'i' => 235,
                            },
                            'n' => 120,
                            '1' => match ($data[$c - 13]) {
                                'o' => 137,
                                '8' => 151,
                            },
                            '2' => 184,
                        },
                        'f' => 28,
                        'l' => match ($data[$c - 18]) {
                            'o' => match ($data[$c - 13]) {
                                'd' => 30,
                                'i' => match ($data[$c - 5]) {
                                    'o' => 38,
                                    'h' => 209,
                                },
                                '-' => 175,
                            },
                            'p' => match ($data[$c - 13]) {
                                '3' => 54,
                                '4' => 75,
                                '1' => 154,
                                '2' => 187,
                            },
                            'n' => match ($data[$c - 5]) {
                                'r' => 56,
                                'a' => 266,
                            },
                            's' => 61,
                            'e' => 68,
                            'a' => 83,
                            'v' => 130,
                            '-' => match ($data[$c - 5]) {
                                'n' => 161,
                                't' => 169,
                            },
                            'w' => 191,
                        },
                        'e' => match ($data[$c - 18]) {
                            'c' => 33,
                            'o' => match ($data[$c - 13]) {
                                'g' => match ($data[$c - 5]) {
                                    '-' => match ($data[$c - 2]) {
                                        '1' => 60,
                                        '2' => match ($data[$c - 1]) {
                                            '0' => 77,
                                            '1' => 127,
                                            '2' => 157,
                                            '3' => 194,
                                            '4' => 221,
                                        },
                                        'o' => 214,
                                    },
                                    'p' => 70,
                                    'e' => 103,
                                    't' => 104,
                                    'd' => 260,
                                },
                                'a' => match ($data[$c - 5]) {
                                    'n' => 90,
                                    '-' => match ($data[$c - 1]) {
                                        '1' => 133,
                                        '2' => 174,
                                        '3' => 210,
                                        '4' => 225,
                                        '5' => 247,
                                    },
                                },
                                't' => 244,
                            },
                            't' => match ($data[$c - 13]) {
                                '-' => 88,
                                'i' => 117,
                            },
                            's' => 89,
                            'a' => 98,
                            'n' => match ($data[$c - 13]) {
                                'v' => 102,
                                'r' => 250,
                                '-' => 264,
                            },
                            'u' => 139,
                            '-' => 219,
                        },
                        'd' => match ($data[$c - 18]) {
                            '-' => 37,
                            'o' => 65,
                            'a' => 147,
                            'e' => 186,
                        },
                        'b' => match ($data[$c - 18]) {
                            '/' => match ($data[$c - 13]) {
                                'y' => 39,
                                '8' => 115,
                                'o' => 142,
                                'i' => 162,
                                'r' => match ($data[$c - 1]) {
                                    '1' => 163,
                                    '2' => 164,
                                    '3' => 165,
                                    '4' => 166,
                                },
                                'e' => 222,
                                'a' => 230,
                            },
                            '-' => 45,
                            'n' => 176,
                        },
                        'p' => match ($data[$c - 18]) {
                            'p' => 76,
                            '8' => match ($data[$c - 13]) {
                                '8' => 84,
                                'o' => 100,
                            },
                        },
                        'y' => match ($data[$c - 18]) {
                            'o' => match ($data[$c - 1]) {
                                '1' => 135,
                                '3' => 204,
                            },
                            '-' => 249,
                        },
                        '8' => match ($data[$c - 13]) {
                            'l' => 146,
                            '-' => 148,
                        },
                    } + match ($data[$c + 4]) {
                        '1' => 0,
                        '2' => 372,
                        '3' => 744,
                        '4' => 1116,
                        '5' => 1488,
                        '6' => 1860,
                    } + match ($data[$c + 6]) {
                        '0' => 0,
                        '1' => 310,
                    } + match ($data[$c + 7]) {
                        '0' => -31,
                        '1' => 0,
                        '2' => 31,
                        '3' => 62,
                        '4' => 93,
                        '5' => 124,
                        '6' => 155,
                        '7' => 186,
                        '8' => 217,
                        '9' => 248,
                    } + match ($data[$c + 9]) {
                        '0' => 0,
                        '1' => 10,
                        '2' => 20,
                        '3' => 30,
                    } + match ($data[$c + 10]) {
                        '0' => -1,
                        '1' => 0,
                        '2' => 1,
                        '3' => 2,
                        '4' => 3,
                        '5' => 4,
                        '6' => 5,
                        '7' => 6,
                        '8' => 7,
                        '9' => 8,
                    }
                ]++;

                $o = $c + self::COMMA_TO_NEWLINE_OFFSET;
            }

            $start += $endData + 2;
        }

        fclose($handle);

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
                $this->worker($socketPair[0], $inputPath, $start, $end, count($indexToLink) * self::DATA_POINTS);
                exit(0);
            }
            socket_close($socketPair[0]);
            $sockets[] = $socketPair[1];

            $start = $end;
        }

        $order = $this->getOrder($handle, count($linkToIndex));
        fclose($handle);

        $results = array_fill(0, self::WORKERS, '');
        $res = null;

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
                    $r = igbinary_unserialize($results[$index]);
                    $results[$index] = null;
                    unset($remaining[array_search($socket, $remaining)]);
                    if ($res === null) {
                        $res = $r;
                    } else {
                        foreach ($r as $i => $v) {
                            $res[$i] += $v;
                        }
                    }
                    break;
                }
                $results[$index] .= $chunk;
            }
        }

        $this->writeResult($outputPath, $res, $order, $linkToIndex, $dates);
    }
}
