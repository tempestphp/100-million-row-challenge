<?php

namespace App;

use App\Commands\Visit;

final class Parser
{
    private const SOCKET_END_MARKER = '|||';
    private const SOCKET_END_MARKER_LENGTH = 3;
    private const COMBO_RECORD_WIDTH = 4;

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        // Pre-build slug mappings from known Visit URIs
        $chars = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';
        $indexToSlugId = [];
        $slugIdToIndex = [];
        $idToJsonHeader = [];
        foreach (Visit::all() as $i => $visit) {
            $slug = substr($visit->uri, 19);
            $id = $chars[(int)($i / 62)] . $chars[$i % 62];
            $indexToSlugId[$i] = $id;
            $slugIdToIndex[$id] = $i;
            $idToJsonHeader[$id] = '    "' . str_replace('/', '\\/', $slug) . '": {' . "\n";
        }
        $seenSlugMaxCount = count($indexToSlugId);
        $seenSlugsShmKey = 0x500000 + (getmypid() & 0x0fffff);
        $seenSlugsShmSize = 65536;
        $seenSlugsShm = shmop_open($seenSlugsShmKey, 'c', 0644, $seenSlugsShmSize);
        shmop_write($seenSlugsShm, "\0\0\0\0", 0);
        $seenSlugsPid = pcntl_fork();

        if ($seenSlugsPid === 0) {
            $seenSlugs = $this->preReadSeenSlugs(
                $inputPath,
                $indexToSlugId,
                $seenSlugMaxCount,
                5000 * 1024
            );
            $seenSlugsBlob = igbinary_serialize($seenSlugs);
            $seenSlugsBlobLength = strlen($seenSlugsBlob);
            shmop_write($seenSlugsShm, $seenSlugsBlob, 4);
            shmop_write($seenSlugsShm, pack('V', $seenSlugsBlobLength), 0);
            exit(0);
        }

        // Pre-build date-to-index mapping (7-char keys: Y-MM-DD, without "202" prefix)
        $dateToIndex = [];
        $indexToDate = [];
        $day = strtotime('2021-01-01');
        $end = strtotime('2026-03-02');
        $nextDateIndex = 0;
        while ($day <= $end) {
            $date = date('y-m-d', $day);
            $date = substr($date, 1);
            $dateToIndex[$date] = $nextDateIndex;
            $indexToDate[$nextDateIndex] = $date;
            $nextDateIndex++;
            $day += 86400;
        }
        // Prefill all slug×date combinations with 0
        $dateCount = $nextDateIndex;
        $comboCount = count($indexToSlugId) * $dateCount;
        $counts = array_fill(0, $comboCount, 0);
        $byteChars = '';
        for ($i = 0; $i < 256; $i++) {
            $byteChars .= chr($i);
        }

        $workers = 5;
        $segments = $this->splitSegments($inputPath, $workers);

        $parentSockets = [];
        $childSockets = [];

        for ($i = 0; $i < $workers; $i++) {
            [$parentSocket, $childSocket] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            $pid = pcntl_fork();

            if ($pid === 0) {
                foreach ($parentSockets as $ps) {
                    fclose($ps);
                }
                foreach ($childSockets as $cs) {
                    fclose($cs);
                }
                fclose($parentSocket);
                $this->childReaderRange($inputPath, $segments[$i][0], $segments[$i][1], $childSocket, $dateToIndex, $dateCount, $byteChars);
                fclose($childSocket);
                exit(0);
            }

            $parentSockets[] = $parentSocket;
            $childSockets[] = $childSocket;
            // $pids[] = $pid;
        }

        foreach ($childSockets as $cs) {
            fclose($cs);
        }

        $buffers = [];
        foreach ($parentSockets as $ps) {
            $buffers[(int) $ps] = '';
        }

        while ($parentSockets) {
            foreach ($parentSockets as $sock) {
                $chunk = fread($sock, (8192*40));
                $id = (int) $sock;
                $data = $buffers[$id] . $chunk;
                $len = strlen($data);
                $processable = $len - ($len % self::COMBO_RECORD_WIDTH);
                $buffers[$id] = substr($data, $processable);

                if ($processable > 0) {
                    foreach (unpack('V*', substr($data, 0, $processable)) as $comboId) {
                        $counts[$comboId]++;
                    }
                }

                if (
                    strlen($buffers[$id]) === self::SOCKET_END_MARKER_LENGTH
                    && $buffers[$id] === self::SOCKET_END_MARKER
                ) {
                    $parentSockets = array_values(array_filter($parentSockets, fn($s) => $s !== $sock));
                }
            }
        }

        $seenSlugsHeader = shmop_read($seenSlugsShm, 0, 4);
        $seenSlugsBlobLength = unpack('V', $seenSlugsHeader)[1];
        while ($seenSlugsBlobLength === 0) {
            usleep(50);
            $seenSlugsHeader = shmop_read($seenSlugsShm, 0, 4);
            $seenSlugsBlobLength = unpack('V', $seenSlugsHeader)[1];
        }
        $seenSlugs = igbinary_unserialize(shmop_read($seenSlugsShm, 4, $seenSlugsBlobLength));
        shmop_delete($seenSlugsShm);

        $this->custom_json_put_content($outputPath, $counts, $idToJsonHeader, $indexToDate, $seenSlugs, $slugIdToIndex, $dateCount);
    }
    protected function custom_json_put_content(string $outputPath, array &$counts, array &$idToJsonHeader, array &$indexToDate, array &$seenSlugs, array &$slugIdToIndex, int $dateCount): void
    {
        $fp = fopen($outputPath, 'w');
        fwrite($fp, "{\n");

        foreach ($seenSlugs as $slugId => $_) {
            $base = $slugIdToIndex[$slugId] * $dateCount;

            $slugJson = '';
            foreach ($indexToDate as $dateIndex => $date) {
                if ($count = $counts[$base + $dateIndex]) {
                    $slugJson .= "        \"202{$date}\": {$count},\n";
                }
            }

            if ($slugJson === '') continue;

            $strlen  = strlen($slugJson)-2;
            $slugJson[$strlen] = "\n";
            $slugJson[$strlen+1] = " ";
            fwrite($fp, $idToJsonHeader[$slugId] . $slugJson . "   },\n");
        }

        fseek($fp, -2, SEEK_CUR);
        fwrite($fp, "\n}");
        fclose($fp);
    }

    private function preReadSeenSlugs(string $inputPath, array &$indexToSlugId, int $seenSlugMaxCount, int $chunkBytes): array
    {
        $seenSlugs = [];
        if ($seenSlugMaxCount === 0 || $chunkBytes <= 0) {
            return $seenSlugs;
        }

        $file = fopen($inputPath, 'r');
        if ($file === false) {
            return $seenSlugs;
        }
        stream_set_read_buffer($file, 0);

        $leftover = '';
        $slugDateOffset = self::$slugDateOffset;

        if ($this->scanSeenSlugsChunk(
            $file,
            $indexToSlugId,
            $slugDateOffset,
            $seenSlugs,
            $seenSlugMaxCount,
            $leftover,
            $chunkBytes,
        )) {
            fclose($file);
            return $seenSlugs;
        }

        while (count($seenSlugs) !== $seenSlugMaxCount && !feof($file)) {
            if ($this->scanSeenSlugsChunk(
                $file,
                $indexToSlugId,
                $slugDateOffset,
                $seenSlugs,
                $seenSlugMaxCount,
                $leftover,
                $chunkBytes,
            )) {
                break;
            }
        }

        fclose($file);
        return $seenSlugs;
    }

    private function scanSeenSlugsChunk(
        $file,
        array &$indexToSlugId,
        array &$slugDateOffset,
        array &$seenSlugs,
        int $seenSlugMaxCount,
        string &$leftover,
        int $chunkBytes,
    ): bool {
        $remaining = $chunkBytes;
        while ($remaining > 0 && ($chunk = fread($file, min(327680, $remaining))) !== '' && $chunk !== false) {
            $remaining -= strlen($chunk);
            $chunk = $leftover . $chunk;
            $lastNewline = strrpos($chunk, "\n");

            if ($lastNewline === false) {
                $leftover = $chunk;
                continue;
            }

            $leftover = substr($chunk, $lastNewline + 1);
            $location = 26;
            $lastNewlineMinusOne = $lastNewline - 1;
            $p = $location - 1;

            while ($p < $lastNewlineMinusOne) {
                $index = match($chunk[$p+4]) {
                    'h' => match($chunk[$p+0]) {
                        'w' => 0,
                        'b' => 147,
                        's' => 202,
                    },
                    'l' => match($chunk[$p+10]) {
                        'e' => match($chunk[$p+32]) {
                            '1' => 1,
                            '2' => 2,
                        },
                        'l' => 6,
                        'r' => 21,
                        'y' => 23,
                        '-' => 24,
                        'd' => 95,
                        't' => 109,
                        'h' => 128,
                    },
                    'e' => match($chunk[$p+8]) {
                        't' => match($chunk[$p+0]) {
                            'i' => 3,
                            'r' => 149,
                        },
                        'f' => 12,
                        'g' => 16,
                        'c' => match($chunk[$p+0]) {
                            'w' => 22,
                            'd' => 103,
                        },
                        'b' => match($chunk[$p+13]) {
                            'a' => 40,
                            'c' => 43,
                            'b' => 45,
                            's' => 48,
                        },
                        's' => match($chunk[$p+9]) {
                            '-' => 120,
                            ',' => 121,
                        },
                        '-' => 171,
                        'e' => 180,
                        'i' => match($chunk[$p+0]) {
                            'd' => 186,
                            't' => 222,
                        },
                        'a' => 207,
                        'o' => 244,
                        'd' => 246,
                        'n' => 264,
                    },
                    'i' => match($chunk[$p+10]) {
                        'e' => match($chunk[$p+0]) {
                            's' => 4,
                            'c' => 82,
                        },
                        'n' => 31,
                        'c' => 33,
                        '-' => match($chunk[$p+12]) {
                            '3' => match($chunk[$p+11]) {
                                '7' => 44,
                                '8' => 213,
                            },
                            '4' => match($chunk[$p+11]) {
                                '7' => 74,
                                '8' => 240,
                            },
                            'n' => 106,
                            ',' => 114,
                            '1' => 152,
                            '2' => 190,
                            'o' => match($chunk[$p+0]) {
                                'a' => 191,
                                '1' => 267,
                            },
                            '5' => 259,
                        },
                        '9' => 60,
                        's' => match($chunk[$p+0]) {
                            'c' => 71,
                            's' => 243,
                        },
                        '0' => 77,
                        'o' => match($chunk[$p+0]) {
                            'b' => 79,
                            'p' => 215,
                        },
                        'f' => 86,
                        'l' => match($chunk[$p+0]) {
                            'm' => 90,
                            'j' => 96,
                        },
                        'i' => match($chunk[$p+0]) {
                            'w' => 117,
                            'o' => 132,
                        },
                        '1' => match($chunk[$p+11]) {
                            ',' => 127,
                            '-' => 162,
                        },
                        'a' => match($chunk[$p+0]) {
                            'c' => match($chunk[$p+36]) {
                                '1' => 135,
                                '3' => 204,
                            },
                            'u' => 239,
                        },
                        '2' => 157,
                        ',' => 158,
                        't' => match($chunk[$p+16]) {
                            'e' => 161,
                            'r' => 169,
                            ',' => 220,
                            'p' => 257,
                        },
                        'u' => 170,
                        '3' => 194,
                        '4' => 221,
                        'm' => 237,
                    },
                    'c' => match($chunk[$p+12]) {
                        'h' => match($chunk[$p+15]) {
                            '4' => 5,
                            '5' => 7,
                        },
                        'a' => match($chunk[$p+14]) {
                            '1' => 9,
                            '2' => 17,
                            'k' => 197,
                        },
                        't' => 13,
                        '-' => match($chunk[$p+0]) {
                            'a' => 20,
                            't' => 107,
                        },
                        'u' => 42,
                        'n' => 173,
                        'i' => 189,
                        'r' => 224,
                        's' => 252,
                        'e' => 255,
                    },
                    'g' => match($chunk[$p+19]) {
                        'y' => 8,
                        'n' => 67,
                        'o' => 124,
                        'r' => 182,
                        'a' => 206,
                        'i' => 238,
                        'w' => 251,
                    },
                    'y' => match($chunk[$p+6]) {
                        'o' => 10,
                        'm' => 39,
                        'i' => 49,
                        'd' => 55,
                        'c' => 83,
                        'u' => 160,
                        'f' => 229,
                    },
                    'o' => match($chunk[$p+9]) {
                        'c' => match($chunk[$p+0]) {
                            'p' => 11,
                            'r' => 185,
                            'v' => 254,
                        },
                        'e' => match($chunk[$p+18]) {
                            'a' => 14,
                            'd' => 25,
                            'm' => 139,
                        },
                        'm' => 15,
                        'd' => 30,
                        '-' => 62,
                        'g' => 66,
                        'n' => match($chunk[$p+0]) {
                            'i' => 88,
                            'a' => 119,
                            'n' => 263,
                        },
                        'y' => 102,
                        't' => 142,
                        'l' => 231,
                        'o' => 250,
                    },
                    't' => match($chunk[$p+6]) {
                        'r' => match($chunk[$p+9]) {
                            'p' => match($chunk[$p+20]) {
                                ',' => 18,
                                '-' => match($chunk[$p+21]) {
                                    'i' => 28,
                                    'o' => 41,
                                },
                            },
                            't' => 26,
                            's' => 108,
                            'o' => 159,
                            'x' => 223,
                        },
                        'm' => 52,
                        'n' => match($chunk[$p+11]) {
                            'n' => 57,
                            'p' => 61,
                        },
                        'c' => match($chunk[$p+20]) {
                            'p' => 58,
                            '-' => 179,
                            ',' => 201,
                        },
                        'e' => match($chunk[$p+24]) {
                            ',' => 68,
                            '-' => 69,
                        },
                        'p' => 70,
                        'd' => match($chunk[$p+0]) {
                            'e' => 80,
                            'l' => 205,
                        },
                        'u' => 93,
                        'a' => 99,
                        't' => 104,
                        'l' => 111,
                        '-' => 155,
                        'g' => 168,
                    },
                    'm' => match($chunk[$p+8]) {
                        'd' => 19,
                        'i' => 138,
                        '-' => 235,
                    },
                    '-' => match($chunk[$p+12]) {
                        '-' => match($chunk[$p+0]) {
                            'w' => 27,
                            'i' => 211,
                        },
                        'u' => 46,
                        'e' => match($chunk[$p+6]) {
                            'r' => 63,
                            'b' => 134,
                            'e' => 208,
                        },
                        'i' => match($chunk[$p+25]) {
                            ',' => 89,
                            '-' => 92,
                        },
                        'y' => 91,
                        'c' => 105,
                        'p' => match($chunk[$p+0]) {
                            'w' => 110,
                            'h' => 228,
                        },
                        'a' => 116,
                        'o' => match($chunk[$p+5]) {
                            'w' => 122,
                            'e' => 125,
                            'a' => 131,
                        },
                        '8' => 177,
                        't' => 195,
                        ',' => 200,
                        'r' => 248,
                        'g' => 258,
                        's' => 262,
                    },
                    'n' => match($chunk[$p+8]) {
                        'c' => 29,
                        '-' => match($chunk[$p+0]) {
                            'o' => 38,
                            'a' => 218,
                        },
                        'i' => 144,
                        'd' => 172,
                        ',' => 199,
                        'n' => 214,
                        'o' => 232,
                        'v' => 234,
                        'e' => 265,
                    },
                    'a' => match($chunk[$p+0]) {
                        'v' => 32,
                        'c' => 47,
                        'p' => 188,
                        'u' => 192,
                        'i' => 230,
                        'a' => 249,
                    },
                    'w' => match($chunk[$p+12]) {
                        '0' => 34,
                        'n' => 98,
                        'm' => 129,
                        'v' => 130,
                        'e' => 227,
                    },
                    'u' => match($chunk[$p+1]) {
                        'l' => 35,
                        'n' => 50,
                        'v' => 175,
                    },
                    'v' => match($chunk[$p+8]) {
                        'v' => match($chunk[$p+19]) {
                            ',' => 36,
                            '-' => 37,
                        },
                        'q' => 53,
                        'h' => 72,
                        'c' => 73,
                        'i' => match($chunk[$p+26]) {
                            '1' => 133,
                            '2' => match($chunk[$p+19]) {
                                'a' => match($chunk[$p+29]) {
                                    '2' => 156,
                                    '3' => 193,
                                    '4' => 216,
                                    '5' => 241,
                                },
                                'u' => 174,
                            },
                            '3' => 210,
                            '4' => 225,
                            '5' => 247,
                        },
                        '-' => 136,
                        ',' => 212,
                        'e' => 245,
                    },
                    'j' => 51,
                    '7' => match($chunk[$p+5]) {
                        '3' => 54,
                        '4' => match($chunk[$p+7]) {
                            'u' => 75,
                            'i' => 87,
                        },
                    },
                    'f' => match($chunk[$p+0]) {
                        'u' => 56,
                        't' => 217,
                    },
                    'd' => match($chunk[$p+6]) {
                        'i' => 59,
                        'p' => 65,
                        'r' => 85,
                        '-' => 141,
                        'a' => 143,
                        'n' => match($chunk[$p+11]) {
                            'c' => 219,
                            'f' => 236,
                        },
                        'd' => 260,
                    },
                    's' => match($chunk[$p+0]) {
                        't' => 64,
                        'e' => 78,
                        'h' => 123,
                        's' => match($chunk[$p+7]) {
                            's' => 196,
                            'i' => 256,
                        },
                        'y' => 226,
                        'w' => 253,
                    },
                    'p' => match($chunk[$p+5]) {
                        'r' => 76,
                        'e' => 178,
                    },
                    'r' => match($chunk[$p+0]) {
                        'm' => 81,
                        'a' => 94,
                        'p' => match($chunk[$p+14]) {
                            ',' => 118,
                            '-' => 181,
                        },
                        'f' => 126,
                        't' => match($chunk[$p+3]) {
                            '-' => 140,
                            'o' => 242,
                        },
                        'g' => match($chunk[$p+16]) {
                            'v' => 150,
                            '1' => 163,
                            '2' => 164,
                            '3' => 165,
                            '4' => 166,
                        },
                        'u' => 176,
                        'o' => 209,
                    },
                    '8' => match($chunk[$p+9]) {
                        '8' => 84,
                        'c' => 97,
                        'o' => 100,
                        'e' => 101,
                        '-' => match($chunk[$p+5]) {
                            '-' => 112,
                            '1' => 151,
                            '2' => 184,
                            '4' => 233,
                        },
                        'l' => 113,
                        'r' => match($chunk[$p+5]) {
                            '-' => 115,
                            '1' => 153,
                            '6' => 266,
                        },
                        'f' => 137,
                        'a' => 146,
                        'w' => 148,
                        'g' => match($chunk[$p+5]) {
                            '1' => 154,
                            '2' => 187,
                        },
                    },
                    'k' => match($chunk[$p+0]) {
                        'm' => 145,
                        't' => 203,
                    },
                    'b' => 167,
                    ',' => 183,
                    '0' => 198,
                    '2' => 261,
                };
                $seenSlugs[$indexToSlugId[$index]] = true;
                $p += $slugDateOffset[$index]+49;

                if(count($seenSlugs) === $seenSlugMaxCount){
                    return true;
                }
            }
        }

        return count($seenSlugs) === $seenSlugMaxCount;
    }

    private function childReaderRange(string $inputPath, int $start, int $end, $childSocket, array &$dateToIndex, int $dateCount, string &$byteChars): void
    {
        $file = fopen($inputPath, 'r');
        stream_set_read_buffer($file, 327680);
//        stream_set_write_buffer($childSocket, 1024 * 1024);

        // Read file in large chunks, identify slugs via decision-tree matcher
        // Each line: https://stitcher.io/blog/slug,2022-09-10T13:55:25+00:00\n
        // Matcher identifies slug in 1-5 char checks, gives us the date position
        $leftover = '';
        $slugDateOffset = self::$slugDateOffset;

        fseek($file, $start);
        $remaining = $end - $start;

        while ($remaining > 0 && ($chunk = fread($file, min(327680, $remaining))) !== '' && $chunk !== false) {
            $remaining -= strlen($chunk);
            $chunk = $leftover . $chunk;
            $lastNewline = strrpos($chunk, "\n");

            if ($lastNewline === false) {
                $leftover = $chunk;
                continue;
            }

            $leftover = substr($chunk, $lastNewline + 1);
            $location = 26;
            $blob = '';
            $lastNewlineMinusOne = $lastNewline - 1;
            $p = $location - 1;

            while ($p < $lastNewlineMinusOne) {
                $index = match($chunk[$p+4]) {
                    'h' => match($chunk[$p+0]) {
                        'w' => 0,
                        'b' => 147,
                        's' => 202,
                    },
                    'l' => match($chunk[$p+10]) {
                        'e' => match($chunk[$p+32]) {
                            '1' => 1,
                            '2' => 2,
                        },
                        'l' => 6,
                        'r' => 21,
                        'y' => 23,
                        '-' => 24,
                        'd' => 95,
                        't' => 109,
                        'h' => 128,
                    },
                    'e' => match($chunk[$p+8]) {
                        't' => match($chunk[$p+0]) {
                            'i' => 3,
                            'r' => 149,
                        },
                        'f' => 12,
                        'g' => 16,
                        'c' => match($chunk[$p+0]) {
                            'w' => 22,
                            'd' => 103,
                        },
                        'b' => match($chunk[$p+13]) {
                            'a' => 40,
                            'c' => 43,
                            'b' => 45,
                            's' => 48,
                        },
                        's' => match($chunk[$p+9]) {
                            '-' => 120,
                            ',' => 121,
                        },
                        '-' => 171,
                        'e' => 180,
                        'i' => match($chunk[$p+0]) {
                            'd' => 186,
                            't' => 222,
                        },
                        'a' => 207,
                        'o' => 244,
                        'd' => 246,
                        'n' => 264,
                    },
                    'i' => match($chunk[$p+10]) {
                        'e' => match($chunk[$p+0]) {
                            's' => 4,
                            'c' => 82,
                        },
                        'n' => 31,
                        'c' => 33,
                        '-' => match($chunk[$p+12]) {
                            '3' => match($chunk[$p+11]) {
                                '7' => 44,
                                '8' => 213,
                            },
                            '4' => match($chunk[$p+11]) {
                                '7' => 74,
                                '8' => 240,
                            },
                            'n' => 106,
                            ',' => 114,
                            '1' => 152,
                            '2' => 190,
                            'o' => match($chunk[$p+0]) {
                                'a' => 191,
                                '1' => 267,
                            },
                            '5' => 259,
                        },
                        '9' => 60,
                        's' => match($chunk[$p+0]) {
                            'c' => 71,
                            's' => 243,
                        },
                        '0' => 77,
                        'o' => match($chunk[$p+0]) {
                            'b' => 79,
                            'p' => 215,
                        },
                        'f' => 86,
                        'l' => match($chunk[$p+0]) {
                            'm' => 90,
                            'j' => 96,
                        },
                        'i' => match($chunk[$p+0]) {
                            'w' => 117,
                            'o' => 132,
                        },
                        '1' => match($chunk[$p+11]) {
                            ',' => 127,
                            '-' => 162,
                        },
                        'a' => match($chunk[$p+0]) {
                            'c' => match($chunk[$p+36]) {
                                '1' => 135,
                                '3' => 204,
                            },
                            'u' => 239,
                        },
                        '2' => 157,
                        ',' => 158,
                        't' => match($chunk[$p+16]) {
                            'e' => 161,
                            'r' => 169,
                            ',' => 220,
                            'p' => 257,
                        },
                        'u' => 170,
                        '3' => 194,
                        '4' => 221,
                        'm' => 237,
                    },
                    'c' => match($chunk[$p+12]) {
                        'h' => match($chunk[$p+15]) {
                            '4' => 5,
                            '5' => 7,
                        },
                        'a' => match($chunk[$p+14]) {
                            '1' => 9,
                            '2' => 17,
                            'k' => 197,
                        },
                        't' => 13,
                        '-' => match($chunk[$p+0]) {
                            'a' => 20,
                            't' => 107,
                        },
                        'u' => 42,
                        'n' => 173,
                        'i' => 189,
                        'r' => 224,
                        's' => 252,
                        'e' => 255,
                    },
                    'g' => match($chunk[$p+19]) {
                        'y' => 8,
                        'n' => 67,
                        'o' => 124,
                        'r' => 182,
                        'a' => 206,
                        'i' => 238,
                        'w' => 251,
                    },
                    'y' => match($chunk[$p+6]) {
                        'o' => 10,
                        'm' => 39,
                        'i' => 49,
                        'd' => 55,
                        'c' => 83,
                        'u' => 160,
                        'f' => 229,
                    },
                    'o' => match($chunk[$p+9]) {
                        'c' => match($chunk[$p+0]) {
                            'p' => 11,
                            'r' => 185,
                            'v' => 254,
                        },
                        'e' => match($chunk[$p+18]) {
                            'a' => 14,
                            'd' => 25,
                            'm' => 139,
                        },
                        'm' => 15,
                        'd' => 30,
                        '-' => 62,
                        'g' => 66,
                        'n' => match($chunk[$p+0]) {
                            'i' => 88,
                            'a' => 119,
                            'n' => 263,
                        },
                        'y' => 102,
                        't' => 142,
                        'l' => 231,
                        'o' => 250,
                    },
                    't' => match($chunk[$p+6]) {
                        'r' => match($chunk[$p+9]) {
                            'p' => match($chunk[$p+20]) {
                                ',' => 18,
                                '-' => match($chunk[$p+21]) {
                                    'i' => 28,
                                    'o' => 41,
                                },
                            },
                            't' => 26,
                            's' => 108,
                            'o' => 159,
                            'x' => 223,
                        },
                        'm' => 52,
                        'n' => match($chunk[$p+11]) {
                            'n' => 57,
                            'p' => 61,
                        },
                        'c' => match($chunk[$p+20]) {
                            'p' => 58,
                            '-' => 179,
                            ',' => 201,
                        },
                        'e' => match($chunk[$p+24]) {
                            ',' => 68,
                            '-' => 69,
                        },
                        'p' => 70,
                        'd' => match($chunk[$p+0]) {
                            'e' => 80,
                            'l' => 205,
                        },
                        'u' => 93,
                        'a' => 99,
                        't' => 104,
                        'l' => 111,
                        '-' => 155,
                        'g' => 168,
                    },
                    'm' => match($chunk[$p+8]) {
                        'd' => 19,
                        'i' => 138,
                        '-' => 235,
                    },
                    '-' => match($chunk[$p+12]) {
                        '-' => match($chunk[$p+0]) {
                            'w' => 27,
                            'i' => 211,
                        },
                        'u' => 46,
                        'e' => match($chunk[$p+6]) {
                            'r' => 63,
                            'b' => 134,
                            'e' => 208,
                        },
                        'i' => match($chunk[$p+25]) {
                            ',' => 89,
                            '-' => 92,
                        },
                        'y' => 91,
                        'c' => 105,
                        'p' => match($chunk[$p+0]) {
                            'w' => 110,
                            'h' => 228,
                        },
                        'a' => 116,
                        'o' => match($chunk[$p+5]) {
                            'w' => 122,
                            'e' => 125,
                            'a' => 131,
                        },
                        '8' => 177,
                        't' => 195,
                        ',' => 200,
                        'r' => 248,
                        'g' => 258,
                        's' => 262,
                    },
                    'n' => match($chunk[$p+8]) {
                        'c' => 29,
                        '-' => match($chunk[$p+0]) {
                            'o' => 38,
                            'a' => 218,
                        },
                        'i' => 144,
                        'd' => 172,
                        ',' => 199,
                        'n' => 214,
                        'o' => 232,
                        'v' => 234,
                        'e' => 265,
                    },
                    'a' => match($chunk[$p+0]) {
                        'v' => 32,
                        'c' => 47,
                        'p' => 188,
                        'u' => 192,
                        'i' => 230,
                        'a' => 249,
                    },
                    'w' => match($chunk[$p+12]) {
                        '0' => 34,
                        'n' => 98,
                        'm' => 129,
                        'v' => 130,
                        'e' => 227,
                    },
                    'u' => match($chunk[$p+1]) {
                        'l' => 35,
                        'n' => 50,
                        'v' => 175,
                    },
                    'v' => match($chunk[$p+8]) {
                        'v' => match($chunk[$p+19]) {
                            ',' => 36,
                            '-' => 37,
                        },
                        'q' => 53,
                        'h' => 72,
                        'c' => 73,
                        'i' => match($chunk[$p+26]) {
                            '1' => 133,
                            '2' => match($chunk[$p+19]) {
                                'a' => match($chunk[$p+29]) {
                                    '2' => 156,
                                    '3' => 193,
                                    '4' => 216,
                                    '5' => 241,
                                },
                                'u' => 174,
                            },
                            '3' => 210,
                            '4' => 225,
                            '5' => 247,
                        },
                        '-' => 136,
                        ',' => 212,
                        'e' => 245,
                    },
                    'j' => 51,
                    '7' => match($chunk[$p+5]) {
                        '3' => 54,
                        '4' => match($chunk[$p+7]) {
                            'u' => 75,
                            'i' => 87,
                        },
                    },
                    'f' => match($chunk[$p+0]) {
                        'u' => 56,
                        't' => 217,
                    },
                    'd' => match($chunk[$p+6]) {
                        'i' => 59,
                        'p' => 65,
                        'r' => 85,
                        '-' => 141,
                        'a' => 143,
                        'n' => match($chunk[$p+11]) {
                            'c' => 219,
                            'f' => 236,
                        },
                        'd' => 260,
                    },
                    's' => match($chunk[$p+0]) {
                        't' => 64,
                        'e' => 78,
                        'h' => 123,
                        's' => match($chunk[$p+7]) {
                            's' => 196,
                            'i' => 256,
                        },
                        'y' => 226,
                        'w' => 253,
                    },
                    'p' => match($chunk[$p+5]) {
                        'r' => 76,
                        'e' => 178,
                    },
                    'r' => match($chunk[$p+0]) {
                        'm' => 81,
                        'a' => 94,
                        'p' => match($chunk[$p+14]) {
                            ',' => 118,
                            '-' => 181,
                        },
                        'f' => 126,
                        't' => match($chunk[$p+3]) {
                            '-' => 140,
                            'o' => 242,
                        },
                        'g' => match($chunk[$p+16]) {
                            'v' => 150,
                            '1' => 163,
                            '2' => 164,
                            '3' => 165,
                            '4' => 166,
                        },
                        'u' => 176,
                        'o' => 209,
                    },
                    '8' => match($chunk[$p+9]) {
                        '8' => 84,
                        'c' => 97,
                        'o' => 100,
                        'e' => 101,
                        '-' => match($chunk[$p+5]) {
                            '-' => 112,
                            '1' => 151,
                            '2' => 184,
                            '4' => 233,
                        },
                        'l' => 113,
                        'r' => match($chunk[$p+5]) {
                            '-' => 115,
                            '1' => 153,
                            '6' => 266,
                        },
                        'f' => 137,
                        'a' => 146,
                        'w' => 148,
                        'g' => match($chunk[$p+5]) {
                            '1' => 154,
                            '2' => 187,
                        },
                    },
                    'k' => match($chunk[$p+0]) {
                        'm' => 145,
                        't' => 203,
                    },
                    'b' => 167,
                    ',' => 183,
                    '0' => 198,
                    '2' => 261,
                };
                $p += $slugDateOffset[$index] + 1;
                $comboId = ($index * $dateCount) + $dateToIndex[substr($chunk, $p, 7)];
                $blob .= $byteChars[$comboId & 255]
                    . $byteChars[($comboId >> 8) & 255]
                    . $byteChars[($comboId >> 16) & 255]
                    . $byteChars[($comboId >> 24) & 255];
                $p += 48;
            }

            if ($remaining === 0) {
                fwrite($childSocket, $blob . self::SOCKET_END_MARKER);
                break;
            }

            fwrite($childSocket, $blob);
        }

        fclose($file);
    }
    // Date position offset from $p for each slug index
    // (slug_length + 1 comma + 3 to skip "202" prefix)
    private function splitSegments(string $inputPath, int $workers): array
    {
        $fileSize = filesize($inputPath);
        if ($fileSize === false || $workers <= 1) {
            return [[0, $fileSize ?: 0]];
        }

        $segments = [];
        $chunkSize = (int) floor($fileSize / $workers);

        $fh = fopen($inputPath, 'r');
        if ($fh === false) {
            return [[0, $fileSize]];
        }

        $start = 0;
        for ($i = 0; $i < $workers; $i++) {
            if ($i === $workers - 1) {
                $segments[] = [$start, $fileSize];
                break;
            }

            $boundary = $start + $chunkSize;
            if ($boundary >= $fileSize) {
                $segments[] = [$start, $fileSize];
                break;
            }

            fseek($fh, $boundary);
            $line = fgets($fh);
            if ($line === false) {
                $segments[] = [$start, $fileSize];
                break;
            }

            $end = ftell($fh);
            $segments[] = [$start, $end];
            $start = $end;
        }

        fclose($fh);
        return $segments;
    }

    private static array $slugDateOffset = [25, 36, 36, 19, 26, 19, 26, 19, 36, 18, 33, 42, 16, 29, 38, 31, 25, 18, 23, 27, 19, 41, 32, 22, 32, 31, 32, 18, 37, 37, 25, 24, 28, 31, 18, 23, 22, 40, 21, 20, 30, 36, 32, 32, 16, 33, 32, 18, 36, 27, 23, 10, 29, 28, 21, 39, 34, 24, 24, 45, 14, 21, 22, 42, 18, 29, 23, 43, 27, 40, 14, 28, 27, 34, 16, 21, 25, 14, 22, 26, 19, 34, 48, 21, 25, 51, 18, 26, 31, 28, 41, 28, 36, 33, 37, 38, 36, 24, 34, 31, 25, 24, 33, 14, 14, 17, 22, 35, 18, 51, 28, 17, 18, 26, 15, 20, 34, 26, 17, 30, 26, 12, 32, 10, 29, 35, 30, 14, 15, 47, 48, 29, 24, 30, 29, 40, 26, 26, 36, 35, 18, 17, 20, 41, 31, 16, 29, 33, 29, 19, 24, 26, 16, 34, 21, 34, 33, 14, 13, 33, 17, 28, 20, 20, 20, 20, 20, 10, 13, 28, 38, 23, 34, 19, 30, 28, 40, 17, 34, 34, 42, 24, 36, 7, 26, 29, 25, 21, 16, 33, 16, 27, 22, 33, 14, 18, 11, 37, 32, 11, 15, 23, 12, 19, 40, 23, 28, 37, 17, 21, 30, 18, 11, 16, 14, 19, 33, 42, 40, 50, 19, 14, 20, 15, 19, 30, 13, 30, 19, 23, 20, 24, 32, 18, 24, 41, 23, 29, 43, 19, 16, 33, 24, 31, 29, 17, 30, 30, 26, 27, 26, 23, 16, 23, 16, 23, 25, 30, 25, 16, 14, 11, 25, 15, 29, 24, 38, 29];

}
