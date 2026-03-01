<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $stream = fopen($inputPath, 'rb');

        $results = [];

        while (($buffer = fgets($stream, 4096)) !== false) {
            $commaPos = strpos($buffer, ',');
            $url      = substr($buffer, 19, $commaPos - 19);
            $date     = substr($buffer, $commaPos + 1, 10);

            if (!isset($results[$url])) {
                $results[$url] = [];
            }

            if (!isset($results[$url][$date])) {
                $results[$url][$date] = 1;
                continue;
            }

            $results[$url][$date]++;
        }

        fclose($stream);

        $stream = fopen($outputPath, 'wb');

        fwrite($stream, "{\n");

        fwrite(
            $stream,
            implode(
                ",\n",
                array_map(
                    static function ($url, $dates) {
                        $url = str_replace("/", "\/", $url);
                        ksort($dates);
                        return
                            "    \"{$url}\": {\n" .
                            implode(
                                ",\n",
                                array_map(
                                    static function ($date, $value) {
                                        return "        \"{$date}\": {$value}";
                                    },
                                    array_keys((array) $dates),
                                    array_values((array) $dates),
                                ),
                            ) .
                            "\n    }"
                        ;
                    },
                    array_keys($results),
                    array_values($results),
                ),
            ),
        );

        fwrite($stream, "\n}");
        fclose($stream);
    }
}
