<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $handle = fopen($inputPath, 'r');

        $urlIds = [];
        $urls = [];
        $nextId = 0;

        $counts = [];

        $buffer = '';
        $chunkSize = 5 * 1024 * 1024;

        while (!feof($handle)) {
            $buffer .= fread($handle, $chunkSize);

            $lines = explode("\n", $buffer);
            $buffer = array_pop($lines);

            foreach ($lines as $row) {
                if ($row === '') {
                    continue;
                }

                $comma = strpos($row, ',');

                $url = substr($row, 19, $comma - 19);
                $date = substr($row, $comma + 1, 10);

                if (!isset($urlIds[$url])) {
                    $urlIds[$url] = $nextId;
                    $urls[$nextId] = $url;
                    $nextId++;
                }

                $id = $urlIds[$url];

                $counts[$id][$date] = ($counts[$id][$date] ?? 0) + 1;
            }
        }

        fclose($handle);

        $json = "{\n";
        $firstUrl = true;

        foreach ($counts as $id => $dates) {
            if (! $firstUrl) {
                $json .= ",\n";
            }
            $firstUrl = false;

            ksort($dates);

            $json .= "    " . json_encode($urls[$id]) . ": {\n";

            $firstDate = true;

            foreach ($dates as $date => $count) {
                if (! $firstDate) {
                    $json .= ",\n";
                }
                $firstDate = false;

                $json .= "        \"$date\": $count";
            }

            $json .= "\n    }";
        }

        $json .= "\n}";

        file_put_contents($outputPath, $json);
    }
}