<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        try {
            $input = \fopen($inputPath, 'r');
            $output = \fopen($outputPath, 'w');

            $counters = [];

            while ($line = \fgets($input)) {
                $uri = substr($line, 19, -27);
                $date = substr($line, -26, 10);

                if (! isset($counters[$uri])) {
                    $counters[$uri] = [$date => 0];
                } elseif (! isset($counters[$uri][$date])) {
                    $counters[$uri][$date] = 0;
                }
                $counters[$uri][$date]++;
            }

            $lastUri = null;
            $lastDate = null;
            \fwrite($output, "{");
            foreach ($counters as $uri => $dates) {
                ksort($dates);
                foreach ($dates as $date => $count) {
                    if ($lastUri !== $uri) {
                        if ($lastUri !== null) {
                            \fwrite($output, "\n    },");
                        }
                        $lastUri = $uri;
                        $lastDate = null;
                        \fwrite($output, "\n    \"". str_replace('/', '\\/', $uri) . "\": {");
                    }
                    if ($lastDate !== null) {
                        \fwrite($output, ",");
                    }
                    $lastDate = $date;
                    \fwrite($output, "\n        \"$date\": $count");
                }
            }
            \fwrite($output, "\n    }\n}");
        } finally {
            @\fclose($input);
            @\fclose($output);
        }
    }
}
