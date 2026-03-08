<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $results = [];
        $output = [];
        $f = fopen($inputPath, 'r');
        $wf = fopen($outputPath, "w+");
        
        if ($f !== FALSE) {
            try {
                while (($data = fgetcsv($f, 1000, ",", '"', '\\')) != FALSE) {
                    if (!isset($data[0]) || !isset($data[1])) {
                        continue;
                    }
                    $date = explode("T", $data[1])[0];
                    $website = parse_url($data[0], PHP_URL_PATH);
                    if (!isset($results[$website])) {
                        $results[$website] = [];
                    }
                    if (!isset($results[$website][$date])) {
                        $results[$website][$date] = 1;
                    } else {
                        $results[$website][$date] += 1;
                    }
                }
                foreach($results as $site => $dates) {
                    $dates_ = array_keys($dates);
                    usort($dates_, function ($a, $b) {
                        return $a <=> $b;
                    });
                    $output[$site] = [];
                    foreach($dates_ as $date__) {
                        $output[$site][$date__] = $results[$site][$date__];
                    }
                }
                fwrite($wf, json_encode($output, JSON_PRETTY_PRINT));
                fclose($f);
                fclose($wf);
            } catch (Exception $e) {
                fclose($f);
                fclose($wf);
            }
        }
    }
}
