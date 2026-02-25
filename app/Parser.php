<?php

namespace App;

final class Parser
{
  public function parse(string $inputPath, string $outputPath): void
  {
    $handle = fopen($inputPath, "r");

    if ($handle) {
      while (($row = fgets($handle)) !== false) {
        // skip empty rows
        if ($row == [null]) continue;

        [$url, $date] = explode(',', $row);

        // TODO: Bottleneck
        // encode path as json and push into data array
        $path = parse_url($url, PHP_URL_PATH);
        // $path = $url;

        if (!isset($data[$path])) {
          $data[$path] = [];
        }

        // parse date 
        $date_str = strtotime($date);
        $date_record = date('Y-m-d', $date_str);

        // nested upsert value in subarray
        $data[$path][$date_record] = ($data[$path][$date_record] ?? 0) + 1;

        ksort($data[$path]);
      }
    }

    file_put_contents($outputPath, json_encode($data, JSON_PRETTY_PRINT));

    fclose($handle);
  }
}

// 1,000 iterations
// 1772053169,main,0.0024039745330811
