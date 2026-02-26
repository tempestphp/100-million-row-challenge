<?php

namespace App;

final class Parser
{
  public function parse(string $inputPath, string $outputPath): void
  {
    $handle = fopen($inputPath, "r");
    $data = [];

    while (($row = fgetcsv($handle, 0, ',', '"', '\\')) !== false) {
      [$url, $date] = $row;
      $path = parse_url($url, PHP_URL_PATH);

      // parse date from beginning of string up to 'T'
      $date_record = substr($date, 0, strpos($date, 'T'));

      // nested upsert value in subarray
      $data[$path][$date_record] = ($data[$path][$date_record] ?? 0) + 1;
    }

    fclose($handle);

    // sort date subarrays asc
    foreach ($data as &$record) {
      ksort($record);
    }

    file_put_contents($outputPath, json_encode($data, JSON_PRETTY_PRINT));
  }
}
