<?php

namespace App;

use Exception;
const BUF_SIZE = 65535;

final class Parser {
  private function read($inputFile) {
    $leaf = "";
    $fs = fopen($inputFile, "r");
    while (! feof($fs)) {
      $start = 0;
      $end = 0;
      $buffer = fread($fs, BUF_SIZE);
      while (true) {
        $end = strpos($buffer, "\n", $start);
        if ($end !== false) {
          if ($start === 0) {
            yield substr($leaf . $buffer, 19, strlen($leaf) + $end - $start - 34);
          } else {
            yield substr($buffer, $start + 19, $end - $start - 34);
          }
          $start = $end + 1;
        } else {
          $leaf = substr($buffer, $start);
          break;
        }
      }
    }
    fclose($fs);
  }

  public function parse(string $inputPath, string $outputPath): void {
    $result = Array();

    //$s = microtime(true);
    foreach ($this->read($inputPath) as $uri) {
      $len = strlen($uri);
      $url = substr($uri, 0, $len - 11);
      $date = substr($uri, $len - 10);

      if (!array_key_exists($url, $result)){
        $result[$url] = Array();
      }
      if (!array_key_exists($date, $result[$url])){
        $result[$url][$date] = 1;
      } else {
        $result[$url][$date]++;
      }
    }
    //printf("Parsing time: %fs\n", microtime(true) - $s);

    //$s = microtime(true);
    foreach($result as $k => $v) {
      ksort($result[$k], SORT_REGULAR);
    }
    //printf("Ordering time: %fs\n", microtime(true) - $s);

    $ofs = fopen($outputPath, "w");
    //$s = microtime(true);
    fwrite($ofs, json_encode($result, JSON_PRETTY_PRINT));
    //printf("Writing time: %fs\n", microtime(true) - $s);
    fclose($ofs);
  }
}
