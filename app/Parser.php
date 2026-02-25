<?php

namespace App;

use Exception;
use SplFileObject;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $data = [];
        $firstSeen = [];
        $orderCounter = 1;

        $fp = new SplFileObject($inputPath, 'r');
        $fp->setFlags(SplFileObject::READ_CSV | SplFileObject::SKIP_EMPTY | SplFileObject::READ_AHEAD);
        $fp->setCsvControl(',', '"', '');

        while (!$fp->eof() && ($row = $fp->fgetcsv()) !== false) {
            if (count($row) < 2) continue;

            $url = $row[0];
            $ts  = $row[1];

            $path = substr($url, 19);
            $ymd = substr($ts, 0, 10);

            $data[$path] ??= [];
            $firstSeen[$path] = $orderCounter++;

            $data[$path][$ymd] = ($data[$path][$ymd] ?? 0) + 1;
        }

        // Write output
        $out = fopen($outputPath, 'w');
        fwrite($out, "{\n");

        $first = true;
        foreach ($data as $path => $dates) {
            if (!$first) fwrite($out, ",\n");

            ksort($dates);

            $jsonInner = json_encode($dates, JSON_PRETTY_PRINT);

            $lines = explode("\n", $jsonInner);
            $indented = $lines[0] . "\n"; // {
            for ($i = 1; $i < count($lines); $i++) {
                $indented .= "    " . $lines[$i] . "\n";
            }
            $indented = rtrim($indented, "\n");

            fwrite($out, '    ' . json_encode($path) . ': ' . $indented);

            $first = false;
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
