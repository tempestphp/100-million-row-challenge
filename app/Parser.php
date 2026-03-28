<?php

namespace App;

use SplFileObject;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $inputFile = new SplFileObject($inputPath, 'r');
        $inputFile->setCsvControl(',', '"', '');
        $inputFile->setFlags(
            SplFileObject::READ_CSV |
            SPLFileObject::SKIP_EMPTY |
            SplFileObject::DROP_NEW_LINE |
            SPLFileObject::READ_AHEAD
        );

        $results = [];

        while (! $inputFile->eof()) {
            $row = $inputFile->fgetcsv();

            if ($row === false || $row === [null]) {
                continue;
            }

            $path = $this->parsePath($row[0]);
            $date = substr($row[1], 0, 10);

            $results[$path][$date] = 1 + ($results[$path][$date] ?? 0);
        }

        foreach ($results as &$path) {
            ksort($path, SORT_STRING);
        }

        $json = json_encode($results, JSON_PRETTY_PRINT);

        file_put_contents($outputPath, $json);
    }

    private function parsePath(string $url): string
    {
        $schemeStartPos = strpos($url, '://');
        $schemeEndPos = ($schemeStartPos === false) ? 0 : $schemeStartPos + 3;
        $firstSlashPos = strpos($url, '/', $schemeEndPos);

        return substr($url, $firstSlashPos);
    }
}