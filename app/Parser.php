<?php

namespace App;

use App\Ilex\Visits;
use Exception;

final class Parser
{

    public function parse(string $inputPath, string $outputPath): void
    {
        $handle = \fopen($inputPath, 'rb');

        if ($handle === false) {
            throw new Exception("Cannot open file: $inputPath");
        }

        $ar = [];

        try {
            // Read file line by line
            while (($line = fgets($handle)) !== false) {

                [$key, $date] = \explode(',', $line);
                $date = \substr($date, 0, 10);

                $key = \strstr($key, '/blog');

                if (isset($ar[$key])) {
                    $ar[$key]->add($date);
                    continue;
                }

                $ar[$key] = Visits::init($date);

            }

        } finally {
            \fclose($handle);
        }


        file_put_contents($outputPath, \json_encode($ar, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT));
    }
}