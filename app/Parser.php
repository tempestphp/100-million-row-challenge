<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $result = [];
        $regex = '/(\/blog\/[^,]+,\d{4}-\d{2}-\d{2})/';

        $anotherOne = true;
        $offset = 0;
        $chunkSize = 100000; // 100_000

        while($anotherOne) {
            $fileChunk = file_get_contents(
                filename:$inputPath,
                use_include_path:false,
                context:null,
                offset:$offset,
                length:$chunkSize
            );

            // end of file
            if (!$fileChunk || $fileChunk === PHP_EOL) {
                $anotherOne = false;
                continue;
            }

            // calculate new offset
            $lastNewLinePos = strripos($fileChunk, PHP_EOL);
            $offset += $chunkSize - ($chunkSize-$lastNewLinePos);

            // get data
            preg_match_all($regex, $fileChunk, $matches, PREG_SET_ORDER);

            for ($i = 0; $i < count($matches); $i++) {
                [$page, $date] = explode(',', $matches[$i][0]);

                if( isset($result[$page][$date]) ) {
                    $result[$page][$date] += 1;
                    continue;
                }

                if ( isset($result[$page]) ) {
                    $result[$page][$date] = 1;
                    continue;
                }

                $result[$page][$date] = 1;
            }
        }

        foreach($result as $page => $visits) {
            if (count($result[$page]) > 1) {
                ksort($result[$page]);
            }
        }

        file_put_contents($outputPath, json_encode($result, JSON_PRETTY_PRINT));
    }
}