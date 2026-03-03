<?php

namespace App;

use Exception;

use function Tempest\Support\Arr\sort_keys;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {

        try{
            $dataFile = fopen($inputPath, "rt");
            $buffer = [];
            $lineNumber = 0;
            while( ($line = fgets($dataFile)) && ++$lineNumber){
                
                /** 0 => all str, 1 => url path, 2 => date */
                $matches = [];
                preg_match("/https:\/\/stitcher\.io([^,]+),(\d\d\d\d-\d\d-\d\d)T/s",  $line, $matches)
                ?  ($buffer[$matches[1]][$matches[2]] = ($buffer[$matches[1]][$matches[2]] ?? 0) + 1)
                : throw new Exception("Parse failed at line $lineNumber"); 
            }

            foreach($buffer as $path  => &$visits){
                $visits = sort_keys($visits);
            }
            file_put_contents($outputPath, json_encode($buffer, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT, 2) );
        }
        catch(Exception $ex){
            throw $ex;
        }
        finally{
            fclose($dataFile);
        }
    }
}