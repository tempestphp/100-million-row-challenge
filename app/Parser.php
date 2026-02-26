<?php

namespace App;

use Exception;

final class Parser
{
    /**
     * @throws Exception
     */
    public function parse(string $inputPath, string $outputPath): void
    {
        /** @var resource $inputHandle */
        ($inputHandle = \fopen($inputPath, "r")) || throw new \Exception("Couldn't open input file");
//        $this->fGetsCsvMethod($inputHandle);
        $this->fReadMethod($inputHandle);
        \fclose($inputHandle);
    }

    private function fGetsCsvMethod($handle): void
    {
        while(!\feof($handle)) {
            $arr = \fgetcsv($handle, 1024, escape: "\\");
        }
    }

    private function fReadMethod($handle): void
    {
        while(!feof($handle)) {
            $str = \fread($handle, 1024 * 1024 * 8);
            if (!\str_ends_with($str, "\n")) {
                $str .= \fgets($handle, 1024);
            }
            $arr = \explode("\n", $str);
        }
    }
}
