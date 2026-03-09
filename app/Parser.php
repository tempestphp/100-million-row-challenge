<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $handle = fopen($inputPath, 'rb');
        $bufferSize = 1024 * 1024;

        $result = [];
        $remainder = '';
        
        $template = [];
        for ($y = 2020; $y <= date('Y'); $y++) {
            for ($m = 1; $m <= 12; $m++) {
                for ($d = 1; $d <= 31; $d++) {
                    $template[sprintf('%d-%02d-%02d', $y, $m, $d)] = 0;
                }
            }
        }
    
        while (!feof($handle)) {
            $chunk = $remainder . fread($handle, $bufferSize);
            $lines = explode("\n", $chunk);
            $remainder = array_pop($lines);

            if ($remainder === '' && count($lines) === 0) {
                break;
            }

            foreach ($lines as $line) {
                $u = substr($line, 19, strlen($line) - 45);
                $d = substr($line, strlen($line) - 25, 10);

                if (!isset($result[$u])) {
                    $result[$u] = $template;
                }

                $result[$u][$d]++;
            }
        }

        fclose($handle);
        
        foreach ($result as $s => $v) {
            $tmp = [];
            foreach ($v as $date => $num) {
                if ($num > 0) {
                    $tmp[$date] = $num;
                }
            }
            $result[$s] = $tmp;
        }

        file_put_contents($outputPath, json_encode($result, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE));
    }
}