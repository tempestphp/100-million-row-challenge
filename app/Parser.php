<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $data = $this->parseRows($inputPath);
        
        $chunks = array_chunk($data, floor(count($data) / 8), preserve_keys: true);
        
        $fp = fopen($outputPath, 'w');
        fwrite($fp, "{\n");
        fclose($fp);

        $childPids = [];
    
        foreach ($chunks as $chunk) {
            $pid = pcntl_fork();

            if ($pid === -1) {
                throw new \RuntimeException('Failed to fork process');
            }

            if ($pid === 0) {
                $this->processChunk($chunk, $outputPath);
                exit(0);
            }

            $childPids[] = $pid;
            usleep(250);
        }

        foreach ($childPids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $fp = fopen($outputPath, 'r+');
        fseek($fp, -2, SEEK_END);
        fwrite($fp, "\n}");
        fclose($fp);
    }

    private function processChunk(array $chunk, string $outputPath): void
    {
        $fragment = '';
        $padding0 = '    ';
        $padding1 = '        ';
        
        foreach ($chunk as $key => $rows) {
            $fragment .= "{$padding0}\"\/blog\/{$key}\": {\n";
            
            foreach ($rows as $date => $count) {
                $fragment .= "{$padding1}\"{$date}\": {$count},\n";
            }
            
            $fragment = substr($fragment, 0, -2);
            $fragment .= "\n{$padding0}},\n";
        }

        $fp = fopen($outputPath, 'a');
        flock($fp, LOCK_EX);
        fwrite($fp, $fragment);
        flock($fp, LOCK_UN);
        fclose($fp);
    }

    private function parseRows(string $inputPath): array
    {
        $data = [];
        $rows = file($inputPath);
        
        foreach ($rows as $row) {
            $tmp = explode(',', $row);
            $key = substr($tmp[0], 25);
            $date = substr($tmp[1], 0, 10);
            $data[$key][$date] = ($data[$key][$date] ?? 0) + 1;
        }
        
        foreach ($data as &$values) {
            ksort($values);
        }
        
        return $data;
    }
}
