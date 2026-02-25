<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        ini_set('memory_limit', '512M');
        $fileSize = filesize($inputPath);

        if ($fileSize < 10_000_000 || !function_exists('pcntl_fork')) {
            $data = $this->scan($inputPath, 0, $fileSize);
            $this->emit($data, $outputPath);
            return;
        }

        $workers = 4;

        // Line-aligned split points
        $fp = fopen($inputPath, 'rb');
        $splits = [0];
        for ($w = 1; $w < $workers; $w++) {
            fseek($fp, (int)($fileSize * $w / $workers));
            fgets($fp);
            $splits[] = ftell($fp);
        }
        fclose($fp);
        $splits[] = $fileSize;

        $pid = getmypid();
        $children = [];
        $files = [];

        for ($w = 0; $w < $workers - 1; $w++) {
            $f = sys_get_temp_dir() . "/c_{$pid}_{$w}";
            $files[$w] = $f;
            $cpid = pcntl_fork();
            if ($cpid === 0) {
                $d = $this->scan($inputPath, $splits[$w], $splits[$w + 1]);
                $enc = function_exists('igbinary_serialize') ? igbinary_serialize($d) : serialize($d);
                file_put_contents($f, $enc);
                exit(0);
            }
            $children[] = $cpid;
        }

        // Parent takes last chunk
        $parent = $this->scan($inputPath, $splits[$workers - 1], $splits[$workers]);

        foreach ($children as $cpid) {
            pcntl_waitpid($cpid, $st);
        }

        // Merge in chunk order (children first for first-appearance key ordering)
        $result = [];
        for ($w = 0; $w < $workers - 1; $w++) {
            $raw = file_get_contents($files[$w]);
            $chunk = function_exists('igbinary_unserialize') ? igbinary_unserialize($raw) : unserialize($raw);
            unlink($files[$w]);
            foreach ($chunk as $p => $dates) {
                if (isset($result[$p])) {
                    foreach ($dates as $d => $c) {
                        $result[$p][$d] = ($result[$p][$d] ?? 0) + $c;
                    }
                } else {
                    $result[$p] = $dates;
                }
            }
        }
        foreach ($parent as $p => $dates) {
            if (isset($result[$p])) {
                foreach ($dates as $d => $c) {
                    $result[$p][$d] = ($result[$p][$d] ?? 0) + $c;
                }
            } else {
                $result[$p] = $dates;
            }
        }

        $this->emit($result, $outputPath);
    }

    private function scan(string $inputPath, int $from, int $to): array
    {
        $data = [];
        $fp = fopen($inputPath, 'rb');
        stream_set_read_buffer($fp, 0);
        fseek($fp, $from);

        $remaining = $to - $from;
        $tail = '';

        while ($remaining > 0) {
            $raw = fread($fp, min(2_097_152, $remaining));
            if ($raw === false || $raw === '') break;
            $remaining -= strlen($raw);

            $pos = 0;
            $len = strlen($raw);

            // Splice tail from previous read onto the first line
            if ($tail !== '') {
                $nl = strpos($raw, "\n");
                if ($nl === false) {
                    $tail .= $raw;
                    continue;
                }
                $line = $tail . substr($raw, 0, $nl);
                $tail = '';
                $c = strpos($line, ',', 19);
                if ($c !== false) {
                    $p = substr($line, 19, $c - 19);
                    $d = substr($line, $c + 1, 10);
                    if (isset($data[$p][$d])) $data[$p][$d]++;
                    elseif (isset($data[$p])) $data[$p][$d] = 1;
                    else $data[$p] = [$d => 1];
                }
                $pos = $nl + 1;
            }

            // Process complete lines
            // Comma found via strpos; newline inferred from fixed 25-char timestamp
            while ($pos + 46 <= $len) {
                $c = strpos($raw, ',', $pos + 19);
                if ($c === false || $c + 27 > $len) {
                    $tail = substr($raw, $pos);
                    break;
                }

                $p = substr($raw, $pos + 19, $c - $pos - 19);
                $d = substr($raw, $c + 1, 10);

                if (isset($data[$p][$d])) $data[$p][$d]++;
                elseif (isset($data[$p])) $data[$p][$d] = 1;
                else $data[$p] = [$d => 1];

                $pos = $c + 27;
            }

            // Anything left after the loop is a partial line
            if ($pos < $len) {
                $tail = substr($raw, $pos);
            }
        }

        if ($tail !== '') {
            $c = strpos($tail, ',', 19);
            if ($c !== false) {
                $p = substr($tail, 19, $c - 19);
                $d = substr($tail, $c + 1, 10);
                if (isset($data[$p][$d])) $data[$p][$d]++;
                elseif (isset($data[$p])) $data[$p][$d] = 1;
                else $data[$p] = [$d => 1];
            }
        }

        fclose($fp);
        return $data;
    }

    private function emit(array $data, string $path): void
    {
        $fp = fopen($path, 'wb');
        stream_set_write_buffer($fp, 1_048_576);

        $buf = "{\n";
        $first = true;

        foreach ($data as $key => $dates) {
            ksort($dates);
            if (!$first) $buf .= ",\n";
            $first = false;

            $buf .= '    "' . str_replace('/', '\\/', $key) . "\": {\n";
            $fd = true;
            foreach ($dates as $dt => $cnt) {
                if (!$fd) $buf .= ",\n";
                $fd = false;
                $buf .= "        \"{$dt}\": {$cnt}";
            }
            $buf .= "\n    }";

            if (strlen($buf) > 1_048_576) {
                fwrite($fp, $buf);
                $buf = '';
            }
        }

        $buf .= "\n}";
        fwrite($fp, $buf);
        fclose($fp);
    }
}
