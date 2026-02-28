<?php

namespace App;

final class Parser
{
    private const string EOL = "\n";
    private array $output = [];

    public function parse(string $inputPath, string $outputPath): void
    {
        $handle = fopen($inputPath, 'r');
        while ($line = fgets($handle)) {
            $matches = [];
            preg_match('/io(.+),(.+)T/', $line, $matches);
            $url = $matches[1];
            $timestamp = $matches[2];

            if (isset($this->output[$url][$timestamp])) {
                $this->output[$url][$timestamp]++;
            } else {
                $this->output[$url][$timestamp] = 1;
            }
        }
        fclose($handle);

        $this->writeOutput($outputPath);
    }

    private function writeOutput(string $outputPath): void
    {
        // Open file for writing
        $handle = fopen($outputPath, 'w');

        // Begin url line block
        fwrite($handle, '{' . self::EOL);

        $j = 0;
        $urlCount = count($this->output);
        foreach ($this->output as $url => $timestamps) {
            $this->writeUrl($handle, $url);
            $this->writeTimestamps($handle, $timestamps);

            // Close timestamps line block
            if (++$j === $urlCount) {
                fwrite($handle, '    }' . self::EOL);
            } else {
                fwrite($handle, '    },' . self::EOL);
            }
        }

        // Close url line block
        fwrite($handle, '}');
        fclose($handle);
    }

    private function writeUrl($handle, string $url): void
    {
        // Format url and write
        fwrite(
            $handle,
            '    "' . str_replace('/', '\/', $url) . '": {' . self::EOL
        );
    }

    private function writeTimestamps($handle, array $timestamps): void
    {
        // Sort timestamps
        ksort($timestamps);
        $i = 0;
        $timestampCount = count($timestamps);
        foreach ($timestamps as $timestamp => $count) {
            // Format timestamp lines and write
            if (++$i === $timestampCount) {
                $timestampLine = sprintf('        "%s": %d', $timestamp, $count);
            } else {
                $timestampLine = sprintf('        "%s": %d,', $timestamp, $count);
            }
            fwrite($handle, $timestampLine . self::EOL);
        }
    }
}