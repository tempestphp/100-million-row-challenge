<?php

namespace App;

use Exception;
use function gc_disable;
use function fopen;
use function stream_set_read_buffer;
use function fgets;
use function substr;
use function strpos;
use function ksort;
use function str_replace;
use function file_put_contents;

final class Parser
{
		private const READ_FILE_SIZE = 16777216; # 16MB

		private const PREFIX_LENGTH = 19;
		private const SUFFIX_LENGTH = -16;
    public function parse(string $inputPath, string $outputPath): void
    {
				// gc? where we're going we dont need gc
        gc_disable();

        $handle = fopen($inputPath, 'r');
        if (! $handle) {
            throw new Exception("Could not open file at $inputPath");
        }

        stream_set_read_buffer($handle, self::READ_FILE_SIZE);

        $flatData = [];

				$prefix = self::PREFIX_LENGTH;
				$suffix = self::SUFFIX_LENGTH;

        while (($line = fgets($handle)) !== false) {
            $key = substr($line, $prefix, $suffix);
            $flatData[$key] = ($flatData[$key] ?? 0) + 1;
        }

        fclose($handle);

        ksort($flatData, SORT_STRING);

        $json = "{\n";
        $currentUrl = null;
        $firstUrl = true;
        $firstDate = true;

        foreach ($flatData as $key => $count) {
            $commaPos = strpos($key, ',');
            $url = substr($key, 0, $commaPos);
            $date = substr($key, $commaPos + 1);

            if ($url !== $currentUrl) {
                if ($currentUrl !== null) {
                    $json .= "\n    }";
                }
                
                if (!$firstUrl) {
                    $json .= ",\n";
                }
                
                $escapedUrl = str_replace('/', '\/', $url);
                $json .= '    "' . $escapedUrl . '": {';
                
                $currentUrl = $url;
                $firstUrl = false;
                $firstDate = true;
            }

            if (!$firstDate) {
                $json .= ',';
            }
            $json .= "\n" . '        "' . $date . '": ' . $count;
            $firstDate = false;
        }

        if ($currentUrl !== null) {
            $json .= "\n    }\n}";
        } else {
            $json .= "\n}";
        }

        file_put_contents($outputPath, $json);
    }
}
