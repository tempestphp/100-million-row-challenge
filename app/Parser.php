<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        ini_set('memory_limit', '-1');
        gc_disable();

        $fp = \fopen($inputPath, 'rb');
        if (!$fp) {
            return;
        }

        $result = [];
        $order = [];
        $orderCounter = 0;
        $chunkSize = 256 * 1024 * 1024; // 128MB

        while (!\feof($fp)) {
            $buffer = \fread($fp, $chunkSize);
            if ($buffer === false || $buffer === '') {
                break;
            }

            // Ensure we read until the next newline to not sever a record in half
            $extra = \fgets($fp);
            if ($extra !== false) {
                $buffer .= $extra;
            }

            if (\preg_match_all('/^https?:\/\/[^\/]+(\/[^,]+),(.{10})/m', $buffer, $matches)) {
                $paths = $matches[1];
                $dates = $matches[2];
                $count = \count($paths);

                for ($i = 0; $i < $count; ++$i) {
                    $path = $paths[$i];
                    $date = $dates[$i];

                    $order[$path] ??= ++$orderCounter;
                    $result[$path][$date] = ($result[$path][$date] ?? 0) + 1;
                }
            }

            unset($buffer, $matches, $paths, $dates);
        }

        \fclose($fp);

        // Sort dates for each fully accumulated path
        foreach ($result as &$dates) {
            \ksort($dates, SORT_STRING);
        }

        $this->writeOutput($result, $order, $outputPath);
    }

    private function writeOutput(array $result, array $order, string $outputPath): void
    {
        // Sort paths by first-appearance order
        \asort($order, SORT_NUMERIC);

        // Build ordered result (dates already sorted)
        $ordered = [];
        foreach (\array_keys($order) as $path) {
            if (isset($result[$path])) {
                $ordered[$path] = $result[$path];
            }
        }

        \file_put_contents($outputPath, \json_encode($ordered, JSON_PRETTY_PRINT));
    }
}
