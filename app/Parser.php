<?php

namespace App;

use Exception;

final class Parser
{
    private bool $isDebug;

    public function __construct()
    {
        $this->isDebug = (bool)getenv('PAST_ONE_DEBUG');
    }

    /**
     * @throws Exception
     */
    public function parse(string $inputPath, string $outputPath): void
    {
        $this->timer('start');
        $result = [];
        $input = fopen($inputPath, "rb") ?: throw new Exception("Unable to open input file");
        $this->timer('open');
        $prev = '';
        while ($data = fread($input, 1024 * 16)) {
            $prevLines = explode("\n", $prev);
            $lines = explode("\n", $data);
            $prev = array_pop($lines);
            $lines[0] = array_pop($prevLines) . $lines[0];
            foreach ($prevLines as $line) {
                $url = substr($line, 19, -26);
                $date = substr($line, -25, -15);
                $result[$url][$date] = ($result[$url][$date] ?? 0) + 1;
            }
            foreach ($lines as $line) {
                $url = substr($line, 19, -26);
                $date = substr($line, -25, -15);
                $result[$url][$date] = ($result[$url][$date] ?? 0) + 1;
            }
        }
        $this->timer('parse');

        foreach ($result as $key => $value) {
            ksort($result[$key]);
        }
        $this->timer('sort');

        $json = json_encode($result, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT);
        $this->timer('json');

        file_put_contents($outputPath, $json);
        $this->timer('output');
    }

    private function timer(string $name): void
    {
        if (!$this->isDebug) {
            return;
        }
        static $last = microtime(true);

        $current = microtime(true);

        echo "$name:\t" . ($current - $last) . "\n";
        $last = $current;
    }
}
