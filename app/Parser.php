<?php

namespace App;

use Exception;

use function substr;

final class Parser
{
    private bool $isDebug;
    private array $map = [];
    private array $revMap = [];

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
            $lines = explode("\n", $data);
            $lines[0] = $prev . $lines[0];
            $prev = array_pop($lines);
            foreach ($lines as $line) {
                $url = substr($line, 25, -26);
                $date = substr($line, -23, -15);
                $key = $this->getKey($date);
                $result[$url][$key] = ($result[$url][$key] ?? 0) + 1;
            }
        }
        $this->timer('parse');

        foreach ($result as $uri => $value) {
            unset($result[$uri]);
            foreach ($value as $key => $count) {
                unset($value[$key]);
                $value[$this->getDate($key)] = $count;
            }
            ksort($value);
            $result["/blog/$uri"] = $value;
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

    private function getKey(string $date): int
    {
        if (isset($this->map[$date])) {
            return $this->map[$date];
        }

        $this->revMap[] = $date;
        return $this->map[$date] = count($this->revMap) - 1;
    }

    private function getDate(int $key): string
    {
        return "20{$this->revMap[$key]}";
    }
}
