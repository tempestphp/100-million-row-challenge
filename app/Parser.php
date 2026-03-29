<?php

declare(strict_types=1);

namespace App;

use App\Commands\Visit;

use const JSON_PRETTY_PRINT;
use const JSON_THROW_ON_ERROR;

final class Parser
{
    public array $data = [];
    private int $buffer = 1024 * 1024;

    public function setBuffer(int $buffer): self
    {
        if ($buffer > 0) {
            $this->buffer = $buffer;
        }

        return $this;
    }

    public function parse(string $inputPath, string $outputPath): void
    {
        $handle = fopen($inputPath, 'r');
        stream_set_read_buffer($handle, 0);

        $data = [];

        //        $worker = 4;
        //        $pids = [];
        //
        //        for ($i = 0; $i < $worker; ++$i) {
        //            $pid = pcntl_fork();
        //            if (-1 === $pid) {
        //                exit(1);
        //            }
        //
        //            if ($pid) {
        //                $pids[] = $pid;
        //            } else {
        //                // child
        //                dump($i.' - start');
        //                sleep(random_int(1, 5));
        //                dump($i.' - end');
        //                exit(0);
        //            }
        //        }
        //
        //        foreach ($pids as $pid) {
        //            pcntl_waitpid($pid, $status);
        //        }
        //
        //        dd($pids);
        //
        //        exit('ende');

        //        foreach (Visit::all() as $visit) {
        //            $data[substr($visit->uri, 19)] = [];
        //        }
        //        dd($data);

        $toSeek = 0;
        while (!feof($handle)) {
            if ($toSeek) {
                fseek($handle, $toSeek);
            }

            $content = fread($handle, $this->buffer);
            $curSeek = ftell($handle);

            $posLastNewline = strrpos($content, "\n");
            $posDiffFromLastNewline = $curSeek - $toSeek - $posLastNewline;

            $content = substr($content, 0, -1 * $posDiffFromLastNewline);
            $toSeek = $curSeek - $posDiffFromLastNewline + 1;

            $lines = explode("\n", $content);
            foreach ($lines as $line) {
                $uri = substr($line, 19, strpos($line, ',') - 19);
                $date = substr($line, -25, 10);

                $data[$uri][] = $date;
            }
        }

        foreach ($data as &$item) {
            $item = array_count_values($item);
            ksort($item);
        }
        unset($item);

        file_put_contents($outputPath, json_encode($data, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT));
    }
}
