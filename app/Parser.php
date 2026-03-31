<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        \gc_disable();

        $data = $this->parseInput($inputPath);
        $this->formatOutput($outputPath, $data);
    }

    private function parseInput(string $inputPath): array
    {
        $data = [];
        $counters = \array_fill(0, 32 * 16 * 6, 0);

        $stream = \fopen($inputPath, 'rb');
        \stream_set_read_buffer($stream, 16 << 20);

        while (4 === \fscanf($stream, 'https://stitcher.io/blog/%[^,],%d-%d-%d', $path, $y, $m, $d)) {
            $data[$path] ??= $counters;
            ++$data[$path][($y - 2021) << 9 | $m << 5 | $d];
        }

        \fclose($stream);

        return $data;
    }

    private function formatOutput(string $outputPath, array $data): void
    {
        $stream = \fopen($outputPath, 'wb');
        \stream_set_write_buffer($stream, 16 << 20);

        $path_sep = "{";
        foreach ($data as $path => $counters) {
            \fwrite($stream, "{$path_sep}\n    \"\\/blog\\/{$path}\": {\n");
            $date_sep = '';
            foreach ($counters as $date => $count) {
                if ($count !== 0) {
                    \fprintf(
                        $stream,
                        "{$date_sep}        \"%d-%02d-%02d\": {$count}",
                        ($date >> 9) + 2021,
                        ($date >> 5) & 0b1111,
                        $date & 0b11111,
                    );
                    $date_sep = ",\n";
                }
            }
            $path_sep = "\n    },";
        }
        \fwrite($stream, "\n    }\n}");

        \fsync($stream) and \fclose($stream);
    }
}
