<?php

namespace App;

final class Parser
{
    private const int HOST_PREFIX_LENGTH = 19;
    private const int MAX_LINE_LENGTH = 1_048_576; // 1mb
    private const int READ_BUFFER_SIZE = 1_048_576; // 1mb
    private const int WRITE_BUFFER_SIZE = 65_536; //64kb
    private const string LINE_DELIMITER = "\n";
    private const int DATE_LENGTH = 10;

    public function parse(string $inputPath, string $outputPath): void
    {
        $visitsByPath = [];

        foreach ($this->parsedRows($inputPath) as [$path, $date]) {
            $visitsByPath[$path][$date] =
                ($visitsByPath[$path][$date] ??= 0) + 1;
        }

        foreach ($visitsByPath as &$visitsByDate) {
            ksort($visitsByDate);
        }

        unset($visitsByDate);

        file_put_contents(
            $outputPath,
            json_encode($visitsByPath, JSON_PRETTY_PRINT),
        );
    }

    private function lines(string $inputPath): \Generator
    {
        $file = fopen($inputPath, "r");

        stream_set_read_buffer($file, self::READ_BUFFER_SIZE);

        try {
            while (
                $line = stream_get_line(
                    $file,
                    self::MAX_LINE_LENGTH,
                    self::LINE_DELIMITER,
                )
            ) {
                yield $line;
            }
        } finally {
            fclose($file);
        }
    }

    private function parsedRows(string $inputPath): \Generator
    {
        foreach ($this->lines($inputPath) as $line) {
            $commaPosition = strpos($line, ",");

            $pathLength = strcspn($line, ",", self::HOST_PREFIX_LENGTH);

            yield [
                substr($line, self::HOST_PREFIX_LENGTH, $pathLength),
                substr($line, $commaPosition + 1, self::DATE_LENGTH),
            ];
        }
    }
}
