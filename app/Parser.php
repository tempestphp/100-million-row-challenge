<?php

namespace App;

use SplFixedArray;

final class Parser
{
    private const int SLUG_OFFSET = 25;
    private const int DATE_LENGTH = 10;
    private const int COMMA_TO_NEXT_SLUG = 52;
    private const int CHUNK_TARGET_SIZE = 512 * 1024;

    private array $result = [];

    public function parse(string $inputPath, string $outputPath): void
    {
        $boundaries = $this->getChunkBoundaries($inputPath, filesize($inputPath));
        $totalChunks = $boundaries->count() - 1;
        $currentChunk = 0;
        while ($currentChunk < $totalChunks) {
            $this->processChunk(
                $inputPath,
                $boundaries[$currentChunk],
                $boundaries[$currentChunk + 1]
            );
            $currentChunk++;
        }

        $this->writeOutput($outputPath);
    }

    private function getChunkBoundaries(string $inputPath, int $fileSize): SplFixedArray
    {
        $handle = fopen($inputPath, 'r');
        $boundaries = new SplFixedArray(ceil($fileSize / self::CHUNK_TARGET_SIZE) + 1);
        $boundaries[0] = 0;
        $boundaryIndex = 1;
        $nextPos = 0;
        do {
            $nextPos = min($nextPos + self::CHUNK_TARGET_SIZE, $fileSize);
            fseek($handle, $nextPos);
            $line = fgets($handle);
            if (false === $line) {
                $boundaries[$boundaryIndex] = $fileSize;
                break;
            }
            $nextPos += strlen($line);
            $boundaries[$boundaryIndex++] = $nextPos;
        } while ($fileSize > $nextPos);

        fclose($handle);

        return $boundaries;
    }

    private function processChunk(string $inputPath, int $start, int $end): void
    {
        $contentLength = $end - $start;
        $buffer = file_get_contents($inputPath, false, null, $start, $end - $start);
        $slugStart = self::SLUG_OFFSET;
        do {
            $commaPosition = strpos($buffer, ',', $slugStart);
            $slug = substr($buffer, $slugStart, $commaPosition - $slugStart);
            $date = substr($buffer, $commaPosition + 1, self::DATE_LENGTH);
            $this->result[$slug][$date] = ($this->result[$slug][$date] ?? 0) + 1;
            $slugStart = $commaPosition + self::COMMA_TO_NEXT_SLUG;
        } while ($slugStart <= $contentLength);
    }

    private function writeOutput(string $outputPath): void
    {
        $out = "{\n";
        $firstUrl = true;
        foreach ($this->result as $slug => $dates) {
            ksort($dates);
            if (!$firstUrl) {
                $out .= ",\n";
            }
            $firstUrl = false;
            $out .= "    \"\\/blog\\/{$slug}\": {\n";
            $firstDate = true;
            foreach ($dates as $date => $count) {
                if (!$firstDate) {
                    $out .= ",\n";
                }
                $firstDate = false;
                $out .= "        \"{$date}\": {$count}";
            }
            $out .= "\n    }";
        }

        $out .= "\n}";

        file_put_contents($outputPath, $out);
    }
}
