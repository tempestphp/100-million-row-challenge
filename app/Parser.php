<?php

namespace App;

use App\Commands\Visit;
use RuntimeException;

final class Parser
{
    private const int URL_PREFIX_LEN = 19;
    private const int READ_CHUNK_BYTES = 8388608;
    private const int WRITE_BUFFER_FLUSH_BYTES = 1048576;
    private const int INITIAL_DAY_CAPACITY = 2048;
    private const int DAY_GROWTH = 1024;

    public function parse(string $inputPath, string $outputPath): void
    {
        $gcWasEnabled = gc_enabled();
        if ($gcWasEnabled) {
            gc_disable();
        }

        $input = null;
        $output = null;

        try {
            $pathToId = [];
            $escapedPathById = [];

            $visits = Visit::all();
            $pathCount = count($visits);

            for ($pathId = 0; $pathId < $pathCount; $pathId++) {
                $path = substr($visits[$pathId]->uri, self::URL_PREFIX_LEN);
                $pathToId[$path] = $pathId;
                $escapedPathById[$pathId] = str_replace(
                    ['\\', '"', '/'],
                    ['\\\\', '\\"', '\\/'],
                    $path,
                );
            }

            $counts = [];
            for ($pathId = 0; $pathId < $pathCount; $pathId++) {
                $counts[$pathId] = array_fill(0, self::INITIAL_DAY_CAPACITY, 0);
            }

            $pathSeen = array_fill(0, $pathCount, false);
            $firstSeenPathOrder = [];

            $dayToId = [];
            $dayById = [];
            $dayCount = 0;
            $dayCapacity = self::INITIAL_DAY_CAPACITY;

            $input = fopen($inputPath, 'rb');
            if ($input === false) {
                throw new RuntimeException("Unable to open input file: {$inputPath}");
            }

            stream_set_read_buffer($input, self::READ_CHUNK_BYTES);

            $carry = '';

            while (true) {
                $chunk = fread($input, self::READ_CHUNK_BYTES);

                if ($chunk === false) {
                    throw new RuntimeException("Failed reading input file: {$inputPath}");
                }

                if ($chunk === '') {
                    break;
                }

                $buffer = $carry . $chunk;
                $bufferLength = strlen($buffer);
                $lineStart = 0;

                while (true) {
                    $newlinePos = strpos($buffer, "\n", $lineStart);

                    if ($newlinePos === false) {
                        break;
                    }

                    $lineEnd = $newlinePos;
                    if ($lineEnd > $lineStart && $buffer[$lineEnd - 1] === "\r") {
                        $lineEnd--;
                    }

                    if ($lineEnd > $lineStart) {
                        $commaPos = strpos($buffer, ',', $lineStart);

                        $pathOffset = $lineStart + self::URL_PREFIX_LEN;
                        $path = substr($buffer, $pathOffset, $commaPos - $pathOffset);

                        $pathId = $pathToId[$path];

                        $dayOffset = $commaPos + 1;
                        $day = substr($buffer, $dayOffset, 10);

                        if (isset($dayToId[$day])) {
                            $dayId = $dayToId[$day];
                        } else {
                            if ($dayCount === $dayCapacity) {
                                for ($growPathId = 0; $growPathId < $pathCount; $growPathId++) {
                                    for ($growI = 0; $growI < self::DAY_GROWTH; $growI++) {
                                        $counts[$growPathId][] = 0;
                                    }
                                }

                                $dayCapacity += self::DAY_GROWTH;
                            }

                            $dayId = $dayCount;
                            $dayToId[$day] = $dayId;
                            $dayById[$dayId] = $day;
                            $dayCount++;
                        }

                        $counts[$pathId][$dayId]++;

                        if (! $pathSeen[$pathId]) {
                            $pathSeen[$pathId] = true;
                            $firstSeenPathOrder[] = $pathId;
                        }
                    }

                    $lineStart = $newlinePos + 1;
                }

                if ($lineStart < $bufferLength) {
                    $carry = substr($buffer, $lineStart);
                } else {
                    $carry = '';
                }
            }

            if ($carry !== '') {
                if (substr($carry, -1) === "\r") {
                    $carry = substr($carry, 0, -1);
                }

                if ($carry !== '') {
                    $commaPos = strpos($carry, ',');
                    $path = substr($carry, self::URL_PREFIX_LEN, $commaPos - self::URL_PREFIX_LEN);
                    $pathId = $pathToId[$path];

                    $dayOffset = $commaPos + 1;
                    $day = substr($carry, $dayOffset, 10);

                    if (isset($dayToId[$day])) {
                        $dayId = $dayToId[$day];
                    } else {
                        if ($dayCount === $dayCapacity) {
                            for ($growPathId = 0; $growPathId < $pathCount; $growPathId++) {
                                for ($growI = 0; $growI < self::DAY_GROWTH; $growI++) {
                                    $counts[$growPathId][] = 0;
                                }
                            }

                            $dayCapacity += self::DAY_GROWTH;
                        }

                        $dayId = $dayCount;
                        $dayToId[$day] = $dayId;
                        $dayById[$dayId] = $day;
                        $dayCount++;
                    }

                    $counts[$pathId][$dayId]++;

                    if (! $pathSeen[$pathId]) {
                        $pathSeen[$pathId] = true;
                        $firstSeenPathOrder[] = $pathId;
                    }
                }
            }

            ksort($dayToId, SORT_STRING);
            $sortedDayIds = array_values($dayToId);

            $output = fopen($outputPath, 'wb');
            if ($output === false) {
                throw new RuntimeException("Unable to open output file: {$outputPath}");
            }

            stream_set_write_buffer($output, self::WRITE_BUFFER_FLUSH_BYTES);

            $pathInOutputCount = count($firstSeenPathOrder);
            if ($pathInOutputCount === 0) {
                if (fwrite($output, '{}') === false) {
                    throw new RuntimeException("Failed writing output file: {$outputPath}");
                }

                return;
            }

            $flushThreshold = self::WRITE_BUFFER_FLUSH_BYTES;
            $outBuffer = "{\n";

            for ($pathIndex = 0; $pathIndex < $pathInOutputCount; $pathIndex++) {
                $pathId = $firstSeenPathOrder[$pathIndex];
                $outBuffer .= '    "' . $escapedPathById[$pathId] . "\": {\n";

                if (strlen($outBuffer) >= $flushThreshold) {
                    if (fwrite($output, $outBuffer) === false) {
                        throw new RuntimeException("Failed writing output file: {$outputPath}");
                    }

                    $outBuffer = '';
                }

                $row = $counts[$pathId];
                $firstDayForPath = true;

                foreach ($sortedDayIds as $dayId) {
                    $count = $row[$dayId];

                    if ($count === 0) {
                        continue;
                    }

                    if ($firstDayForPath) {
                        $outBuffer .= '        "' . $dayById[$dayId] . '": ' . $count;
                        $firstDayForPath = false;
                    } else {
                        $outBuffer .= ",\n        \"" . $dayById[$dayId] . '": ' . $count;
                    }

                    if (strlen($outBuffer) >= $flushThreshold) {
                        if (fwrite($output, $outBuffer) === false) {
                            throw new RuntimeException("Failed writing output file: {$outputPath}");
                        }

                        $outBuffer = '';
                    }
                }

                $outBuffer .= "\n    }";

                if ($pathIndex !== $pathInOutputCount - 1) {
                    $outBuffer .= ",\n";
                } else {
                    $outBuffer .= "\n";
                }

                if (strlen($outBuffer) >= $flushThreshold) {
                    if (fwrite($output, $outBuffer) === false) {
                        throw new RuntimeException("Failed writing output file: {$outputPath}");
                    }

                    $outBuffer = '';
                }
            }

            $outBuffer .= '}';

            if ($outBuffer !== '') {
                if (fwrite($output, $outBuffer) === false) {
                    throw new RuntimeException("Failed writing output file: {$outputPath}");
                }
            }
        } finally {
            if (is_resource($input)) {
                fclose($input);
            }

            if (is_resource($output)) {
                fclose($output);
            }

            if ($gcWasEnabled) {
                gc_enable();
            }
        }
    }
}
