<?php

namespace App;

use App\Commands\Visit;
use RuntimeException;

final class Parser
{
    private const DOMAIN_LENGTH = 19;
    private const FIRST_SLUG_OFFSET = 25;
    private const DATE_OFFSET_FROM_EOL = 25;
    private const COMMA_OFFSET_FROM_EOL = 26;
    private const MIN_LINE_END_OFFSET = 55;
    private const DAY_RANGE = 4096;
    private const DATE_POOL_SIZE = 10_000;
    private const READ_SIZE = 1_048_576;
    private const DEFAULT_WORKERS = 10;
    private const FORK_THRESHOLD = 67_108_864;

    private static ?array $uris = null;
    private static ?array $paths = null;
    private static ?array $encodedPaths = null;
    private static ?array $uriJsonOpeners = null;
    private static ?array $uriIndexByUri = null;
    private static ?array $resolverTrees = null;
    private static ?array $byteChars = null;
    private static ?array $dateCache = null;
    private static ?array $dayLabels = null;
    private static ?array $dayJsonPrefixes = null;
    private static ?array $outputDaySlots = null;
    private static ?array $outputDayPrefixes = null;
    private static ?array $countBaseOffsetsByte = null;
    private static ?array $countBaseOffsetsWord = null;
    private static int $activeDayCount = self::DAY_RANGE;
    private static int $dayCodeBase = 0;
    private static bool $useDayMajorLayout = false;
    private static ?bool $useIntegerDateCache = null;
    private static ?bool $checkByteOverflow = null;
    private static ?\Closure $cachedChunkProcessor = null;
    private static ?\Closure $uncachedChunkProcessor = null;
    private static ?\Closure $cachedByteChunkProcessor = null;
    private static ?\Closure $uncachedByteChunkProcessor = null;
    private static ?\Closure $cachedCountOnlyChunkProcessor = null;
    private static ?\Closure $uncachedCountOnlyChunkProcessor = null;
    private static ?\Closure $cachedByteCountOnlyChunkProcessor = null;
    private static ?\Closure $uncachedByteCountOnlyChunkProcessor = null;

    public function parse(string $inputPath, string $outputPath): void
    {
        if (function_exists('gc_disable')) {
            gc_disable();
        }

        $fileSize = filesize($inputPath);

        if ($fileSize === false) {
            throw new RuntimeException("Unable to read {$inputPath}");
        }

        self::bootstrap();
        $orderedUris = $this->prescanInput($inputPath);
        $workerCount = $this->resolveWorkerCount($fileSize);
        $useByteCounts = $this->shouldUseByteCounts($workerCount);

        if ($workerCount === 1 || ! function_exists('pcntl_fork')) {
            $counts = $this->parseRangeCountsOnly($inputPath, 0, $fileSize, $useByteCounts);
        } else {
            $this->prewarmCountOnlyChunkProcessor($useByteCounts);
            $counts = $this->parseInParallelCountsOnly(
                $inputPath,
                $outputPath,
                $fileSize,
                $workerCount,
                $useByteCounts,
            );
        }

        file_put_contents(
            $outputPath,
            $this->buildOutput($counts, $orderedUris, $useByteCounts),
        );
    }

    private static function bootstrap(): void
    {
        if (self::$uris !== null) {
            return;
        }

        self::$uris = [];
        self::$paths = [];
        self::$encodedPaths = [];
        self::$uriJsonOpeners = [];
        self::$uriIndexByUri = [];
        self::$resolverTrees = [];
        self::$byteChars = [];
        self::$dateCache = [];
        self::$dayLabels = [];
        self::$dayJsonPrefixes = [];
        self::$outputDaySlots = [];
        self::$outputDayPrefixes = [];
        self::$countBaseOffsetsByte = [];
        self::$countBaseOffsetsWord = [];
        $groups = [];

        foreach (Visit::all() as $index => $visit) {
            $uri = $visit->uri;
            self::$uris[$index] = $uri;
            self::$paths[$index] = substr($uri, self::DOMAIN_LENGTH);
            self::$encodedPaths[$index] = json_encode(self::$paths[$index], JSON_THROW_ON_ERROR);
            self::$uriJsonOpeners[$index] = '    ' . self::$encodedPaths[$index] . ": {\n";
            self::$uriIndexByUri[$uri] = $index;
            $groups[strlen($uri)][$index] = $uri;
        }

        ksort($groups);
        self::$resolverTrees = self::buildResolverTrees($groups);

        for ($byte = 0; $byte < 256; $byte++) {
            self::$byteChars[$byte] = chr($byte);
        }

        self::configureFullDayDomain();
        self::$useIntegerDateCache = self::shouldUseIntegerDateCache();
        self::$checkByteOverflow = self::shouldCheckByteOverflow();
    }

    private static function buildChunkProcessor(
        bool $trackFirstSeen,
        bool $useDateCache,
        bool $useByteCounts,
        bool $useIntegerDateCache,
    ): \Closure
    {
        $code = '$byteChars = self::$byteChars;' . "\n";
        $code .= 'return static function(' . "\n";
        $code .= '    string $buffer,' . "\n";
        $code .= '    int $limit,' . "\n";
        $code .= '    string &$counts,' . "\n";
        if ($trackFirstSeen) {
            $code .= '    array &$firstSeen,' . "\n";
        }
        $code .= '    array &$parsedDateCache,' . "\n";
        if ($trackFirstSeen) {
            $code .= '    int &$remainingFirstSeen,' . "\n";
            $code .= '    int &$sequence,' . "\n";
        }
        $code .= '    int $uriCount,' . "\n";
        $code .= ') use ($byteChars): void {' . "\n";
        $code .= '    $lineStart = 0;' . "\n";
        $code .= '    $lineLimit = $limit - 1;' . "\n";
        $dayCodeExpression = "            ((ord(\$buffer[\$dateStart + 3]) - 48) << 9)\n";
        $dayCodeExpression .= "            | ((((ord(\$buffer[\$dateStart + 5]) - 48) * 10) + ord(\$buffer[\$dateStart + 6]) - 48) << 5)\n";
        $dayCodeExpression .= "            | (((ord(\$buffer[\$dateStart + 8]) - 48) * 10) + ord(\$buffer[\$dateStart + 9]) - 48)";
        $useDayMajorLayout = self::$useDayMajorLayout;
        $useBandedDayRange = ! $useDayMajorLayout && self::$dayCodeBase !== 0;
        $recordProcessor = static function (string $labelName) use ($trackFirstSeen, $useDateCache, $useByteCounts, $useIntegerDateCache, $dayCodeExpression, $useDayMajorLayout, $useBandedDayRange): string {
            $body = '        $uriLength = $newline - $lineStart - 26;' . "\n";
            $body .= '        switch ($uriLength) {' . "\n";

            foreach (self::$resolverTrees as $uriLength => $tree) {
                $body .= "case {$uriLength}:\n";
                $body .= self::emitChunkProcessorTree(
                    $tree,
                    4,
                    $labelName,
                    $trackFirstSeen || $useDayMajorLayout,
                );
            }

            $body .= "default:\n";
            $body .= "throw new \\RuntimeException('Unknown URI length');\n";
            $body .= "        }\n";
            $body .= "        {$labelName}:\n";

            if ($trackFirstSeen) {
                $body .= "        if (\$remainingFirstSeen !== 0 && \$firstSeen[\$uriIndex] === -1) {\n";
                $body .= "            \$firstSeen[\$uriIndex] = \$sequence;\n";
                $body .= "            \$remainingFirstSeen--;\n";
                $body .= "        }\n";
            }

            $body .= "        \$dateStart = \$newline - 25;\n";

            if ($useDateCache) {
                if ($useIntegerDateCache) {
                    $body .= "        \$dayCode =\n";
                    $body .= $dayCodeExpression . ";\n";

                    if ($useDayMajorLayout) {
                        if ($useByteCounts) {
                            $body .= "        \$countOffset = (\$parsedDateCache[\$dayCode] ??= self::\$countBaseOffsetsByte[\$dayCode]) + \$uriIndex;\n";
                        } else {
                            $body .= "        \$countOffset = (\$parsedDateCache[\$dayCode] ??= self::\$countBaseOffsetsWord[\$dayCode]) + (\$uriIndex << 1);\n";
                        }
                    } elseif ($useBandedDayRange) {
                        if ($useByteCounts) {
                            $body .= "        \$countOffset = \$uriSlotBase + (\$parsedDateCache[\$dayCode] ??= (\$dayCode - self::\$dayCodeBase));\n";
                        } else {
                            $body .= "        \$countOffset = ((\$uriSlotBase + (\$parsedDateCache[\$dayCode] ??= (\$dayCode - self::\$dayCodeBase))) << 1);\n";
                        }
                    } else {
                        if ($useByteCounts) {
                            $body .= "        \$countOffset = \$uriSlotBase + \$dayCode;\n";
                        } else {
                            $body .= "        \$countOffset = ((\$uriSlotBase + \$dayCode) << 1);\n";
                        }
                    }
                } else {
                    $body .= "        \$dateKey = substr(\$buffer, \$dateStart, 10);\n";
                    $body .= "        \$dayCode = \$parsedDateCache[\$dateKey] ??= (\n";
                    $body .= $dayCodeExpression . "\n";
                    $body .= "        );\n";
                    if ($useDayMajorLayout) {
                        if ($useByteCounts) {
                            $body .= "        \$countOffset = self::\$countBaseOffsetsByte[\$dayCode] + \$uriIndex;\n";
                        } else {
                            $body .= "        \$countOffset = self::\$countBaseOffsetsWord[\$dayCode] + (\$uriIndex << 1);\n";
                        }
                    } elseif ($useBandedDayRange) {
                        if ($useByteCounts) {
                            $body .= "        \$countOffset = \$uriSlotBase + (\$dayCode - self::\$dayCodeBase);\n";
                        } else {
                            $body .= "        \$countOffset = ((\$uriSlotBase + (\$dayCode - self::\$dayCodeBase)) << 1);\n";
                        }
                    } else {
                        if ($useByteCounts) {
                            $body .= "        \$countOffset = \$uriSlotBase + \$dayCode;\n";
                        } else {
                            $body .= "        \$countOffset = ((\$uriSlotBase + \$dayCode) << 1);\n";
                        }
                    }
                }
            } else {
                $body .= "        \$dayCode =\n";
                $body .= $dayCodeExpression . ";\n";
                if ($useDayMajorLayout) {
                    if ($useByteCounts) {
                        $body .= "        \$countOffset = self::\$countBaseOffsetsByte[\$dayCode] + \$uriIndex;\n";
                    } else {
                        $body .= "        \$countOffset = self::\$countBaseOffsetsWord[\$dayCode] + (\$uriIndex << 1);\n";
                    }
                } elseif ($useBandedDayRange) {
                    if ($useByteCounts) {
                        $body .= "        \$countOffset = \$uriSlotBase + (\$dayCode - self::\$dayCodeBase);\n";
                    } else {
                        $body .= "        \$countOffset = ((\$uriSlotBase + (\$dayCode - self::\$dayCodeBase)) << 1);\n";
                    }
                } else {
                    if ($useByteCounts) {
                        $body .= "        \$countOffset = \$uriSlotBase + \$dayCode;\n";
                    } else {
                        $body .= "        \$countOffset = ((\$uriSlotBase + \$dayCode) << 1);\n";
                    }
                }
            }

            if ($useByteCounts) {
                if (self::$checkByteOverflow) {
                    $body .= "        \$byteValue = ord(\$counts[\$countOffset]) + 1;\n";
                    $body .= "        if (\$byteValue === 256) {\n";
                    $body .= "            throw new \\RuntimeException('Byte counter overflow');\n";
                    $body .= "        }\n";
                    $body .= "        \$counts[\$countOffset] = \$byteChars[\$byteValue];\n";
                } else {
                    $body .= "        \$counts[\$countOffset] = \$byteChars[ord(\$counts[\$countOffset]) + 1];\n";
                }
            } else {
                $body .= "        \$lowByte = ord(\$counts[\$countOffset]) + 1;\n";
                $body .= "        if (\$lowByte === 256) {\n";
                $body .= "            \$counts[\$countOffset] = \"\\0\";\n";
                $body .= "            \$countOffset++;\n";
                $body .= "            \$counts[\$countOffset] = \$byteChars[ord(\$counts[\$countOffset]) + 1];\n";
                $body .= "        } else {\n";
                $body .= "            \$counts[\$countOffset] = \$byteChars[\$lowByte];\n";
                $body .= "        }\n";
            }

            if ($trackFirstSeen) {
                $body .= "        \$sequence++;\n";
            }

            $body .= "        \$lineStart = \$newline + 1;\n";

            return $body;
        };

        if ($trackFirstSeen) {
            $code .= '    while ($lineStart < $lineLimit && ($newline = strpos($buffer, "\n", $lineStart + ' . self::MIN_LINE_END_OFFSET . ')) !== false) {' . "\n";
            $code .= $recordProcessor('resolved_uri');
            $code .= "    }\n";
        } else {
            $code .= '    while ($lineStart < $lineLimit) {' . "\n";
            $code .= '        $newline = strpos($buffer, "\n", $lineStart + ' . self::MIN_LINE_END_OFFSET . ');' . "\n";
            $code .= '        if ($newline === false) {' . "\n";
            $code .= "            break;\n";
            $code .= "        }\n";
            $code .= $recordProcessor('resolved_uri_1');
            $code .= '        if ($lineStart >= $lineLimit) {' . "\n";
            $code .= "            break;\n";
            $code .= "        }\n";
            $code .= '        $newline = strpos($buffer, "\n", $lineStart + ' . self::MIN_LINE_END_OFFSET . ');' . "\n";
            $code .= '        if ($newline === false) {' . "\n";
            $code .= "            break;\n";
            $code .= "        }\n";
            $code .= $recordProcessor('resolved_uri_2');
            $code .= "    }\n";
        }
        $code .= "};";

        /** @var \Closure $processor */
        $processor = eval($code);

        return $processor;
    }

    private static function buildResolverTrees(array $groups): array
    {
        $trees = [];

        foreach ($groups as $uriLength => $candidates) {
            $trees[$uriLength] = self::buildResolverTree($candidates);
        }

        return $trees;
    }

    private static function buildResolverTree(array $candidates): array
    {
        if (count($candidates) === 1) {
            return [
                'uriIndex' => array_key_first($candidates),
            ];
        }

        $uriLength = strlen(reset($candidates));
        $bestPosition = null;
        $bestPartitions = null;
        $bestScore = -1;

        for ($position = self::FIRST_SLUG_OFFSET; $position < $uriLength; $position++) {
            $partitions = [];

            foreach ($candidates as $index => $uri) {
                $partitions[$uri[$position]][$index] = $uri;
            }

            $score = count($partitions);

            if ($score > $bestScore) {
                $bestScore = $score;
                $bestPosition = $position;
                $bestPartitions = $partitions;

                if ($score === count($candidates)) {
                    break;
                }
            }
        }

        if ($bestPartitions === null || $bestPosition === null) {
            throw new RuntimeException('Unable to build URI resolver');
        }

        ksort($bestPartitions);
        $branches = [];

        foreach ($bestPartitions as $char => $subset) {
            $branches[$char] = self::buildResolverTree($subset);
        }

        return [
            'branches' => $branches,
            'position' => $bestPosition,
        ];
    }

    private static function emitChunkProcessorTree(
        array $tree,
        int $indentLevel,
        string $labelName = 'resolved_uri',
        bool $includeUriIndex = true,
    ): string
    {
        if (isset($tree['uriIndex'])) {
            $indent = str_repeat('    ', $indentLevel);
            $uriIndex = $tree['uriIndex'];

            $code = '';

            if ($includeUriIndex) {
                $code .= $indent . '$uriIndex = ' . $uriIndex . ";\n";
            }

            return $code
                . $indent . '$uriSlotBase = ' . ($uriIndex * self::$activeDayCount) . ";\n"
                . $indent . "goto {$labelName};\n";
        }

        $indent = str_repeat('    ', $indentLevel);
        $code = $indent . 'switch ($buffer[$lineStart + ' . $tree['position'] . "]) {\n";

        foreach ($tree['branches'] as $char => $branch) {
            $code .= $indent . 'case ' . var_export($char, true) . ":\n";
            $code .= self::emitChunkProcessorTree($branch, $indentLevel + 1, $labelName, $includeUriIndex);
        }

        $code .= $indent . "default:\n";
        $code .= $indent . "    throw new \\RuntimeException('Unknown URI');\n";
        $code .= $indent . "}\n";

        return $code;
    }

    private function resolveWorkerCount(int $fileSize): int
    {
        if ($fileSize < self::FORK_THRESHOLD) {
            return 1;
        }

        $override = getenv('PARSER_WORKERS');

        if ($override !== false && ctype_digit($override)) {
            return max(1, (int) $override);
        }

        return self::DEFAULT_WORKERS;
    }

    private function orderedUrisFromFirstSeen(array $firstSeen): array
    {
        $orderedUris = [];

        foreach ($firstSeen as $uriIndex => $seen) {
            if ($seen !== -1) {
                $orderedUris[$uriIndex] = $seen;
            }
        }

        asort($orderedUris, SORT_NUMERIC);

        return array_keys($orderedUris);
    }

    private function prescanInput(string $inputPath): array
    {
        $handle = fopen($inputPath, 'rb');

        if ($handle === false) {
            throw new RuntimeException("Unable to open {$inputPath}");
        }

        $orderedUris = [];
        $seenUris = [];
        $remainingUris = count(self::$uris);
        $shouldCompactDays = $this->shouldUseCompactDayDomain();
        $shouldUseBandedDays = $this->shouldUseBandedDayDomain();
        $seenTimestamps = [];
        $uniqueTimestampCount = 0;
        $daySet = [];
        $minDayCode = self::DAY_RANGE;
        $maxDayCode = 0;

        while (($line = fgets($handle)) !== false) {
            $comma = strpos($line, ',', self::FIRST_SLUG_OFFSET);

            if ($comma === false) {
                continue;
            }

            if ($remainingUris !== 0) {
                $uriIndex = self::$uriIndexByUri[substr($line, 0, $comma)] ?? null;

                if ($uriIndex !== null && ! isset($seenUris[$uriIndex])) {
                    $seenUris[$uriIndex] = true;
                    $orderedUris[] = $uriIndex;
                    $remainingUris--;
                }
            }

            if ($uniqueTimestampCount < self::DATE_POOL_SIZE) {
                $timestamp = substr($line, $comma + 1, 25);

                if (! isset($seenTimestamps[$timestamp])) {
                    $seenTimestamps[$timestamp] = true;
                    $dayCode = self::dayCodeFromDateString(substr($timestamp, 0, 10));
                    $daySet[$dayCode] = true;
                    if ($dayCode < $minDayCode) {
                        $minDayCode = $dayCode;
                    }
                    if ($dayCode > $maxDayCode) {
                        $maxDayCode = $dayCode;
                    }
                    $uniqueTimestampCount++;
                }
            }

            if ($remainingUris === 0 && $uniqueTimestampCount === self::DATE_POOL_SIZE) {
                break;
            }
        }

        fclose($handle);

        if ($daySet !== []) {
            if ($shouldUseBandedDays) {
                self::configureBandedDayDomain($minDayCode, $maxDayCode);
            } elseif ($shouldCompactDays) {
                ksort($daySet, SORT_NUMERIC);
                self::configureDayDomain(array_keys($daySet));
            } else {
                self::configureFullDayDomain();
                ksort($daySet, SORT_NUMERIC);
                self::configureOutputDayDomain(array_keys($daySet));
            }
        } else {
            self::configureFullDayDomain();
        }

        return $orderedUris;
    }

    private static function configureFullDayDomain(): void
    {
        $uriCount = count(self::$uris);
        self::resetChunkProcessorCache();
        self::$activeDayCount = self::DAY_RANGE;
        self::$dayCodeBase = 0;
        self::$useDayMajorLayout = false;
        self::$dayLabels = [];
        self::$dayJsonPrefixes = [];
        self::$outputDaySlots = [];
        self::$outputDayPrefixes = [];
        self::$countBaseOffsetsByte = array_fill(0, self::DAY_RANGE, 0);
        self::$countBaseOffsetsWord = array_fill(0, self::DAY_RANGE, 0);

        for ($dayCode = 0; $dayCode < self::DAY_RANGE; $dayCode++) {
            self::$dayLabels[$dayCode] = self::formatDayCode($dayCode);
            self::$dayJsonPrefixes[$dayCode] = '        "' . self::$dayLabels[$dayCode] . '": ';
            self::$outputDaySlots[$dayCode] = $dayCode;
            self::$outputDayPrefixes[$dayCode] = self::$dayJsonPrefixes[$dayCode];
            self::$countBaseOffsetsByte[$dayCode] = $dayCode * $uriCount;
            self::$countBaseOffsetsWord[$dayCode] = ($dayCode * $uriCount) << 1;
        }
    }

    private static function configureDayDomain(array $dayCodes): void
    {
        $uriCount = count(self::$uris);
        self::resetChunkProcessorCache();
        self::$activeDayCount = count($dayCodes);
        self::$dayCodeBase = 0;
        self::$useDayMajorLayout = true;
        self::$dayLabels = array_fill(0, self::$activeDayCount, '');
        self::$dayJsonPrefixes = array_fill(0, self::$activeDayCount, '');
        self::$outputDaySlots = range(0, self::$activeDayCount - 1);
        self::$outputDayPrefixes = array_fill(0, self::$activeDayCount, '');
        self::$countBaseOffsetsByte = array_fill(0, self::DAY_RANGE, -1);
        self::$countBaseOffsetsWord = array_fill(0, self::DAY_RANGE, -1);

        foreach ($dayCodes as $dayIndex => $dayCode) {
            self::$dayLabels[$dayIndex] = self::formatDayCode($dayCode);
            self::$dayJsonPrefixes[$dayIndex] = '        "' . self::$dayLabels[$dayIndex] . '": ';
            self::$outputDayPrefixes[$dayIndex] = self::$dayJsonPrefixes[$dayIndex];
            self::$countBaseOffsetsByte[$dayCode] = $dayIndex * $uriCount;
            self::$countBaseOffsetsWord[$dayCode] = ($dayIndex * $uriCount) << 1;
        }
    }

    private static function configureBandedDayDomain(int $minDayCode, int $maxDayCode): void
    {
        self::resetChunkProcessorCache();
        self::$activeDayCount = ($maxDayCode - $minDayCode) + 1;
        self::$dayCodeBase = $minDayCode;
        self::$useDayMajorLayout = false;
        self::$dayLabels = array_fill(0, self::$activeDayCount, '');
        self::$dayJsonPrefixes = array_fill(0, self::$activeDayCount, '');
        self::$outputDaySlots = range(0, self::$activeDayCount - 1);
        self::$outputDayPrefixes = array_fill(0, self::$activeDayCount, '');
        self::$countBaseOffsetsByte = [];
        self::$countBaseOffsetsWord = [];

        for ($dayIndex = 0, $dayCode = $minDayCode; $dayCode <= $maxDayCode; $dayCode++, $dayIndex++) {
            self::$dayLabels[$dayIndex] = self::formatDayCode($dayCode);
            self::$dayJsonPrefixes[$dayIndex] = '        "' . self::$dayLabels[$dayIndex] . '": ';
            self::$outputDayPrefixes[$dayIndex] = self::$dayJsonPrefixes[$dayIndex];
        }
    }

    private static function configureOutputDayDomain(array $dayCodes): void
    {
        self::$outputDaySlots = [];
        self::$outputDayPrefixes = [];

        foreach ($dayCodes as $outputIndex => $dayCode) {
            self::$outputDaySlots[$outputIndex] = $dayCode;
            self::$outputDayPrefixes[$outputIndex] = self::$dayJsonPrefixes[$dayCode];
        }
    }

    private static function dayCodeFromDateString(string $dayLabel): int
    {
        return (((int) substr($dayLabel, 2, 2) - 20) << 9)
            | (((int) substr($dayLabel, 5, 2)) << 5)
            | (int) substr($dayLabel, 8, 2);
    }

    private static function resetChunkProcessorCache(): void
    {
        self::$cachedChunkProcessor = null;
        self::$uncachedChunkProcessor = null;
        self::$cachedByteChunkProcessor = null;
        self::$uncachedByteChunkProcessor = null;
        self::$cachedCountOnlyChunkProcessor = null;
        self::$uncachedCountOnlyChunkProcessor = null;
        self::$cachedByteCountOnlyChunkProcessor = null;
        self::$uncachedByteCountOnlyChunkProcessor = null;
    }

    private function parseInParallel(
        string $inputPath,
        string $outputPath,
        int $fileSize,
        int $workerCount,
        bool $useByteCounts,
    ): array
    {
        $profile = $this->shouldProfile();
        $profileStart = $profile ? microtime(true) : 0.0;
        $ranges = $this->resolveRanges($inputPath, $fileSize, $workerCount);
        $rangesResolvedAt = $profile ? microtime(true) : 0.0;

        if (count($ranges) === 1) {
            return $this->parseRange($inputPath, 0, $fileSize, $useByteCounts);
        }

        if ($this->shouldUseSocketTransport()) {
            return $this->parseInParallelWithSockets(
                $inputPath,
                $fileSize,
                $ranges,
                $useByteCounts,
                $profile,
                $profileStart,
                $rangesResolvedAt,
            );
        }

        $tempDir = dirname($outputPath) . '/.parser-tmp-' . md5($outputPath . ':' . getmypid() . ':' . microtime(true));

        if (! is_dir($tempDir) && ! mkdir($tempDir, 0777, true) && ! is_dir($tempDir)) {
            throw new RuntimeException("Unable to create {$tempDir}");
        }

        $children = [];
        $status = 0;
        $uriCount = count(self::$uris);
        $slotCount = $uriCount * self::$activeDayCount;
        $useSodiumMerge = ! $useByteCounts
            && function_exists('sodium_add')
            && getenv('PARSER_DISABLE_SODIUM_MERGE') === false;
        $counts = $useSodiumMerge ? null : array_fill(0, $slotCount, 0);
        $firstSeen = array_fill(0, $uriCount, -1);

        foreach ($ranges as $workerIndex => [$start, $end]) {
            $resultPath = "{$tempDir}/worker-{$workerIndex}.bin";

            if (is_file($resultPath)) {
                unlink($resultPath);
            }

            $pid = pcntl_fork();

            if ($pid === -1) {
                throw new RuntimeException('Unable to fork worker');
            }

            if ($pid === 0) {
                [$counts, $firstSeen] = $this->parseRange($inputPath, $start, $end, $useByteCounts);
                $this->writeWorkerResult($resultPath, $counts, $firstSeen);
                exit(0);
            }

            $children[$pid] = [$workerIndex, $resultPath];
        }

        while ($children !== []) {
            $pid = pcntl_wait($status);

            if ($pid <= 0) {
                break;
            }

            if (! isset($children[$pid])) {
                continue;
            }

            if (! pcntl_wifexited($status) || pcntl_wexitstatus($status) !== 0) {
                throw new RuntimeException("Worker {$pid} failed");
            }

            [$workerIndex, $resultPath] = $children[$pid];
            [$workerCounts, $workerSeen] = $this->readWorkerResult(
                $resultPath,
                $slotCount,
                $uriCount,
                $useSodiumMerge || $useByteCounts,
                $useByteCounts,
            );

            if ($useSodiumMerge) {
                if ($counts === null) {
                    $counts = $workerCounts;
                } else {
                    sodium_add($counts, $workerCounts);
                }
            } else {
                if ($useByteCounts) {
                    for ($slot = 0; $slot < $slotCount; $slot++) {
                        $counts[$slot] += ord($workerCounts[$slot]);
                    }
                } else {
                    for ($slot = 0; $slot < $slotCount; $slot++) {
                        $counts[$slot] += $workerCounts[$slot];
                    }
                }
            }

            $workerPrefix = $workerIndex << 32;

            for ($uriIndex = 0; $uriIndex < $uriCount; $uriIndex++) {
                if ($workerSeen[$uriIndex] === -1) {
                    continue;
                }

                $seen = ($workerIndex << 32) | $workerSeen[$uriIndex];

                if ($firstSeen[$uriIndex] === -1 || $seen < $firstSeen[$uriIndex]) {
                    $firstSeen[$uriIndex] = $seen;
                }
            }

            if (is_file($resultPath)) {
                unlink($resultPath);
            }

            unset($children[$pid]);
        }
        $workersFinishedAt = $profile ? microtime(true) : 0.0;

        @rmdir($tempDir);

        if ($profile) {
            $mergeFinishedAt = microtime(true);
            fwrite(STDERR, sprintf(
                "profile ranges=%.6f wait=%.6f merge=%.6f total=%.6f\n",
                $rangesResolvedAt - $profileStart,
                $workersFinishedAt - $rangesResolvedAt,
                $mergeFinishedAt - $workersFinishedAt,
                $mergeFinishedAt - $profileStart,
            ));
        }

        return [$counts ?? str_repeat("\0", $slotCount * 2), $firstSeen];
    }

    private function parseInParallelCountsOnly(
        string $inputPath,
        string $outputPath,
        int $fileSize,
        int $workerCount,
        bool $useByteCounts,
    ): array|string {
        $profile = $this->shouldProfile();
        $profileStart = $profile ? microtime(true) : 0.0;
        $ranges = $this->resolveRanges($inputPath, $fileSize, $workerCount);
        $rangesResolvedAt = $profile ? microtime(true) : 0.0;

        if (count($ranges) === 1) {
            return $this->parseRangeCountsOnly($inputPath, 0, $fileSize, $useByteCounts);
        }

        if ($this->shouldUseShmopTransport()) {
            try {
                return $this->parseInParallelCountsOnlyWithShmop(
                    $inputPath,
                    $ranges,
                    $useByteCounts,
                    $profile,
                    $profileStart,
                    $rangesResolvedAt,
                );
            } catch (RuntimeException $exception) {
                if (! $this->isRecoverableShmopTransportFailure($exception)) {
                    throw $exception;
                }
            }
        }

        if ($this->shouldUseSocketTransport()) {
            return $this->parseInParallelCountsOnlyWithSockets(
                $inputPath,
                $outputPath,
                $ranges,
                $useByteCounts,
                $profile,
                $profileStart,
                $rangesResolvedAt,
            );
        }

        $tempDir = dirname($outputPath) . '/.parser-tmp-' . md5($outputPath . ':' . getmypid() . ':' . microtime(true));

        if (! is_dir($tempDir) && ! mkdir($tempDir, 0777, true) && ! is_dir($tempDir)) {
            throw new RuntimeException("Unable to create {$tempDir}");
        }

        $children = [];
        $status = 0;
        $uriCount = count(self::$uris);
        $slotCount = $uriCount * self::$activeDayCount;
        $useSodiumMerge = ! $useByteCounts
            && function_exists('sodium_add')
            && getenv('PARSER_DISABLE_SODIUM_MERGE') === false;
        $counts = $useSodiumMerge ? null : array_fill(0, $slotCount, 0);

        foreach ($ranges as $workerIndex => [$start, $end]) {
            $resultPath = "{$tempDir}/worker-{$workerIndex}.bin";

            if (is_file($resultPath)) {
                unlink($resultPath);
            }

            $pid = pcntl_fork();

            if ($pid === -1) {
                throw new RuntimeException('Unable to fork worker');
            }

            if ($pid === 0) {
                $workerCounts = $this->parseRangeCountsOnly($inputPath, $start, $end, $useByteCounts);
                $this->writeWorkerCounts($resultPath, $workerCounts);
                exit(0);
            }

            $children[$pid] = $resultPath;
        }

        while ($children !== []) {
            $pid = pcntl_wait($status);

            if ($pid <= 0) {
                break;
            }

            if (! isset($children[$pid])) {
                continue;
            }

            if (! pcntl_wifexited($status) || pcntl_wexitstatus($status) !== 0) {
                throw new RuntimeException("Worker {$pid} failed");
            }

            $workerCounts = $this->readWorkerCounts(
                $children[$pid],
                $slotCount,
                $useSodiumMerge || $useByteCounts,
                $useByteCounts,
            );
            $this->mergeCounts($counts, $workerCounts, $slotCount, $useSodiumMerge, $useByteCounts);

            if (is_file($children[$pid])) {
                unlink($children[$pid]);
            }

            unset($children[$pid]);
        }
        $workersFinishedAt = $profile ? microtime(true) : 0.0;

        @rmdir($tempDir);

        if ($profile) {
            $mergeFinishedAt = microtime(true);
            fwrite(STDERR, sprintf(
                "profile ranges=%.6f wait=%.6f merge=%.6f total=%.6f\n",
                $rangesResolvedAt - $profileStart,
                $workersFinishedAt - $rangesResolvedAt,
                $mergeFinishedAt - $workersFinishedAt,
                $mergeFinishedAt - $profileStart,
            ));
        }

        return $counts ?? str_repeat("\0", $slotCount * 2);
    }

    private function parseInParallelWithSockets(
        string $inputPath,
        int $fileSize,
        array $ranges,
        bool $useByteCounts,
        bool $profile,
        float $profileStart,
        float $rangesResolvedAt,
    ): array
    {
        $uriCount = count(self::$uris);
        $slotCount = $uriCount * self::$activeDayCount;
        $payloadSize = $this->resolveWorkerPayloadSize($slotCount, $uriCount, $useByteCounts);
        $useSodiumMerge = ! $useByteCounts
            && function_exists('sodium_add')
            && getenv('PARSER_DISABLE_SODIUM_MERGE') === false;
        $counts = $useSodiumMerge ? null : array_fill(0, $slotCount, 0);
        $firstSeen = array_fill(0, $uriCount, -1);
        $children = [];
        $status = 0;

        foreach ($ranges as $workerIndex => [$start, $end]) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);

            if ($pair === false) {
                throw new RuntimeException('Unable to create worker socket pair');
            }

            [$parentStream, $childStream] = $pair;
            $this->tuneWorkerSocketStream($parentStream, $payloadSize, SO_RCVBUF);
            $this->tuneWorkerSocketStream($childStream, $payloadSize, SO_SNDBUF);
            @stream_set_chunk_size($parentStream, $payloadSize);
            @stream_set_chunk_size($childStream, $payloadSize);
            stream_set_blocking($parentStream, false);
            $pid = pcntl_fork();

            if ($pid === -1) {
                fclose($parentStream);
                fclose($childStream);
                throw new RuntimeException('Unable to fork worker');
            }

            if ($pid === 0) {
                fclose($parentStream);
                [$workerCounts, $workerSeen] = $this->parseRange($inputPath, $start, $end, $useByteCounts);
                $this->writeWorkerStream($childStream, $workerCounts, $workerSeen);
                fclose($childStream);
                exit(0);
            }

            fclose($childStream);
            $children[$pid] = [
                'workerIndex' => $workerIndex,
                'stream' => $parentStream,
            ];
        }

        while ($children !== []) {
            $pid = pcntl_wait($status);

            if ($pid <= 0) {
                break;
            }

            if (! isset($children[$pid])) {
                continue;
            }

            if (! pcntl_wifexited($status) || pcntl_wexitstatus($status) !== 0) {
                throw new RuntimeException("Worker {$pid} failed");
            }

            $stream = $children[$pid]['stream'];
            $raw = stream_get_contents($stream);
            fclose($stream);

            if ($raw === false) {
                throw new RuntimeException('Unable to read worker payload');
            }

            [$workerCounts, $workerSeen] = $this->decodeWorkerResult(
                $raw,
                $slotCount,
                $uriCount,
                $useSodiumMerge || $useByteCounts,
                $useByteCounts,
            );
            $this->mergeWorkerResult(
                $counts,
                $firstSeen,
                $workerCounts,
                $workerSeen,
                $children[$pid]['workerIndex'],
                $slotCount,
                $uriCount,
                $useSodiumMerge,
                $useByteCounts,
            );
            unset($children[$pid]);
        }

        if ($profile) {
            $finishedAt = microtime(true);
            fwrite(STDERR, sprintf(
                "profile ranges=%.6f wait=%.6f merge=%.6f total=%.6f\n",
                $rangesResolvedAt - $profileStart,
                $finishedAt - $rangesResolvedAt,
                0.0,
                $finishedAt - $profileStart,
            ));
        }

        return [$counts ?? str_repeat("\0", $slotCount * 2), $firstSeen];
    }

    private function parseInParallelCountsOnlyWithShmop(
        string $inputPath,
        array $ranges,
        bool $useByteCounts,
        bool $profile,
        float $profileStart,
        float $rangesResolvedAt,
    ): string {
        $uriCount = count(self::$uris);
        $slotCount = $uriCount * self::$activeDayCount;
        $payloadSize = $useByteCounts ? $slotCount : $slotCount * 2;
        $useSodiumMerge = ! $useByteCounts
            && function_exists('sodium_add')
            && getenv('PARSER_DISABLE_SODIUM_MERGE') === false;
        $counts = $useSodiumMerge ? null : array_fill(0, $slotCount, 0);
        $children = [];
        $status = 0;

        try {
            foreach ($ranges as [$start, $end]) {
                $segment = $this->createWorkerShmopSegment($payloadSize);
                $pid = pcntl_fork();

                if ($pid === -1) {
                    $this->deleteWorkerShmopSegment($segment);
                    throw new RuntimeException('Unable to fork worker');
                }

                if ($pid === 0) {
                    $workerCounts = $this->parseRangeCountsOnly($inputPath, $start, $end, $useByteCounts);
                    $this->writeWorkerShmopCounts($segment, $payloadSize, $workerCounts);
                    $this->closeWorkerShmopSegment($segment);
                    exit(0);
                }

                $children[$pid] = $segment;
            }

            while ($children !== []) {
                $pid = pcntl_wait($status);

                if ($pid <= 0) {
                    break;
                }

                if (! isset($children[$pid])) {
                    continue;
                }

                $handle = $children[$pid];

                if (! pcntl_wifexited($status) || pcntl_wexitstatus($status) !== 0) {
                    $this->deleteWorkerShmopSegment($handle);
                    throw new RuntimeException("Worker {$pid} failed");
                }

                $workerCounts = shmop_read($handle, 0, $payloadSize);

                if ($workerCounts === false || strlen($workerCounts) !== $payloadSize) {
                    $this->deleteWorkerShmopSegment($handle);
                    throw new RuntimeException('Unable to read worker shared memory payload');
                }

                $this->mergeCounts($counts, $workerCounts, $slotCount, $useSodiumMerge, $useByteCounts);
                $this->deleteWorkerShmopSegment($handle);
                unset($children[$pid]);
            }
        } finally {
            foreach ($children as $handle) {
                $this->deleteWorkerShmopSegment($handle);
            }
        }

        if ($profile) {
            $finishedAt = microtime(true);
            fwrite(STDERR, sprintf(
                "profile ranges=%.6f wait=%.6f merge=%.6f total=%.6f\n",
                $rangesResolvedAt - $profileStart,
                $finishedAt - $rangesResolvedAt,
                0.0,
                $finishedAt - $profileStart,
            ));
        }

        return $counts ?? str_repeat("\0", $slotCount * 2);
    }

    private function parseInParallelCountsOnlyWithSockets(
        string $inputPath,
        string $outputPath,
        array $ranges,
        bool $useByteCounts,
        bool $profile,
        float $profileStart,
        float $rangesResolvedAt,
    ): array|string {
        $uriCount = count(self::$uris);
        $slotCount = $uriCount * self::$activeDayCount;
        $payloadSize = $this->resolveWorkerCountsPayloadSize($slotCount, $useByteCounts);
        $useSodiumMerge = ! $useByteCounts
            && function_exists('sodium_add')
            && getenv('PARSER_DISABLE_SODIUM_MERGE') === false;
        $counts = $useSodiumMerge ? null : array_fill(0, $slotCount, 0);
        $children = [];
        $status = 0;
        $buffers = [];
        $readClosed = [];
        $exited = [];
        $streamPids = [];

        foreach ($ranges as $workerIndex => [$start, $end]) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);

            if ($pair === false) {
                throw new RuntimeException('Unable to create worker socket pair');
            }

            [$parentStream, $childStream] = $pair;
            $this->tuneWorkerSocketStream($parentStream, $payloadSize, SO_RCVBUF);
            $this->tuneWorkerSocketStream($childStream, $payloadSize, SO_SNDBUF);
            @stream_set_chunk_size($parentStream, $payloadSize);
            @stream_set_chunk_size($childStream, $payloadSize);
            stream_set_blocking($parentStream, false);
            $pid = pcntl_fork();

            if ($pid === -1) {
                fclose($parentStream);
                fclose($childStream);
                throw new RuntimeException('Unable to fork worker');
            }

            if ($pid === 0) {
                fclose($parentStream);
                $workerCounts = $this->parseRangeCountsOnly($inputPath, $start, $end, $useByteCounts);
                $this->writeWorkerCountsStream($childStream, $workerCounts);
                fclose($childStream);
                exit(0);
            }

            fclose($childStream);
            $children[$pid] = $parentStream;
            $buffers[$pid] = '';
            $readClosed[$pid] = false;
            $exited[$pid] = false;
            $streamPids[(int) $parentStream] = $pid;
        }

        while ($children !== []) {
            $readStreams = [];

            foreach ($children as $pid => $stream) {
                if (! $readClosed[$pid]) {
                    $readStreams[] = $stream;
                }
            }

            if ($readStreams !== []) {
                $writeStreams = null;
                $exceptStreams = null;
                $ready = @stream_select($readStreams, $writeStreams, $exceptStreams, 0, 200000);

                if ($ready === false) {
                    throw new RuntimeException('Unable to poll worker streams');
                }

                if ($ready > 0) {
                    foreach ($readStreams as $stream) {
                        $pid = $streamPids[(int) $stream] ?? null;

                        if ($pid === null) {
                            continue;
                        }

                        $chunk = fread($stream, 262144);

                        if ($chunk === false) {
                            throw new RuntimeException('Unable to read worker payload');
                        }

                        if ($chunk !== '') {
                            $buffers[$pid] .= $chunk;
                        }

                        if (feof($stream)) {
                            fclose($stream);
                            $readClosed[$pid] = true;
                        }
                    }
                }
            }

            while (($pid = pcntl_waitpid(-1, $status, WNOHANG)) > 0) {
                if (! isset($children[$pid])) {
                    continue;
                }

                if (! pcntl_wifexited($status) || pcntl_wexitstatus($status) !== 0) {
                    throw new RuntimeException("Worker {$pid} failed");
                }

                $exited[$pid] = true;
            }

            foreach (array_keys($children) as $pid) {
                if (! $readClosed[$pid] || ! $exited[$pid]) {
                    continue;
                }

                $workerCounts = $this->decodeWorkerCounts(
                    $buffers[$pid],
                    $slotCount,
                    $useSodiumMerge || $useByteCounts,
                    $useByteCounts,
                );
                $this->mergeCounts($counts, $workerCounts, $slotCount, $useSodiumMerge, $useByteCounts);
                unset($streamPids[(int) $children[$pid]], $children[$pid], $buffers[$pid], $readClosed[$pid], $exited[$pid]);
            }
        }

        if ($profile) {
            $finishedAt = microtime(true);
            fwrite(STDERR, sprintf(
                "profile ranges=%.6f wait=%.6f merge=%.6f total=%.6f\n",
                $rangesResolvedAt - $profileStart,
                $finishedAt - $rangesResolvedAt,
                0.0,
                $finishedAt - $profileStart,
            ));
        }

        return $counts ?? str_repeat("\0", $slotCount * 2);
    }

    private function resolveRanges(string $inputPath, int $fileSize, int $workerCount): array
    {
        if ($workerCount <= 1) {
            return [[0, $fileSize]];
        }

        $handle = fopen($inputPath, 'rb');

        if ($handle === false) {
            throw new RuntimeException("Unable to open {$inputPath}");
        }

        $offsets = [0];
        $sliceSize = intdiv($fileSize, $workerCount);

        for ($worker = 1; $worker < $workerCount; $worker++) {
            fseek($handle, $sliceSize * $worker);
            fgets($handle);

            $offset = ftell($handle);

            if ($offset === false) {
                break;
            }

            $offsets[] = $offset;
        }

        $offsets[] = $fileSize;
        fclose($handle);

        $ranges = [];
        $offsetCount = count($offsets) - 1;

        for ($index = 0; $index < $offsetCount; $index++) {
            if ($offsets[$index] < $offsets[$index + 1]) {
                $ranges[] = [$offsets[$index], $offsets[$index + 1]];
            }
        }

        return $ranges;
    }

    private function parseRange(string $inputPath, int $start, int $end, bool $useByteCounts): array
    {
        $chunkProcessor = self::resolveChunkProcessor(
            trackFirstSeen: true,
            useDateCache: $this->shouldUseDateCache(),
            useByteCounts: $useByteCounts,
        );
        $uriCount = count(self::$uris);
        $readSize = $this->resolveReadSize();
        $slotCount = $uriCount * self::$activeDayCount;
        $counts = str_repeat("\0", $useByteCounts ? $slotCount : $slotCount * 2);
        $firstSeen = array_fill(0, $uriCount, -1);
        $handle = fopen($inputPath, 'rb');

        if ($handle === false) {
            throw new RuntimeException("Unable to open {$inputPath}");
        }

        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);

        $carry = '';
        $parsedDateCache = [];
        $remainingFirstSeen = $uriCount;
        $sequence = 0;
        $position = $start;

        while ($position < $end) {
            $remaining = $end - $position;
            $buffer = fread($handle, $remaining > $readSize ? $readSize : $remaining);

            if ($buffer === false || $buffer === '') {
                break;
            }

            $position += strlen($buffer);

            if ($carry !== '') {
                $buffer = $carry . $buffer;
                $carry = '';
            }

            if ($position < $end) {
                $lastNewline = strrpos($buffer, "\n");

                if ($lastNewline === false) {
                    $carry = $buffer;
                    continue;
                }

                $carry = substr($buffer, $lastNewline + 1);
                $limit = $lastNewline + 1;
            } else {
                $limit = strlen($buffer);
            }

            $chunkProcessor($buffer, $limit, $counts, $firstSeen, $parsedDateCache, $remainingFirstSeen, $sequence, $uriCount);
        }

        if ($carry !== '') {
            $buffer = $carry . "\n";
            $chunkProcessor($buffer, strlen($buffer), $counts, $firstSeen, $parsedDateCache, $remainingFirstSeen, $sequence, $uriCount);
        }

        fclose($handle);

        return [$counts, $firstSeen];
    }

    private function parseRangeCountsOnly(string $inputPath, int $start, int $end, bool $useByteCounts): string
    {
        $chunkProcessor = self::resolveChunkProcessor(
            trackFirstSeen: false,
            useDateCache: $this->shouldUseDateCache(),
            useByteCounts: $useByteCounts,
        );
        $uriCount = count(self::$uris);
        $readSize = $this->resolveReadSize();
        $slotCount = $uriCount * self::$activeDayCount;
        $counts = str_repeat("\0", $useByteCounts ? $slotCount : $slotCount * 2);
        $handle = fopen($inputPath, 'rb');

        if ($handle === false) {
            throw new RuntimeException("Unable to open {$inputPath}");
        }

        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);

        $carry = '';
        $parsedDateCache = [];
        $position = $start;

        while ($position < $end) {
            $remaining = $end - $position;
            $buffer = fread($handle, $remaining > $readSize ? $readSize : $remaining);

            if ($buffer === false || $buffer === '') {
                break;
            }

            $position += strlen($buffer);

            if ($carry !== '') {
                $buffer = $carry . $buffer;
                $carry = '';
            }

            if ($position < $end) {
                $lastNewline = strrpos($buffer, "\n");

                if ($lastNewline === false) {
                    $carry = $buffer;
                    continue;
                }

                $carry = substr($buffer, $lastNewline + 1);
                $limit = $lastNewline + 1;
            } else {
                $limit = strlen($buffer);
            }

            $chunkProcessor($buffer, $limit, $counts, $parsedDateCache, $uriCount);
        }

        if ($carry !== '') {
            $buffer = $carry . "\n";
            $chunkProcessor($buffer, strlen($buffer), $counts, $parsedDateCache, $uriCount);
        }

        fclose($handle);

        return $counts;
    }


    private function resolveReadSize(): int
    {
        $override = getenv('PARSER_READ_SIZE');

        if ($override !== false && ctype_digit($override)) {
            return max(1_048_576, (int) $override);
        }

        return self::READ_SIZE;
    }

    private function buildOutput(array|string $counts, array $orderedUris, bool $useByteCounts): string
    {
        if (is_string($counts)) {
            return $useByteCounts
                ? $this->buildOutputFromByteCounts($counts, $orderedUris)
                : $this->buildOutputFromWordCounts($counts, $orderedUris);
        }

        return $this->buildOutputFromArrayCounts($counts, $orderedUris);
    }

    private function buildOutputFromArrayCounts(array $counts, array $orderedUris): string
    {
        $outputDayPrefixes = self::$outputDayPrefixes;
        $outputDaySlots = self::$outputDaySlots;
        $outputDayCount = count($outputDayPrefixes);
        $useDayMajorLayout = self::$useDayMajorLayout;
        $uriCount = count(self::$uris);
        $buffer = "{\n";
        $uriTotal = count($orderedUris);

        foreach ($orderedUris as $uriOffset => $uriIndex) {
            $buffer .= self::$uriJsonOpeners[$uriIndex];
            $hasVisit = false;

            if ($useDayMajorLayout) {
                $slot = $uriIndex;

                for ($dayIndex = 0; $dayIndex < $outputDayCount; $dayIndex++) {
                    $count = $counts[$slot];

                    if ($count !== 0) {
                        if ($hasVisit) {
                            $buffer .= ",\n";
                        }

                        $buffer .= $outputDayPrefixes[$dayIndex] . $count;
                        $hasVisit = true;
                    }

                    $slot += $uriCount;
                }
            } else {
                $slotBase = $uriIndex * self::$activeDayCount;

                for ($dayIndex = 0; $dayIndex < $outputDayCount; $dayIndex++) {
                    $count = $counts[$slotBase + $outputDaySlots[$dayIndex]];

                    if ($count !== 0) {
                        if ($hasVisit) {
                            $buffer .= ",\n";
                        }

                        $buffer .= $outputDayPrefixes[$dayIndex] . $count;
                        $hasVisit = true;
                    }
                }
            }

            $buffer .= "\n    }";

            if ($uriOffset !== $uriTotal - 1) {
                $buffer .= ",\n";
            } else {
                $buffer .= "\n";
            }
        }

        return $buffer . '}';
    }

    private function buildOutputFromByteCounts(string $counts, array $orderedUris): string
    {
        $outputDayPrefixes = self::$outputDayPrefixes;
        $outputDaySlots = self::$outputDaySlots;
        $outputDayCount = count($outputDayPrefixes);
        $useDayMajorLayout = self::$useDayMajorLayout;
        $uriCount = count(self::$uris);
        $buffer = "{\n";
        $uriTotal = count($orderedUris);

        foreach ($orderedUris as $uriOffset => $uriIndex) {
            $buffer .= self::$uriJsonOpeners[$uriIndex];
            $hasVisit = false;

            if ($useDayMajorLayout) {
                $slot = $uriIndex;

                for ($dayIndex = 0; $dayIndex < $outputDayCount; $dayIndex++) {
                    $countByte = $counts[$slot];

                    if ($countByte !== "\0") {
                        if ($hasVisit) {
                            $buffer .= ",\n";
                        }

                        $buffer .= $outputDayPrefixes[$dayIndex] . ord($countByte);
                        $hasVisit = true;
                    }

                    $slot += $uriCount;
                }
            } else {
                $slotBase = $uriIndex * self::$activeDayCount;

                for ($dayIndex = 0; $dayIndex < $outputDayCount; $dayIndex++) {
                    $countByte = $counts[$slotBase + $outputDaySlots[$dayIndex]];

                    if ($countByte !== "\0") {
                        if ($hasVisit) {
                            $buffer .= ",\n";
                        }

                        $buffer .= $outputDayPrefixes[$dayIndex] . ord($countByte);
                        $hasVisit = true;
                    }
                }
            }

            $buffer .= "\n    }";

            if ($uriOffset !== $uriTotal - 1) {
                $buffer .= ",\n";
            } else {
                $buffer .= "\n";
            }
        }

        return $buffer . '}';
    }

    private function buildOutputFromWordCounts(string $counts, array $orderedUris): string
    {
        $outputDayPrefixes = self::$outputDayPrefixes;
        $outputDaySlots = self::$outputDaySlots;
        $outputDayCount = count($outputDayPrefixes);
        $useDayMajorLayout = self::$useDayMajorLayout;
        $uriCount = count(self::$uris);
        $slotStride = $useDayMajorLayout ? ($uriCount << 1) : 2;
        $buffer = "{\n";
        $uriTotal = count($orderedUris);

        foreach ($orderedUris as $uriOffset => $uriIndex) {
            $buffer .= self::$uriJsonOpeners[$uriIndex];
            $hasVisit = false;

            if ($useDayMajorLayout) {
                $slot = $uriIndex << 1;

                for ($dayIndex = 0; $dayIndex < $outputDayCount; $dayIndex++) {
                    $lowByte = ord($counts[$slot]);
                    $highByte = ord($counts[$slot + 1]);

                    if (($lowByte | $highByte) !== 0) {
                        if ($hasVisit) {
                            $buffer .= ",\n";
                        }

                        $buffer .= $outputDayPrefixes[$dayIndex] . ($lowByte | ($highByte << 8));
                        $hasVisit = true;
                    }

                    $slot += $slotStride;
                }
            } else {
                $slotBase = $uriIndex * self::$activeDayCount;

                for ($dayIndex = 0; $dayIndex < $outputDayCount; $dayIndex++) {
                    $slot = ($slotBase + $outputDaySlots[$dayIndex]) << 1;
                    $lowByte = ord($counts[$slot]);
                    $highByte = ord($counts[$slot + 1]);

                    if (($lowByte | $highByte) !== 0) {
                        if ($hasVisit) {
                            $buffer .= ",\n";
                        }

                        $buffer .= $outputDayPrefixes[$dayIndex] . ($lowByte | ($highByte << 8));
                        $hasVisit = true;
                    }
                }
            }

            $buffer .= "\n    }";

            if ($uriOffset !== $uriTotal - 1) {
                $buffer .= ",\n";
            } else {
                $buffer .= "\n";
            }
        }

        return $buffer . '}';
    }

    private static function formatDayCode(int $dayCode): string
    {
        return self::$dateCache[$dayCode] ??= sprintf(
            '%04d-%02d-%02d',
            2020 + ($dayCode >> 9),
            ($dayCode >> 5) & 0x0F,
            $dayCode & 0x1F,
        );
    }

    private function writeWorkerResult(string $path, array|string $counts, array $firstSeen): void
    {
        $handle = fopen($path, 'wb');

        if ($handle === false) {
            throw new RuntimeException("Unable to open {$path}");
        }

        $this->writeEncodedWorkerResult($handle, $counts, $firstSeen);

        fclose($handle);
    }

    private function writeWorkerCounts(string $path, array|string $counts): void
    {
        $handle = fopen($path, 'wb');

        if ($handle === false) {
            throw new RuntimeException("Unable to open {$path}");
        }

        $this->writeWorkerCountsStream($handle, $counts);

        fclose($handle);
    }

    private function writeWorkerStream($stream, array|string $counts, array $firstSeen): void
    {
        $this->writeEncodedWorkerResult($stream, $counts, $firstSeen);
    }

    private function writeWorkerCountsStream($stream, array|string $counts): void
    {
        if ($this->shouldUseIgbinary()) {
            $this->writeAll($stream, igbinary_serialize($counts));

            return;
        }

        $this->writeAll($stream, $counts);
    }

    private function writeEncodedWorkerResult($stream, array|string $counts, array $firstSeen): void
    {
        if ($this->shouldUseIgbinary()) {
            $this->writeAll($stream, igbinary_serialize([$firstSeen, $counts]));

            return;
        }

        $this->writeAll($stream, $this->encodeFirstSeen($firstSeen));
        $this->writeAll($stream, $counts);
    }

    private function encodeWorkerResult(array|string $counts, array $firstSeen): string
    {
        if ($this->shouldUseIgbinary()) {
            return igbinary_serialize([$firstSeen, $counts]);
        }

        return $this->encodeFirstSeen($firstSeen) . $counts;
    }

    private function readWorkerResult(
        string $path,
        int $slotCount,
        int $uriCount,
        bool $rawCounts = false,
        bool $useByteCounts = false,
    ): array
    {
        if ($this->shouldUseIgbinary()) {
            $result = igbinary_unserialize(file_get_contents($path));

            if (! is_array($result) || count($result) !== 2) {
                throw new RuntimeException("Unable to read {$path}");
            }

            if (! $rawCounts && is_string($result[1])) {
                $result[1] = array_values(unpack('v*', $result[1]));
            }

            return $result;
        }

        $raw = file_get_contents($path);

        if ($raw === false) {
            throw new RuntimeException("Unable to read {$path}");
        }

        return $this->decodeWorkerResult($raw, $slotCount, $uriCount, $rawCounts, $useByteCounts);
    }

    private function readWorkerCounts(
        string $path,
        int $slotCount,
        bool $rawCounts = false,
        bool $useByteCounts = false,
    ): array|string {
        $raw = file_get_contents($path);

        if ($raw === false) {
            throw new RuntimeException("Unable to read {$path}");
        }

        return $this->decodeWorkerCounts($raw, $slotCount, $rawCounts, $useByteCounts);
    }

    private function decodeWorkerResult(
        string $raw,
        int $slotCount,
        int $uriCount,
        bool $rawCounts = false,
        bool $useByteCounts = false,
    ): array
    {
        if ($this->shouldUseIgbinary()) {
            $result = igbinary_unserialize($raw);

            if (! is_array($result) || count($result) !== 2) {
                throw new RuntimeException('Unable to decode worker payload');
            }

            if (! $rawCounts && is_string($result[1])) {
                $result[1] = array_values(unpack($useByteCounts ? 'C*' : 'v*', $result[1]));
            }

            return $result;
        }

        $headerSize = $uriCount * 4;
        $firstSeen = array_values(unpack('V*', substr($raw, 0, $headerSize)));
        $countSize = $useByteCounts ? $slotCount : $slotCount * 2;
        $counts = substr($raw, $headerSize, $countSize);

        if (! $rawCounts) {
            $counts = array_values(unpack($useByteCounts ? 'C*' : 'v*', $counts));
        }

        foreach ($firstSeen as $index => $value) {
            if ($value === 0xFFFFFFFF) {
                $firstSeen[$index] = -1;
            }
        }

        return [$counts, $firstSeen];
    }

    private function decodeWorkerCounts(
        string $raw,
        int $slotCount,
        bool $rawCounts = false,
        bool $useByteCounts = false,
    ): array|string {
        if ($this->shouldUseIgbinary()) {
            $counts = igbinary_unserialize($raw);

            if (! $rawCounts && is_string($counts)) {
                $counts = array_values(unpack($useByteCounts ? 'C*' : 'v*', $counts));
            }

            return $counts;
        }

        if (! $rawCounts) {
            return array_values(unpack($useByteCounts ? 'C*' : 'v*', $raw));
        }

        return $raw;
    }

    private function mergeWorkerResult(
        array|string|null &$counts,
        array &$firstSeen,
        array|string $workerCounts,
        array $workerSeen,
        int $workerIndex,
        int $slotCount,
        int $uriCount,
        bool $useSodiumMerge,
        bool $useByteCounts,
    ): void {
        if ($useSodiumMerge) {
            if ($counts === null) {
                $counts = $workerCounts;
            } else {
                sodium_add($counts, $workerCounts);
            }
        } else {
            if ($useByteCounts) {
                for ($slot = 0; $slot < $slotCount; $slot++) {
                    $counts[$slot] += ord($workerCounts[$slot]);
                }
            } else {
                for ($slot = 0; $slot < $slotCount; $slot++) {
                    $counts[$slot] += $workerCounts[$slot];
                }
            }
        }

        for ($uriIndex = 0; $uriIndex < $uriCount; $uriIndex++) {
            if ($workerSeen[$uriIndex] === -1) {
                continue;
            }

            $seen = ($workerIndex << 32) | $workerSeen[$uriIndex];

            if ($firstSeen[$uriIndex] === -1 || $seen < $firstSeen[$uriIndex]) {
                $firstSeen[$uriIndex] = $seen;
            }
        }
    }

    private function mergeCounts(
        array|string|null &$counts,
        array|string $workerCounts,
        int $slotCount,
        bool $useSodiumMerge,
        bool $useByteCounts,
    ): void {
        if ($useSodiumMerge) {
            if ($counts === null) {
                $counts = $workerCounts;
            } else {
                sodium_add($counts, $workerCounts);
            }

            return;
        }

        if ($useByteCounts) {
            for ($slot = 0; $slot < $slotCount; $slot++) {
                $counts[$slot] += ord($workerCounts[$slot]);
            }

            return;
        }

        for ($slot = 0; $slot < $slotCount; $slot++) {
            $counts[$slot] += $workerCounts[$slot];
        }
    }

    private function encodeFirstSeen(array $firstSeen): string
    {
        $encodedSeen = [];

        foreach ($firstSeen as $index => $value) {
            $encodedSeen[$index] = $value === -1 ? 0xFFFFFFFF : $value;
        }

        return pack('V*', ...$encodedSeen);
    }

    private function writeAll($stream, string $payload): void
    {
        $written = 0;
        $length = strlen($payload);

        while ($written < $length) {
            $chunk = fwrite($stream, $written === 0 ? $payload : substr($payload, $written));

            if ($chunk === false || $chunk === 0) {
                throw new RuntimeException('Unable to write worker payload');
            }

            $written += $chunk;
        }
    }

    private function createWorkerShmopSegment(int $payloadSize)
    {
        for ($attempt = 0; $attempt < 64; $attempt++) {
            $key = random_int(1, 0x7FFFFFFF);
            $handle = @shmop_open($key, 'n', 0600, $payloadSize);

            if ($handle !== false) {
                return $handle;
            }
        }

        throw new RuntimeException('Unable to allocate worker shared memory');
    }

    private function writeWorkerShmopCounts($handle, int $payloadSize, string $counts): void
    {
        $written = shmop_write($handle, $counts, 0);

        if ($written !== $payloadSize) {
            throw new RuntimeException('Unable to write worker shared memory payload');
        }
    }

    private function deleteWorkerShmopSegment($handle): void
    {
        @shmop_delete($handle);
        $this->closeWorkerShmopSegment($handle);
    }

    private function closeWorkerShmopSegment($handle): void
    {
        if (function_exists('shmop_close')) {
            @shmop_close($handle);
        }
    }

    private function isRecoverableShmopTransportFailure(RuntimeException $exception): bool
    {
        return str_starts_with($exception->getMessage(), 'Unable to allocate worker shared memory')
            || str_starts_with($exception->getMessage(), 'Unable to open worker shared memory segment')
            || str_starts_with($exception->getMessage(), 'Unable to read worker shared memory payload')
            || str_starts_with($exception->getMessage(), 'Unable to write worker shared memory payload');
    }

    private function resolveWorkerPayloadSize(int $slotCount, int $uriCount, bool $useByteCounts): int
    {
        $headerSize = $uriCount * 4;

        if ($this->shouldUseIgbinary()) {
            return $headerSize + ($useByteCounts ? $slotCount : $slotCount * 2) + 4096;
        }

        return $headerSize + ($useByteCounts ? $slotCount : $slotCount * 2);
    }

    private function resolveWorkerCountsPayloadSize(int $slotCount, bool $useByteCounts): int
    {
        if ($this->shouldUseIgbinary()) {
            return ($useByteCounts ? $slotCount : $slotCount * 2) + 4096;
        }

        return $useByteCounts ? $slotCount : $slotCount * 2;
    }

    private function tuneWorkerSocketStream($stream, int $payloadSize, int $option): void
    {
        if (! function_exists('socket_import_stream') || ! function_exists('socket_set_option')) {
            return;
        }

        $socket = @socket_import_stream($stream);

        if ($socket === false) {
            return;
        }

        @socket_set_option($socket, SOL_SOCKET, $option, $payloadSize);
    }

    private function shouldUseIgbinary(): bool
    {
        if (! function_exists('igbinary_serialize') || ! function_exists('igbinary_unserialize')) {
            return false;
        }

        $override = getenv('PARSER_USE_IGBINARY');

        if ($override === false) {
            return false;
        }

        return $override !== '0';
    }

    private function shouldUseDateCache(): bool
    {
        return getenv('PARSER_DISABLE_DATE_CACHE') === false;
    }

    private function prewarmCountOnlyChunkProcessor(bool $useByteCounts): void
    {
        self::resolveChunkProcessor(
            trackFirstSeen: false,
            useDateCache: $this->shouldUseDateCache(),
            useByteCounts: $useByteCounts,
        );
    }

    private function shouldUseCompactDayDomain(): bool
    {
        return getenv('PARSER_USE_COMPACT_DAYS') !== false;
    }

    private function shouldUseBandedDayDomain(): bool
    {
        return getenv('PARSER_USE_BANDED_DAYS') !== false;
    }

    private static function shouldUseIntegerDateCache(): bool
    {
        return getenv('PARSER_USE_INT_DATE_CACHE') !== false;
    }

    private static function shouldCheckByteOverflow(): bool
    {
        return getenv('PARSER_CHECK_BYTE_OVERFLOW') !== false;
    }

    private static function resolveChunkProcessor(
        bool $trackFirstSeen,
        bool $useDateCache,
        bool $useByteCounts,
    ): \Closure {
        if ($trackFirstSeen) {
            if ($useByteCounts) {
                if ($useDateCache) {
                    return self::$cachedByteChunkProcessor ??= self::buildChunkProcessor(
                        trackFirstSeen: true,
                        useDateCache: true,
                        useByteCounts: true,
                        useIntegerDateCache: self::$useIntegerDateCache ?? false,
                    );
                }

                return self::$uncachedByteChunkProcessor ??= self::buildChunkProcessor(
                    trackFirstSeen: true,
                    useDateCache: false,
                    useByteCounts: true,
                    useIntegerDateCache: false,
                );
            }

            if ($useDateCache) {
                return self::$cachedChunkProcessor ??= self::buildChunkProcessor(
                    trackFirstSeen: true,
                    useDateCache: true,
                    useByteCounts: false,
                    useIntegerDateCache: self::$useIntegerDateCache ?? false,
                );
            }

            return self::$uncachedChunkProcessor ??= self::buildChunkProcessor(
                trackFirstSeen: true,
                useDateCache: false,
                useByteCounts: false,
                useIntegerDateCache: false,
            );
        }

        if ($useByteCounts) {
            if ($useDateCache) {
                return self::$cachedByteCountOnlyChunkProcessor ??= self::buildChunkProcessor(
                    trackFirstSeen: false,
                    useDateCache: true,
                    useByteCounts: true,
                    useIntegerDateCache: self::$useIntegerDateCache ?? false,
                );
            }

            return self::$uncachedByteCountOnlyChunkProcessor ??= self::buildChunkProcessor(
                trackFirstSeen: false,
                useDateCache: false,
                useByteCounts: true,
                useIntegerDateCache: false,
            );
        }

        if ($useDateCache) {
            return self::$cachedCountOnlyChunkProcessor ??= self::buildChunkProcessor(
                trackFirstSeen: false,
                useDateCache: true,
                useByteCounts: false,
                useIntegerDateCache: self::$useIntegerDateCache ?? false,
            );
        }

        return self::$uncachedCountOnlyChunkProcessor ??= self::buildChunkProcessor(
            trackFirstSeen: false,
            useDateCache: false,
            useByteCounts: false,
            useIntegerDateCache: false,
        );
    }

    private function shouldUseByteCounts(int $workerCount): bool
    {
        return $workerCount >= 4 && getenv('PARSER_DISABLE_BYTE_COUNTS') === false;
    }

    private function shouldUseSocketTransport(): bool
    {
        return function_exists('stream_socket_pair')
            && defined('STREAM_PF_UNIX')
            && getenv('PARSER_DISABLE_SOCKET_TRANSPORT') === false;
    }

    private function shouldUseShmopTransport(): bool
    {
        $override = getenv('PARSER_USE_SHMOP_TRANSPORT');

        return $override !== false
            && $override !== '0'
            && function_exists('shmop_open')
            && function_exists('shmop_read')
            && function_exists('shmop_write')
            && function_exists('shmop_delete');
    }


    private function shouldProfile(): bool
    {
        return getenv('PARSER_PROFILE') !== false;
    }

}
