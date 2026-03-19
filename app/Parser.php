<?php

namespace App;

use function chr;
use function chunk_split;
use function count;
use function min;
use function fclose;
use function fgets;
use function file_get_contents;
use function file_put_contents;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function pcntl_fork;
use function pcntl_waitpid;
use function sodium_add;
use function str_repeat;
use function str_replace;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function unlink;
use function unpack;

use const SEEK_CUR;
use const SEEK_END;

final class Parser
{
    public static function parse($inputPath, $outputPath)
    {
        gc_disable();

        $dateStringToId = [];
        $dateIdToString = [];
        $dateCount = 0;
        for ($year = 1; $year <= 6; $year++) {
            for ($month = 1; $month <= 12; $month++) {
                $daysInMonth = match ($month) {
                    2 => $year === 4 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $monthString = ($month < 10 ? '0' : '') . $month;
                $yearMonthPrefix = "$year-$monthString-";
                for ($day = 1; $day <= $daysInMonth; $day++) {
                    $dateKey = $yearMonthPrefix . (($day < 10 ? '0' : '') . $day);
                    $dateStringToId[$dateKey] = $dateCount;
                    $dateIdToString[$dateCount] = '202' . $dateKey;
                    $dateCount++;
                }
            }
        }

        $nextChar = [];
        for ($i = 0; $i < 255; $i++) {
            $nextChar[chr($i)] = chr($i + 1);
        }

        $fileHandle = fopen($inputPath, 'rb');
        stream_set_read_buffer($fileHandle, 0);
        $headerData = fread($fileHandle, 181_000);

        $slugPaths = [];
        $slugLookup = [];
        $slugCount = 0;
        $position = 0;
        $lastNewline = strrpos($headerData, "\n") ?: 0;

        $slugNewlines = [];
        while ($position < $lastNewline && $slugCount < 268) {
            $newlinePos = strpos($headerData, "\n", $position + 52);
            if ($newlinePos === false) break;
            $slug = substr($headerData, $position + 25, $newlinePos - $position - 51);
            if (!isset($slugLookup[$slug])) {
                $slugPaths[$slugCount] = $slug;
                $slugNewlines[$slugCount] = $newlinePos;
                $slugLookup[$slug] = $slugCount * $dateCount;
                $slugCount++;
            }
            $position = $newlinePos + 1;
        }

        $tailLength = 1;
        find_unique_tail:
        $slugLookup = [];
        for ($s = 0; $s < $slugCount; $s++) {
            $tailKey = substr($headerData, $slugNewlines[$s] - 26 - $tailLength, $tailLength);
            if (isset($slugLookup[$tailKey])) {
                $tailLength++;
                goto find_unique_tail;
            }
            $slugLookup[$tailKey] = true;
        }

        $baseMask = (1 << 20) - 1;
        $maxStride = 0;
        $slugLookup = [];
        for ($s = 0; $s < $slugCount; $s++) {
            $stride = strlen($slugPaths[$s]) + 52;
            if ($stride > $maxStride) {
                $maxStride = $stride;
            }
            $tailKey = substr($headerData, $slugNewlines[$s] - 26 - $tailLength, $tailLength);
            $slugLookup[$tailKey] = ($stride << 20) | ($s * $dateCount);
        }

        unset($headerData, $slugNewlines);
        $tailOffset = 26 + $tailLength;
        $unrollFence = ($maxStride * 18) + $tailOffset;

        $outputSize = $slugCount * $dateCount;

        fseek($fileHandle, 0, SEEK_END);
        $fileSize = ftell($fileHandle);
        fclose($fileHandle);

        $grainSize = 8 * 1024 * 1024;
        $grains = [];
        $offset = 0;
        $fileHandle = fopen($inputPath, 'rb');
        stream_set_read_buffer($fileHandle, 0);
        while ($offset < $fileSize) {
            $end = $offset + $grainSize;
            if ($end < $fileSize) {
                fseek($fileHandle, $end);
                fgets($fileHandle);
                $end = ftell($fileHandle);
            } else {
                $end = $fileSize;
            }
            $grains[] = [$offset, $end];
            $offset = $end;
        }
        fclose($fileHandle);

        $workerGrains = [[], [], [], [], [], [], [], []];
        $grainCount = count($grains);
        for ($g = 0; $g < $grainCount; $g++) {
            $workerGrains[$g % 8][] = $grains[$g];
        }

        $tmpDir = '/tmp/parser_' . getmypid();
        @mkdir($tmpDir);

        $workerIndex = 1;
        fork_worker:
        if (pcntl_fork() === 0) {
            $countBuffer = str_repeat("\0", $outputSize);
            $reader = fopen($inputPath, 'r');
            stream_set_read_buffer($reader, 0);
            $myGrains = $workerGrains[$workerIndex];
            $myGrainCount = count($myGrains);
            $grainIdx = 0;

            next_grain_child:
            fseek($reader, $myGrains[$grainIdx][0]);
            $remaining = $myGrains[$grainIdx][1] - $myGrains[$grainIdx][0];

            read_child:
            $chunk = fread($reader, min($remaining, 163_840));
            $chunkLength = strlen($chunk);
            $remaining -= $chunkLength;

            $lastNewline = strrpos($chunk, "\n");
            if ($lastNewline === false) goto advance_grain_child;

            $leftover = $chunkLength - $lastNewline - 1;
            if ($leftover > 0) {
                fseek($reader, -$leftover, SEEK_CUR);
                $remaining += $leftover;
            }

            $cursor = $lastNewline;

            if ($cursor <= $unrollFence) goto cleanup_child;
            unrolled_child:
            $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
            $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
            $countBuffer[$countIndex] = $nextChar[$countBuffer[$countIndex]];
            $cursor -= $packed >> 20;

            $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
            $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
            $countBuffer[$countIndex] = $nextChar[$countBuffer[$countIndex]];
            $cursor -= $packed >> 20;

            $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
            $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
            $countBuffer[$countIndex] = $nextChar[$countBuffer[$countIndex]];
            $cursor -= $packed >> 20;

            $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
            $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
            $countBuffer[$countIndex] = $nextChar[$countBuffer[$countIndex]];
            $cursor -= $packed >> 20;

            $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
            $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
            $countBuffer[$countIndex] = $nextChar[$countBuffer[$countIndex]];
            $cursor -= $packed >> 20;

            $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
            $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
            $countBuffer[$countIndex] = $nextChar[$countBuffer[$countIndex]];
            $cursor -= $packed >> 20;

            $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
            $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
            $countBuffer[$countIndex] = $nextChar[$countBuffer[$countIndex]];
            $cursor -= $packed >> 20;

            $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
            $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
            $countBuffer[$countIndex] = $nextChar[$countBuffer[$countIndex]];
            $cursor -= $packed >> 20;

            $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
            $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
            $countBuffer[$countIndex] = $nextChar[$countBuffer[$countIndex]];
            $cursor -= $packed >> 20;

            $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
            $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
            $countBuffer[$countIndex] = $nextChar[$countBuffer[$countIndex]];
            $cursor -= $packed >> 20;

            $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
            $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
            $countBuffer[$countIndex] = $nextChar[$countBuffer[$countIndex]];
            $cursor -= $packed >> 20;

            $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
            $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
            $countBuffer[$countIndex] = $nextChar[$countBuffer[$countIndex]];
            $cursor -= $packed >> 20;

            $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
            $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
            $countBuffer[$countIndex] = $nextChar[$countBuffer[$countIndex]];
            $cursor -= $packed >> 20;

            $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
            $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
            $countBuffer[$countIndex] = $nextChar[$countBuffer[$countIndex]];
            $cursor -= $packed >> 20;

            $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
            $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
            $countBuffer[$countIndex] = $nextChar[$countBuffer[$countIndex]];
            $cursor -= $packed >> 20;

            $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
            $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
            $countBuffer[$countIndex] = $nextChar[$countBuffer[$countIndex]];
            $cursor -= $packed >> 20;
            if ($cursor > $unrollFence) goto unrolled_child;

            cleanup_child:
            if ($cursor < $tailOffset) goto next_chunk_child;
            $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
            $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
            $countBuffer[$countIndex] = $nextChar[$countBuffer[$countIndex]];
            $cursor -= $packed >> 20;
            if ($cursor >= $tailOffset) goto cleanup_child;

            next_chunk_child:
            if ($remaining > 0) goto read_child;

            advance_grain_child:
            $grainIdx++;
            if ($grainIdx < $myGrainCount) goto next_grain_child;

            fclose($reader);
            file_put_contents($tmpDir . '/' . $workerIndex, chunk_split($countBuffer, 1, "\0"));
            exit(0);
        }
        if ($workerIndex < 7) {
            $workerIndex++;
            goto fork_worker;
        }

        $parentCounts = str_repeat("\0", $outputSize);
        $reader = fopen($inputPath, 'r');
        stream_set_read_buffer($reader, 0);
        $myGrains = $workerGrains[0];
        $myGrainCount = count($myGrains);
        $grainIdx = 0;

        next_grain_parent:
        fseek($reader, $myGrains[$grainIdx][0]);
        $remaining = $myGrains[$grainIdx][1] - $myGrains[$grainIdx][0];

        read_parent:
        $chunk = fread($reader, min($remaining, 163_840));
        $chunkLength = strlen($chunk);
        $remaining -= $chunkLength;

        $lastNewline = strrpos($chunk, "\n");
        if ($lastNewline === false) goto advance_grain_parent;

        $leftover = $chunkLength - $lastNewline - 1;
        if ($leftover > 0) {
            fseek($reader, -$leftover, SEEK_CUR);
            $remaining += $leftover;
        }

        $cursor = $lastNewline;

        if ($cursor <= $unrollFence) goto cleanup_parent;
        unrolled_parent:
        $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
        $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
        $parentCounts[$countIndex] = $nextChar[$parentCounts[$countIndex]];
        $cursor -= $packed >> 20;

        $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
        $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
        $parentCounts[$countIndex] = $nextChar[$parentCounts[$countIndex]];
        $cursor -= $packed >> 20;

        $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
        $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
        $parentCounts[$countIndex] = $nextChar[$parentCounts[$countIndex]];
        $cursor -= $packed >> 20;

        $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
        $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
        $parentCounts[$countIndex] = $nextChar[$parentCounts[$countIndex]];
        $cursor -= $packed >> 20;

        $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
        $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
        $parentCounts[$countIndex] = $nextChar[$parentCounts[$countIndex]];
        $cursor -= $packed >> 20;

        $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
        $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
        $parentCounts[$countIndex] = $nextChar[$parentCounts[$countIndex]];
        $cursor -= $packed >> 20;

        $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
        $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
        $parentCounts[$countIndex] = $nextChar[$parentCounts[$countIndex]];
        $cursor -= $packed >> 20;

        $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
        $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
        $parentCounts[$countIndex] = $nextChar[$parentCounts[$countIndex]];
        $cursor -= $packed >> 20;

        $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
        $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
        $parentCounts[$countIndex] = $nextChar[$parentCounts[$countIndex]];
        $cursor -= $packed >> 20;

        $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
        $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
        $parentCounts[$countIndex] = $nextChar[$parentCounts[$countIndex]];
        $cursor -= $packed >> 20;

        $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
        $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
        $parentCounts[$countIndex] = $nextChar[$parentCounts[$countIndex]];
        $cursor -= $packed >> 20;

        $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
        $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
        $parentCounts[$countIndex] = $nextChar[$parentCounts[$countIndex]];
        $cursor -= $packed >> 20;

        $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
        $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
        $parentCounts[$countIndex] = $nextChar[$parentCounts[$countIndex]];
        $cursor -= $packed >> 20;

        $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
        $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
        $parentCounts[$countIndex] = $nextChar[$parentCounts[$countIndex]];
        $cursor -= $packed >> 20;

        $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
        $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
        $parentCounts[$countIndex] = $nextChar[$parentCounts[$countIndex]];
        $cursor -= $packed >> 20;

        $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
        $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
        $parentCounts[$countIndex] = $nextChar[$parentCounts[$countIndex]];
        $cursor -= $packed >> 20;
        if ($cursor > $unrollFence) goto unrolled_parent;

        cleanup_parent:
        if ($cursor < $tailOffset) goto next_chunk_parent;
        $packed = $slugLookup[substr($chunk, $cursor - $tailOffset, $tailLength)];
        $countIndex = ($packed & $baseMask) + $dateStringToId[substr($chunk, $cursor - 22, 7)];
        $parentCounts[$countIndex] = $nextChar[$parentCounts[$countIndex]];
        $cursor -= $packed >> 20;
        if ($cursor >= $tailOffset) goto cleanup_parent;

        next_chunk_parent:
        if ($remaining > 0) goto read_parent;

        advance_grain_parent:
        $grainIdx++;
        if ($grainIdx < $myGrainCount) goto next_grain_parent;

        fclose($reader);

        while (pcntl_waitpid(-1, $status) > 0) {}

        $merged = chunk_split($parentCounts, 1, "\0");
        unset($parentCounts);

        $workerIndex = 1;
        merge_results:
        sodium_add($merged, file_get_contents($tmpDir . '/' . $workerIndex));
        unlink($tmpDir . '/' . $workerIndex);
        if ($workerIndex < 7) {
            $workerIndex++;
            goto merge_results;
        }
        @rmdir($tmpDir);

        $counts = unpack('v*', $merged);
        $outputHandle = fopen($outputPath, 'w');
        stream_set_write_buffer($outputHandle, 1_048_576);
        fwrite($outputHandle, '{');

        $datePrefixes = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "' . $dateIdToString[$d] . '": ';
        }

        $escapedPaths = [];
        for ($s = 0; $s < $slugCount; $s++) {
            $escapedPaths[$s] = '"\/blog\/' . str_replace('/', '\/', $slugPaths[$s]) . '": {';
        }

        $separator = "\n    ";
        $baseIndex = 1;

        for ($s = 0; $s < $slugCount; $s++) {
            $firstDate = -1;
            $countIndex = $baseIndex;
            for ($d = 0; $d < $dateCount; $d++) {
                if ($counts[$countIndex] !== 0) {
                    $firstDate = $d;
                    break;
                }
                $countIndex++;
            }

            if ($firstDate === -1) {
                $baseIndex += $dateCount;
                continue;
            }

            $buffer = $separator . $escapedPaths[$s] . "\n" . $datePrefixes[$firstDate] . $counts[$countIndex];
            $separator = ",\n    ";

            for ($d = $firstDate + 1; $d < $dateCount; $d++) {
                $countIndex++;
                $count = $counts[$countIndex];
                if ($count === 0) continue;
                $buffer .= ",\n" . $datePrefixes[$d] . $count;
            }

            $buffer .= "\n    }";
            fwrite($outputHandle, $buffer);
            $baseIndex += $dateCount;
        }

        fwrite($outputHandle, "\n}");
        fclose($outputHandle);
    }
}
