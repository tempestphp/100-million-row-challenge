<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $fileSize = filesize($inputPath);

        $handle = fopen($inputPath, 'rb');
        $sampleSize = min($fileSize, 8388608);
        $sample = fread($handle, $sampleSize);
        fclose($handle);

        $pathIds = [];
        $pathList = [];
        $pathCount = 0;
        $pos = 0;
        while (($nlPos = strpos($sample, "\n", $pos)) !== false) {
            if ($nlPos - $pos >= 46) {
                $path = substr($sample, $pos + 19, $nlPos - $pos - 45);
                if (!isset($pathIds[$path])) {
                    $pathIds[$path] = $pathCount;
                    $pathList[$pathCount] = $path;
                    $pathCount++;
                }
            }
            $pos = $nlPos + 1;
        }
        unset($sample);

        $dateIds = [];
        $dateList = [];
        $dateCount = 0;
        for ($year = 20; $year <= 26; $year++) {
            for ($month = 1; $month <= 12; $month++) {
                $maxDay = ($month === 2)
                    ? (($year % 4 === 0) ? 29 : 28)
                    : (($month === 4 || $month === 6 || $month === 9 || $month === 11) ? 30 : 31);
                for ($day = 1; $day <= $maxDay; $day++) {
                    $key = sprintf('%02d-%02d-%02d', $year, $month, $day);
                    $dateIds[$key] = $dateCount;
                    $dateList[$dateCount] = '20' . $key;
                    $dateCount++;
                }
            }
        }

        $stride = $dateCount;
        $totalCells = $pathCount * $stride;

        $numWorkers = 8;
        $handle = fopen($inputPath, 'rb');
        $splits = [0];
        for ($s = 1; $s < $numWorkers; $s++) {
            fseek($handle, (int)($fileSize * $s / $numWorkers));
            fgets($handle);
            $splits[] = ftell($handle);
        }
        $splits[] = $fileSize;
        fclose($handle);

        $tmpDir = sys_get_temp_dir();
        $tmpPrefix = $tmpDir . '/p_' . getmypid() . '_';
        $readSize = 262144;

        $childPids = [];
        for ($w = 1; $w < $numWorkers; $w++) {
            $pid = pcntl_fork();
            if ($pid === 0) {
                $counts = $this->processChunk(
                    $inputPath, $splits[$w], $splits[$w + 1],
                    $pathIds, $dateIds, $stride, $totalCells, $readSize
                );
                file_put_contents($tmpPrefix . $w, pack('V*', ...$counts));
                exit(0);
            }
            $childPids[$w] = $pid;
        }

        $counts = $this->processChunk(
            $inputPath, $splits[0], $splits[1],
            $pathIds, $dateIds, $stride, $totalCells, $readSize
        );

        $escapedPaths = [];
        for ($p = 0; $p < $pathCount; $p++) {
            $escapedPaths[$p] = "\n    \"" . str_replace('/', '\\/', $pathList[$p]) . "\": {";
        }
        $datePrefixes = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = "\n        \"" . $dateList[$d] . "\": ";
        }

        $remaining = count($childPids);
        while ($remaining > 0) {
            $pid = pcntl_wait($status);
            $w = array_search($pid, $childPids);
            if ($w === false) continue;
            $raw = file_get_contents($tmpPrefix . $w);
            @unlink($tmpPrefix . $w);
            $j = 0;
            foreach (unpack('V*', $raw) as $v) {
                $counts[$j] += $v;
                $j++;
            }
            $remaining--;
        }

        $fp = fopen($outputPath, 'wb');
        stream_set_write_buffer($fp, 1048576);
        $buf = '{';
        $firstPath = true;

        for ($p = 0; $p < $pathCount; $p++) {
            $offset = $p * $stride;
            $firstD = -1;
            for ($d = 0; $d < $dateCount; $d++) {
                if ($counts[$offset + $d] > 0) {
                    $firstD = $d;
                    break;
                }
            }
            if ($firstD === -1) continue;

            if (!$firstPath) $buf .= ',';
            $firstPath = false;
            $buf .= $escapedPaths[$p];
            $buf .= $datePrefixes[$firstD] . $counts[$offset + $firstD];
            for ($d = $firstD + 1; $d < $dateCount; $d++) {
                if ($counts[$offset + $d] === 0) continue;
                $buf .= ',' . $datePrefixes[$d] . $counts[$offset + $d];
            }
            $buf .= "\n    }";

            if (strlen($buf) > 65536) {
                fwrite($fp, $buf);
                $buf = '';
            }
        }
        $buf .= "\n}";
        fwrite($fp, $buf);
        fclose($fp);
    }

    private function processChunk(
        string $inputPath, int $start, int $end,
        array $pathIds, array $dateIds, int $stride, int $totalCells, int $readSize
    ): array {
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);

        $counts = array_fill(0, $totalCells, 0);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $toRead = $remaining < $readSize ? $remaining : $readSize;
            $chunk = fread($handle, $toRead);
            if ($chunk === false || $chunk === '')
                break;
            $chunkLen = strlen($chunk);
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) continue;

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $pos = 0;
            while ($pos < $lastNl) {
                $nlPos = strpos($chunk, "\n", $pos + 46);
                if ($nlPos === false || $nlPos > $lastNl) break;

                $pathStr = substr($chunk, $pos + 19, $nlPos - $pos - 45);
                $dateStr = substr($chunk, $nlPos - 23, 8);

                if (isset($pathIds[$pathStr])) {
                    $counts[$pathIds[$pathStr] * $stride + $dateIds[$dateStr]]++;
                }

                $pos = $nlPos + 1;
            }
        }

        fclose($handle);
        return $counts;
    }
}
