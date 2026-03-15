<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $fileSize = filesize($inputPath);

        // Detect year range from first line
        $fp = fopen($inputPath, "r");
        $firstLine = fgets($fp);
        fclose($fp);
        $cp = strpos($firstLine, ",", 25);
        $baseYear = (int) substr($firstLine, $cp + 1, 4);
        $startYear = $baseYear - 7;
        $endYear = $baseYear + 6;

        // Pre-compute date lookup: 8-char "YY-MM-DD" → int ID (chronological order)
        $dateIds = [];
        $datePrefixes = [];
        $dateCount = 0;

        for ($y = $startYear; $y <= $endYear; $y++) {
            $ys = str_pad((string) ($y % 100), 2, "0", STR_PAD_LEFT);

            for ($m = 1; $m <= 12; $m++) {
                $ms = $m < 10 ? "0$m" : (string) $m;
                $dim = match ($m) {
                    1, 3, 5, 7, 8, 10, 12 => 31,
                    4, 6, 9, 11 => 30,
                    2 => $y % 4 === 0 && ($y % 100 !== 0 || $y % 400 === 0)
                        ? 29
                        : 28,
                };
                for ($d = 1; $d <= $dim; $d++) {
                    $ds = $d < 10 ? "0$d" : (string) $d;
                    $dateIds["$ys-$ms-$ds"] = $dateCount;
                    $datePrefixes[$dateCount] = "        \"$y-$ms-$ds\": ";
                    $dateCount++;
                }
            }
        }

        $numWorkers = min(8, max(1, (int) ceil($fileSize / 262144)));

        if ($numWorkers <= 1) {
            $result = $this->processChunk(
                $inputPath,
                0,
                $fileSize,
                $dateIds,
                $dateCount,
            );
            $this->writeOutput($outputPath, $result, $datePrefixes, $dateCount);
            return;
        }

        $chunkSize = (int) ceil($fileSize / $numWorkers);
        $tmpPrefix = sys_get_temp_dir() . "/100m_" . getmypid() . "_";
        $pids = [];

        for ($w = 0; $w < $numWorkers - 1; $w++) {
            $pid = pcntl_fork();
            if ($pid === 0) {
                $r = $this->processChunk(
                    $inputPath,
                    $w * $chunkSize,
                    ($w + 1) * $chunkSize,
                    $dateIds,
                    $dateCount,
                );
                $this->writeWorkerResult($tmpPrefix . $w, $r, $dateCount);
                exit(0);
            }
            $pids[] = $pid;
        }

        $parentResult = $this->processChunk(
            $inputPath,
            ($numWorkers - 1) * $chunkSize,
            $fileSize,
            $dateIds,
            $dateCount,
        );

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        // Merge
        $maxPaths = 300;
        $globalIds = [];
        $globalOrder = [];
        $gpc = 0;
        $merged = array_fill(0, $maxPaths * $dateCount, 0);

        for ($w = 0; $w < $numWorkers - 1; $w++) {
            $this->mergeWorkerResult(
                $tmpPrefix . $w,
                $dateCount,
                $globalIds,
                $globalOrder,
                $gpc,
                $merged,
            );
        }

        // Merge parent result
        $pCounts = $parentResult["counts"];
        foreach ($parentResult["paths"] as $lp => $path) {
            if (!isset($globalIds[$path])) {
                $globalIds[$path] = $gpc;
                $globalOrder[] = $path;
                $gpc++;
            }
            $gBase = $globalIds[$path] * $dateCount;
            $lBase = $lp * $dateCount;
            for ($d = 0; $d < $dateCount; $d++) {
                $merged[$gBase + $d] += $pCounts[$lBase + $d];
            }
        }

        $this->writeOutput(
            $outputPath,
            ["paths" => $globalOrder, "counts" => $merged, "pc" => $gpc],
            $datePrefixes,
            $dateCount,
        );
    }

    private function processChunk(
        string $inputPath,
        int $startOffset,
        int $endOffset,
        array &$dateIds,
        int $dateCount,
    ): array {
        $fp = fopen($inputPath, "r");
        stream_set_read_buffer($fp, 0);

        if ($startOffset > 0) {
            fseek($fp, $startOffset - 1);
            if (fread($fp, 1) !== "\n") {
                fgets($fp);
            }
        }

        $pathIds = [];
        $pathOrder = [];
        $pathCount = 0;
        $counts = array_fill(0, 300 * $dateCount, 0);
        $remaining = "";

        while (($fpPos = ftell($fp)) < $endOffset) {
            $chunk = fread($fp, min(4194304, $endOffset - $fpPos));
            if ($chunk === false || $chunk === "") {
                break;
            }

            if ($remaining !== "") {
                $chunk = $remaining . $chunk;
                $remaining = "";
            }

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) {
                $remaining = $chunk;
                continue;
            }

            if ($lastNl < strlen($chunk) - 1) {
                $remaining = substr($chunk, $lastNl + 1);
            }

            $pos = 0;
            while ($pos < $lastNl) {
                $commaPos = strpos($chunk, ",", $pos + 25);
                if ($commaPos === false || $commaPos + 26 > $lastNl) {
                    break;
                }

                $path = substr($chunk, $pos + 25, $commaPos - $pos - 25);

                $base = $pathIds[$path] ?? -1;
                if ($base === -1) {
                    $base = $pathCount * $dateCount;
                    $pathIds[$path] = $base;
                    $pathOrder[] = $path;
                    $pathCount++;
                }

                $counts[$base + $dateIds[substr($chunk, $commaPos + 3, 8)]]++;

                $pos = $commaPos + 27;
            }
        }

        if ($remaining !== "") {
            $rest = fgets($fp);
            if ($rest !== false) {
                $remaining .= $rest;
            }
            $remaining = rtrim($remaining, "\n\r");
            if (strlen($remaining) > 25) {
                $commaPos = strpos($remaining, ",", 25);
                if ($commaPos !== false) {
                    $path = substr($remaining, 25, $commaPos - 25);
                    $base = $pathIds[$path] ?? -1;
                    if ($base === -1) {
                        $base = $pathCount * $dateCount;
                        $pathIds[$path] = $base;
                        $pathOrder[] = $path;
                        $pathCount++;
                    }
                    $counts[
                        $base + $dateIds[substr($remaining, $commaPos + 3, 8)]
                    ]++;
                }
            }
        }

        fclose($fp);
        return ["counts" => $counts, "paths" => $pathOrder, "pc" => $pathCount];
    }

    private function writeWorkerResult(
        string $file,
        array $result,
        int $dateCount,
    ): void {
        $pc = $result["pc"];
        $buf = pack("V", $pc);
        foreach ($result["paths"] as $p) {
            $buf .= pack("v", strlen($p)) . $p;
        }
        // Pack counts in chunks to avoid argument limit
        $total = $pc * $dateCount;
        $counts = $result["counts"];
        for ($i = 0; $i < $total; $i += 8192) {
            $buf .= pack(
                "V*",
                ...array_slice($counts, $i, min(8192, $total - $i)),
            );
        }
        file_put_contents($file, $buf);
    }

    private function mergeWorkerResult(
        string $file,
        int $dateCount,
        array &$globalIds,
        array &$globalOrder,
        int &$gpc,
        array &$merged,
    ): void {
        $raw = file_get_contents($file);
        unlink($file);

        $off = 0;
        $pc = unpack("V", $raw, $off)[1];
        $off += 4;

        $remap = [];
        for ($i = 0; $i < $pc; $i++) {
            $pl = unpack("v", $raw, $off)[1];
            $off += 2;
            $p = substr($raw, $off, $pl);
            $off += $pl;

            if (!isset($globalIds[$p])) {
                $globalIds[$p] = $gpc;
                $globalOrder[] = $p;
                $gpc++;
            }
            $remap[$i] = $globalIds[$p];
        }

        // Unpack and merge counts
        for ($lp = 0; $lp < $pc; $lp++) {
            $gBase = $remap[$lp] * $dateCount;
            $chunk = unpack("V" . $dateCount, $raw, $off);
            $off += $dateCount * 4;
            $idx = 1;
            for ($d = 0; $d < $dateCount; $d++) {
                $merged[$gBase + $d] += $chunk[$idx++];
            }
        }
    }

    private function writeOutput(
        string $outputPath,
        array $result,
        array &$datePrefixes,
        int $dateCount,
    ): void {
        $out = fopen($outputPath, "w");
        stream_set_write_buffer($out, 2 << 20);
        fwrite($out, "{\n");

        $counts = $result["counts"];
        $first = true;

        foreach ($result["paths"] as $pathIdx => $path) {
            $block = "";
            if (!$first) {
                $block = ",\n";
            }
            $first = false;

            $block .= '    "\/blog\/' . $path . '": {' . "\n";

            $base = $pathIdx * $dateCount;
            $firstDate = true;

            for ($d = 0; $d < $dateCount; $d++) {
                $c = $counts[$base + $d];
                if ($c > 0) {
                    if (!$firstDate) {
                        $block .= ",\n";
                    }
                    $firstDate = false;
                    $block .= $datePrefixes[$d] . $c;
                }
            }

            $block .= "\n    }";
            fwrite($out, $block);
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
