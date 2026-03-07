<?php

namespace App\Traits;

trait LoaderChunkedFileV1Trait {
    /**
     * Here we are using forked processes to split the file into chunks and process them in parallel
     */
    protected function load(): array
    {
        // Hoist class variables
        $inputPath = $this->inputPath;

        echo "Starting chunk loader";

        $fileSize = filesize($inputPath);
        $chunkSize = (int) ceil($fileSize / self::WORKER_COUNT);
        $chunks = $this->calculateChunkBoundaries($chunkSize, $fileSize);

        $tmpDir = sys_get_temp_dir();
        $tmpFiles = [];

        $pids = [];
        $start = 0;

        foreach($chunks as $index => $chunk)
        {
            $tmpFile = $tmpDir . '/chunk_' . $index;
            $tmpFiles[] = $tmpFile;

            $pid = pcntl_fork();

            if ($pid === -1) {
                throw new \RuntimeException("Unable to fork");
            }

            // Child process
            if ($pid === 0) {
                $startProcessing = microtime(true);
                $result = $this->processChunk2($chunk['start'], $chunk['end'], $index);
                echo "[Child#$index] Processing: " . (microtime(true) - $startProcessing) . " seconds" . PHP_EOL;
                $startWriting = microtime(true);
                file_put_contents($tmpFile, serialize($result));
                echo "[Child#$index] Writing: " . (microtime(true) - $startWriting) . " seconds" . PHP_EOL;
                exit(0);
            }

            $pids[$index] = $pid;
        }

        echo "Waiting for children:".implode(', ', $pids).PHP_EOL;

        // Wait for child processes to finish
        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        // Merge results together
        $merged = [];

        $startMerge = microtime(true);
        for ($i = 0; $i < count($chunks); $i++) {
            $chunkResult = unserialize(file_get_contents($tmpFiles[$i]));
            unlink($tmpFiles[$i]);

            $merged = $this->mergeResults($merged, $chunkResult);
        }
        unset($merged[""]);
        echo "Merge time: " . (microtime(true) - $startMerge) . " seconds" . PHP_EOL;

        return $merged;
    }

    private function calculateChunkBoundaries(int $chunkSize, int $fileSize): array
    {
        $chunks = [];
        $start = 0;

        for ($i = 0; $i < self::WORKER_COUNT; $i++) {
            $end = min($start + $chunkSize, $fileSize);
            $chunks[] = [
                'start' => $start,
                'end' => $end
            ];
            $start = $end;
        }

        return $chunks;
    }

    private function processChunk(int $start, int $end, int $index): array
    {
        $handle = fopen($this->inputPath, 'rb', false);

        // Seeing some slight benefit from setting this to the total area on disk we're interested in
        // In theory this should let the stream reader keep it fresh in memory and not have to goto disk
        // In practice it seems to slightly reduce the variations between runs trending slightly faster
        stream_set_read_buffer($handle, $end - $start);

        $results = [];

        // For loaders which started with an offset > 0 we need to find the start of the next line
        // we drop this partial line off the front, and the preceding loader will pick it up at the
        // end of their chunk. This helps us simplify the chunk boundaries calculation.
        if ($start > 0) {
            fseek($handle, $start);
            $junkLine = stream_get_line($handle, self::STREAM_BUFFER_SIZE, "\n");
            $start += strlen($junkLine);
        }

        $i = 0;

        while (true) {
            $urlString = stream_get_line($handle, self::STREAM_BUFFER_SIZE, ',');
            $dateString = stream_get_line($handle, self::STREAM_BUFFER_SIZE, "\n");

            $url = substr($urlString, self::DOMAIN_LENGTH);
            $date = substr($dateString, 0, self::DATE_LENGTH);

            $results[$url][$date] = ($results[$url][$date] ?? 0) + 1;

            $i++;

            // Ensure our new position is within our chunk otherwise break out
            $start += strlen($urlString) + strlen($dateString) + 2;
            if ($start > $end) {
                break;
            }
        }

        echo "[Child#$index] Processed $i lines".PHP_EOL;

        fclose($handle);

        return $results;
    }

    /**
     * This implementation aims to reduce number of stream_get_line calls by pulling it all into memory
     * and parsing it with string functions.
     */
    private function processChunk2(int $start, int $end, int $index): array
    {
        $handle = fopen($this->inputPath, 'rb', false);
        stream_set_read_buffer($handle, $end - $start + 100);
        $results = [];

        // Load the entire chunk into memory + padding for remainder of last line
        $chunk = stream_get_contents($handle, ($end - $start) + 100, $start);

        // Find the end of the final row even if it spans into the next chunk (they will have discarded it)
        $chunkEnd = strpos($chunk, "\n", ($end - $start));
        if ($chunkEnd === false) {
            $chunkEnd = $end-$start;
        }

        // Create a sliding window over the loaded chunk in memory
        $wStart = 0;
        $i = 0;

        // If we have a starting point > 0 then we can discard the partial first row
        if ($start > 0) {
            $wStart = strpos($chunk, "\n")+1;
            $discarded = substr($chunk, 0, $wStart);
            echo "[Child#$index] Discarded: " . $discarded.PHP_EOL;
        }

        echo "[Child#$index] Loaded chunk: " . strlen($chunk) . " bytes".PHP_EOL;
        echo "[Child#$index] Starting at $wStart to $chunkEnd".PHP_EOL;

        while ($wStart < $chunkEnd) {
            // Bump the offset by a min width of line to reduce the search area for the newline
            $wEnd = strpos($chunk, "\n", $wStart+self::MIN_LINE_WIDTH);

            // We push the start position forward to skip the domain name
            // We pull the length from the end of the window minus the fixed
            // width date param and comma so we don't have to search for the comma
            $url = substr(
                $chunk,
                $wStart + self::DOMAIN_LENGTH,
                ($wEnd - $wStart - self::DOMAIN_LENGTH) - (self::DATE_WIDTH + 1)
            );

            // Date width is the full field length, whereas date length is the width
            // of the field we're interested in
            $date = substr(
                $chunk,
                $wEnd - self::DATE_WIDTH,
                self::DATE_LENGTH,
            );

            $results[$url][$date] = ($results[$url][$date] ?? 0) + 1;

            // Update the window position
            $wStart = $wEnd + 1;
            $i++;
        }

        echo "[Child#$index] Processed $i lines".PHP_EOL;

        fclose($handle);

        return $results;
    }

    private function mergeResults(array $base, array $incoming): array
    {
        foreach ($incoming as $url => $dates) {
            foreach ($dates as $date => $count) {
                $base[$url][$date] = ($base[$url][$date] ?? 0) + $count;
            }
        }

        return $base;
    }
}