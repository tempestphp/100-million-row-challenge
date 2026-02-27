<?php

namespace App;

use App\Commands\Visit;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        // Pre-build slug mappings from known Visit URIs
        $chars = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';
        $slugToId = [];
        $idToSlug = [];
        $idToJsonHeader = [];
        foreach (Visit::all() as $i => $visit) {
            $slug = substr($visit->uri, 19);
            $id = $chars[(int)($i / 62)] . $chars[$i % 62];
            $slugToId[$slug] = $id;
            $idToSlug[$id] = str_replace('/', '\\/', $slug);
            $idToJsonHeader[$id] = '    "' . str_replace('/', '\\/', $slug) . '": {' . "\n";
        }

        // Pre-build date-to-index mapping
        $dateToIndex = [];
        $indexToDate = [];
        $day = strtotime('-5 years -2 months');
        $end = time();
        $nextDateIndex = 0;
        while ($day <= $end) {
            $date = date('Y-m-d', $day);
            $dateToIndex[$date] = $nextDateIndex;
            $indexToDate[$nextDateIndex] = $date;
            $nextDateIndex++;
            $day += 86400;
        }
        // Prefill all slug×date combinations with 0
        $emptyDates = array_fill(0, $nextDateIndex, 0);
        $results = [];
        foreach ($idToSlug as $slugId => $_) {
            $results[$slugId] = $emptyDates;
        }


        [$parentSocket, $childSocket] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);

        $pid = pcntl_fork();

        if ($pid === 0) {
            fclose($parentSocket);

            $file = fopen($inputPath, 'r');

            // Read file in large chunks, use fixed increment to skip between lines
            // Each line: https://stitcher.io/blog/slug,2022-09-10T13:55:25+00:00\n
            // Fixed parts: 26 chars prefix before slug area + 27 chars after comma = 53 increment
            $leftover = '';
            $increment = 26 + 27;

            while (($chunk = fread($file, 327680)) !== '' && $chunk !== false) {
                $chunk = $leftover . $chunk;
                $lastNewline = strrpos($chunk, "\n");

                if ($lastNewline === false) {
                    $leftover = $chunk;
                    continue;
                }

                $leftover = substr($chunk, $lastNewline + 1);
                $location = 26;
                $blob = '';

                while ($location < $lastNewline) {
                    $start = $location - 7;
                    $comma = strpos($chunk, ',', $location);
                    if ($comma === false || $comma > $lastNewline) break;

                    $slug = substr($chunk, $start, $comma - $start);
                    $date = substr($chunk, $comma + 1, 10);

                    $blob .= $date;
                    $blob .= $slugToId[$slug];
                    $location = $comma + $increment;
                }

                fwrite($childSocket, $blob);
            }

            fclose($childSocket);
            fclose($file);

            exit(0);
        }

        // Parent: read stream and process batches
        fclose($childSocket);


        $seenSlugs = [];
        $leftover = '';
        $seenSlugMaxCount = count(Visit::all());
        while (($data = stream_get_contents($parentSocket, (8192*4))) !== '' && $data !== false) {
            $data = $leftover . $data;

            // Each record is exactly 12 bytes: 10 byte date + 2 char slug id
            $len = strlen($data);
            $processable = $len - ($len % 12);
            $leftover = substr($data, $processable);

            if(count($seenSlugs) == $seenSlugMaxCount){
                for ($offset = 0; $offset < $processable; $offset += 12) {
                    $results[$data[$offset + 10] . $data[$offset + 11]][$dateToIndex[substr($data, $offset, 10)]]++;
                }
            }
            else {
                for ($offset = 0; $offset < $processable; $offset += 12) {
                    $id = $data[$offset + 10] . $data[$offset + 11];
                    $seenSlugs[$id] = true;
                    $results[$id][$dateToIndex[substr($data, $offset, 10)]]++;
                }
            }
        }

        $this->custom_json_put_content($outputPath, $results, $idToJsonHeader, $indexToDate, $seenSlugs);
    }
    protected function custom_json_put_content(string $outputPath, array &$results, array &$idToJsonHeader, array &$indexToDate, array &$seenSlugs): void
    {
        $fp = fopen($outputPath, 'w');
        fwrite($fp, "{\n");

        foreach ($seenSlugs as $slugId => $_) {
            $counts = $results[$slugId];

            $slugJson = '';
            foreach ($indexToDate as $dateIndex => $date) {
                if ($count = $counts[$dateIndex]) {
                    $slugJson .= "        \"{$date}\": {$count},\n";
                }
            }

            if ($slugJson === '') continue;

            fwrite($fp, $idToJsonHeader[$slugId]);
            $slugJson[strlen($slugJson)-2] = "\n";
            $slugJson[strlen($slugJson)-1] = " ";
            fwrite($fp, $slugJson . "   },\n");
        }

        fseek($fp, -2, SEEK_CUR);
        fwrite($fp, "\n}");
        fclose($fp);
    }



    protected function forked_custom_json_put_content(string $outputPath, array &$results, array &$idToSlug, array &$indexToDate, array &$seenSlugs, int $workerAmount = 4): void
    {
        $slugIds = array_keys($seenSlugs);
        $chunks = array_chunk($slugIds, (int)ceil(count($slugIds) / $workerAmount));

        $pids = [];
        $partFiles = [];

        foreach ($chunks as $i => $chunkSlugs) {
            $partFile = $outputPath . '_p' . $i;
            $partFiles[] = $partFile;

            $pid = pcntl_fork();

            if ($pid === 0) {
                $json = '';
                foreach ($chunkSlugs as $slugId) {
                    $counts = $results[$slugId];

                    $slugJson = '';
                    foreach ($indexToDate as $dateIndex => $date) {
                        if ($count = $counts[$dateIndex]) {
                            $slugJson .= "        \"{$date}\": {$count},\n";
                        }
                    }

                    if ($slugJson === '') continue;

                    $json .= '    "' . $idToSlug[$slugId] . '": {' . "\n";
                    $json .= $slugJson;
                    $json[strlen($json)-2] = "\n";
                    $json[strlen($json)-1] = " ";
                    $json .= "   },\n";
                }

                file_put_contents($partFile, $json);
                exit(0);
            }

            $pids[] = $pid;
        }

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        // Combine parts
        $fp = fopen($outputPath, 'w');
        fwrite($fp, "{\n");
        foreach ($partFiles as $partFile) {
            fwrite($fp, file_get_contents($partFile));
            unlink($partFile);
        }
        // Overwrite trailing comma+newline with closing brace
        fseek($fp, -2, SEEK_CUR);
        fwrite($fp, "\n}");
        fclose($fp);
    }

}
