<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        [$parentSocket, $childSocket] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);

        $pid = pcntl_fork();

        if ($pid === 0) {
            fclose($parentSocket);

            $file = fopen($inputPath, 'r');
            $slugToId = [];
            $idToSlug = [];
            $nextId = 0;

            // Read file in large chunks, use fixed increment to skip between lines
            // Each line: https://stitcher.io/blog/slug,2022-09-10T13:55:25+00:00\n
            // Fixed parts: 26 chars prefix before slug area + 27 chars after comma = 53 increment
            $leftover = '';
            $increment = 26 + 27;

            while (($chunk = fread($file, 32768)) !== '' && $chunk !== false) {
                $chunk = $leftover . $chunk;
                $lastNewline = strrpos($chunk, "\n");

                if ($lastNewline === false) {
                    $leftover = $chunk;
                    continue;
                }

                $leftover = substr($chunk, $lastNewline + 1);
                $location = 26;
                $pieces = [];

                while ($location < $lastNewline) {
                    $start = $location - 7;
                    $comma = strpos($chunk, ',', $location);
                    if ($comma === false || $comma > $lastNewline) break;

                    $slug = substr($chunk, $start, $comma - $start);
                    $date = substr($chunk, $comma + 1, 10);

                    if (!isset($slugToId[$slug])) {
                        $slugToId[$slug] = str_pad($nextId, 3, '0', STR_PAD_LEFT);
                        $idToSlug[$nextId] = $slug;
                        $nextId++;
                    }

                    $pieces[] = $date;
                    $pieces[] = $slugToId[$slug];
                    $location = $comma + $increment;
                }

                fwrite($childSocket, implode('', $pieces));
            }



            // Write slug mapping to temp file for parent to read (padded string keys to match parent's substr keys)
            $paddedMapping = [];
            foreach ($idToSlug as $id => $slug) {
                $paddedMapping[str_pad($id, 3, '0', STR_PAD_LEFT)] = $slug;
            }
            file_put_contents(__DIR__ . '/../data/slug-mapping.php', '<?php return ' . var_export($paddedMapping, true) . ';');

            fclose($childSocket);
            fclose($file);

            exit(0);
        }

        // Parent: read stream and process batches
        fclose($childSocket);

        $results = [];

        $leftover = '';
        while (($data = stream_get_contents($parentSocket, 8192)) !== '' && $data !== false) {
            $data = $leftover . $data;

            // Each record is exactly 13 bytes: 10 byte date + 3 digit slug id
            $len = strlen($data);
            $processable = $len - ($len % 13);
            $leftover = substr($data, $processable);

            for ($offset = 0; $offset < $processable; $offset += 13) {
                $date = substr($data, $offset, 10);
                $id = substr($data, $offset + 10, 3);
                $results[$id][$date] = ($results[$id][$date] ?? 0) + 1;
            }
        }


        foreach ($results as &$dates) {
            ksort($dates);
        }
        unset($dates);


        if(!file_exists(__DIR__ . '/../data/slug-mapping.php')) {
            pcntl_waitpid($pid, $status);
        }

        // Read mapping and replace numeric keys with actual slugs
        $mapping = require __DIR__ . '/../data/slug-mapping.php';

        $final = [];
        foreach ($results as $id => $dates) {
            $final[$mapping[$id]] = $dates;
        }

        file_put_contents($outputPath, json_encode($final, JSON_PRETTY_PRINT));
        unlink(__DIR__ . '/../data/slug-mapping.php');
    }
}
