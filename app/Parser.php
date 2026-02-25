<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $handle = fopen($inputPath, 'r');
        if (!$handle) {
            throw new \RuntimeException('Cannot open file: ' . $inputPath);
        }

        // Increase read buffer and chunk size for better I/O performance with large files
        // Use 16MB buffers for 100M rows
        stream_set_read_buffer($handle, 16 * 1024 * 1024);
        stream_set_chunk_size($handle, 16 * 1024 * 1024);

        // Path ID mapping to avoid duplicate string storage
        $pathToId = [];
        $idToPath = [];
        $nextPathId = 0;
        
        // Data structure: pathId => [dateInt => count]
        $data = [];
        $domainLength = null; // Will be computed from first line
        
        while (($line = fgets($handle)) !== false) {
            // Optimized string processing:
            // 1. Use rtrim instead of substr for newline removal
            // 2. Use explode instead of strpos+substr for splitting
            
            $line = rtrim($line, "\n\r");
            if ($line === '') {
                continue;
            }

            // Find comma using strpos (still needed for performance)
            // But we can combine with substr for single pass
            $commaPos = strpos($line, ',');
            if ($commaPos === false) {
                continue;
            }

            // Compute domain length if not yet known
            if ($domainLength === null) {
                // Find the position of the third slash after ://
                $schemeEnd = strpos($line, '://');
                if ($schemeEnd === false) {
                    continue; // malformed line
                }
                $pathStart = strpos($line, '/', $schemeEnd + 3);
                if ($pathStart === false) {
                    continue; // malformed line
                }
                $domainLength = $pathStart;
            }
            
            // Extract path - skip domain
            $path = substr($line, $domainLength, $commaPos - $domainLength);
            
            // Extract date and convert to integer YYYYMMDD for memory efficiency
            // Date starts at $commaPos+1, format: YYYY-MM-DD
            // Convert to integer: remove hyphens using direct character access
            // Positions: YYYY-MM-DD
            // 0 1 2 3 4 5 6 7 8 9 (relative to $commaPos+1)
            $dateInt = (int) ($line[$commaPos+1] . $line[$commaPos+2] . $line[$commaPos+3] . $line[$commaPos+4]
                            . $line[$commaPos+6] . $line[$commaPos+7] . $line[$commaPos+9] . $line[$commaPos+10]);

            // Get or create path ID
            if (!isset($pathToId[$path])) {
                $pathId = $nextPathId++;
                $pathToId[$path] = $pathId;
                $idToPath[$pathId] = $path;
                $data[$pathId] = [];
            } else {
                $pathId = $pathToId[$path];
            }
            
            // Increment count - use isset for performance
            if (isset($data[$pathId][$dateInt])) {
                $data[$pathId][$dateInt]++;
            } else {
                $data[$pathId][$dateInt] = 1;
            }
        }

        fclose($handle);

        // Write JSON incrementally to avoid large string in memory
        $this->writeIncrementalJsonWithPathMapping($data, $idToPath, $outputPath);
    }
    
    /**
     * Write JSON output incrementally to avoid large string in memory
     */
    private function writeIncrementalJson(array $data, string $outputPath): void
    {
        $fp = fopen($outputPath, 'w');
        if (!$fp) {
            throw new \RuntimeException('Cannot open output file: ' . $outputPath);
        }
        
        fwrite($fp, "{\n");
        
        $firstPath = true;
        foreach ($data as $path => $dates) {
            ksort($dates);
            
            if (!$firstPath) {
                fwrite($fp, ",\n");
            }
            $firstPath = false;
            
            // Write path key
            fwrite($fp, '    "' . $this->escapeJsonString($path) . '": {' . "\n");
            
            $firstDate = true;
            foreach ($dates as $dateInt => $count) {
                if (!$firstDate) {
                    fwrite($fp, ",\n");
                }
                $firstDate = false;
                
                // Convert integer date back to string format
                $dateStr = (string) $dateInt;
                $formattedDate = substr($dateStr, 0, 4) . '-' .
                                 substr($dateStr, 4, 2) . '-' .
                                 substr($dateStr, 6, 2);
                
                fwrite($fp, '        "' . $formattedDate . '": ' . $count);
            }
            
            fwrite($fp, "\n    }");
        }
        
        fwrite($fp, "\n}");
        fclose($fp);
    }
    
    /**
     * Write JSON output with path ID mapping using buffered writes
     */
    private function writeIncrementalJsonWithPathMapping(array $data, array $idToPath, string $outputPath): void
    {
        $fp = fopen($outputPath, 'w');
        if (!$fp) {
            throw new \RuntimeException('Cannot open output file: ' . $outputPath);
        }
        
        // Buffered writing to reduce system call overhead
        $buffer = '';
        $bufferSize = 0;
        $maxBufferSize = 1024 * 1024; // 1MB
        
        $flushBuffer = function() use (&$buffer, &$bufferSize, $fp) {
            if ($bufferSize > 0) {
                fwrite($fp, $buffer);
                $buffer = '';
                $bufferSize = 0;
            }
        };
        
        $write = function(string $data) use (&$buffer, &$bufferSize, $flushBuffer, $maxBufferSize) {
            $buffer .= $data;
            $bufferSize += strlen($data);
            if ($bufferSize >= $maxBufferSize) {
                $flushBuffer();
            }
        };
        
        $write("{\n");
        
        $firstPath = true;
        // Iterate through path IDs in order (0, 1, 2, ...)
        for ($pathId = 0; $pathId < count($idToPath); $pathId++) {
            $path = $idToPath[$pathId];
            $dates = $data[$pathId];
            ksort($dates);
            
            if (!$firstPath) {
                $write(",\n");
            }
            $firstPath = false;
            
            // Write path key
            $write('    "' . $this->escapeJsonString($path) . '": {' . "\n");
            
            $firstDate = true;
            foreach ($dates as $dateInt => $count) {
                if (!$firstDate) {
                    $write(",\n");
                }
                $firstDate = false;
                
                // Convert integer date back to string format
                $dateStr = (string) $dateInt;
                $formattedDate = substr($dateStr, 0, 4) . '-' .
                                 substr($dateStr, 4, 2) . '-' .
                                 substr($dateStr, 6, 2);
                
                $write('        "' . $formattedDate . '": ' . $count);
            }
            
            $write("\n    }");
        }
        
        $write("\n}");
        
        // Flush any remaining buffer
        $flushBuffer();
        
        fclose($fp);
    }
    
    /**
     * Escape string for JSON using json_encode for proper escaping
     */
    private function escapeJsonString(string $str): string
    {
        // Use json_encode for proper JSON escaping, then strip surrounding quotes
        $json = json_encode($str, JSON_UNESCAPED_UNICODE);
        // json_encode returns quoted string, remove the quotes
        return substr($json, 1, -1);
    }
}
