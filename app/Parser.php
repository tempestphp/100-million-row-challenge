<?php

declare(strict_types=1);

namespace App;

use PDO;
use function fclose;
use function file_put_contents;
use function filesize;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function getmypid;
use function pcntl_fork;
use function pcntl_waitpid;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function sys_get_temp_dir;
use function unlink;
use const SEEK_CUR;

final class Parser
{
    private const int READ_CHUNK_SIZE = 1_048_576 * 4; // 4MB chunks
    private const int WORKERS = 4;

    public function parse(string $inputPath, string $outputPath): void
    {
        $dbPath = '/dev/shm/parse_' . getmypid() . '.db';
        if (file_exists($dbPath)) {
            unlink($dbPath);
        }

        // 1. Create database in main thread
        $db = new PDO("sqlite:$dbPath");
        $db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        $db->exec("PRAGMA journal_mode = WAL;");
        $db->exec("PRAGMA synchronous = NORMAL;");
        $db->exec("PRAGMA busy_timeout = 60000;");

        // No temp tables! The main table holds exactly what the final SQL needs.
        $db->exec("
            CREATE TABLE IF NOT EXISTS aggregated_counts (
                path TEXT, 
                date TEXT, 
                cnt INTEGER, 
                first_seen INTEGER, 
                PRIMARY KEY(path, date)
            )
        ");

        $fileSize = filesize($inputPath);
        $boundaries = $this->calculateBoundaries($inputPath, $fileSize);

        $pids = [];

        // 2. Spawn 4 worker threads
        for ($i = 0; $i < self::WORKERS - 1; $i++) {
            $pid = pcntl_fork();

            if ($pid === 0) {
                // Worker executes its chunk
                $this->parseRange(
                    $inputPath,
                    $dbPath,
                    $boundaries[$i],
                    $boundaries[$i + 1]
                );
                exit(0);
            }

            if ($pid < 0) {
                throw new \RuntimeException('Unable to fork parser worker');
            }

            $pids[$i] = $pid;
        }

        // Main thread acts as the 4th worker
        $this->parseRange(
            $inputPath,
            $dbPath,
            $boundaries[self::WORKERS - 1],
            $boundaries[self::WORKERS]
        );

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        // 3. Final SQL statement (no substr or instr)
        $this->writeOutput($db, $outputPath);

        // Cleanup
        unset($db);
        if (file_exists($dbPath)) unlink($dbPath);
        if (file_exists($dbPath . '-wal')) unlink($dbPath . '-wal');
        if (file_exists($dbPath . '-shm')) unlink($dbPath . '-shm');
    }

    private function calculateBoundaries(string $inputPath, int $fileSize): array
    {
        $chunkSize = (int) ($fileSize / self::WORKERS);
        $boundaries = [0];

        $handle = fopen($inputPath, 'rb');

        for ($i = 1; $i < self::WORKERS; $i++) {
            fseek($handle, $i * $chunkSize);
            fgets($handle);
            $boundaries[] = ftell($handle);
        }

        fclose($handle);
        $boundaries[] = $fileSize;

        return $boundaries;
    }

    private function parseRange(
        string $inputPath,
        string $dbPath,
        int $start,
        int $end
    ): void {
        $db = new PDO("sqlite:$dbPath");
        $db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $db->exec("PRAGMA busy_timeout = 60000;");

        // The single prepared statement that directly updates aggregated_counts
        $stmt = $db->prepare("
            INSERT INTO aggregated_counts (path, date, cnt, first_seen)
            VALUES (?, ?, 1, ?)
            ON CONFLICT(path, date) DO UPDATE SET 
              cnt = cnt + 1,
              first_seen = MIN(first_seen, excluded.first_seen)
        ");

        $handle = fopen($inputPath, 'rb');
        fseek($handle, $start);

        $remaining = $end - $start;
        $offset = $start;

        while ($remaining > 0) {
            $readSize = $remaining > self::READ_CHUNK_SIZE ? self::READ_CHUNK_SIZE : $remaining;
            $chunk = fread($handle, $readSize);
            $chunkLen = strlen($chunk);
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");

            if ($lastNl !== false && $lastNl < ($chunkLen - 1)) {
                $excess = $chunkLen - $lastNl - 1;
                fseek($handle, -$excess, SEEK_CUR);
                $remaining += $excess;
                $chunkLen = $lastNl + 1;
                $chunk = substr($chunk, 0, $chunkLen);
            }

            if ($chunk !== '') {
                $db->beginTransaction();
                
                $pos = 0;
                while ($pos < $chunkLen) {
                    $nlPos = strpos($chunk, "\n", $pos);
                    if ($nlPos === false) {
                        break;
                    }

                    // Trim off "https://stitcher.io" (19) and ",202x..." (26)
                    // Then escape paths directly in PHP
                    $pathLen = $nlPos - $pos - 45;
                    if ($pathLen > 0) {
                        $path = str_replace('/', '\\/', substr($chunk, $pos + 19, $pathLen));
                        $date = substr($chunk, $nlPos - 25, 10);

                        $stmt->execute([$path, $date, $offset]);
                    }
                    
                    $offset++;
                    $pos = $nlPos + 1;
                }

                $db->commit();
            }
        }

        fclose($handle);
    }

    private function writeOutput(PDO $db, string $outputPath): void
    {
        // One final SQL statement to build the JSON. 
        // No substr, no instr, no temp tables.
        $outputSql = <<<SQL
SELECT '{
' || group_concat(
    '    "' || ac.path || '": {
' || dates_json || '
    }',
    ',
' ORDER BY first_seen ASC
) || '
}' as result
FROM (
  SELECT
    path,
    MIN(first_seen) as first_seen,
    group_concat(
      '        "' || date || '": ' || cnt,
      ',
' ORDER BY date ASC
    ) as dates_json
  FROM aggregated_counts
  GROUP BY path
) as ac;
SQL;

        $result = $db->query($outputSql)->fetchColumn();
        file_put_contents($outputPath, $result);
    }
}
