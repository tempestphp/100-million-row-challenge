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

    public function parse(string $inputPath, string $outputPath): void
    {
        $db = new PDO('sqlite::memory:');
        $db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        $db->exec("PRAGMA synchronous = OFF;");
        $db->exec("PRAGMA journal_mode = MEMORY;");

        $db->exec("
            CREATE TABLE IF NOT EXISTS aggregated_counts (
                path TEXT, 
                date TEXT, 
                cnt INTEGER, 
                first_seen INTEGER, 
                PRIMARY KEY(path, date)
            ) WITHOUT ROWID
        ");

        $stmt = $db->prepare("
            INSERT INTO aggregated_counts (path, date, cnt, first_seen)
            VALUES (
                replace(substr(:line, 20, instr(:line, ',') - 20), '/', '\/'), 
                substr(:line, instr(:line, ',') + 1, 10), 
                1, 
                :offset
            )
            ON CONFLICT(path, date) DO UPDATE SET 
              cnt = cnt + 1,
              first_seen = MIN(first_seen, excluded.first_seen)
        ");

        $handle = fopen($inputPath, 'rb');
        $offset = 0;

        $db->beginTransaction();

        while (!feof($handle)) {
            $chunk = fread($handle, self::READ_CHUNK_SIZE);
            if ($chunk === false || $chunk === '') {
                break;
            }

            $chunkLen = strlen($chunk);

            $lastNl = strrpos($chunk, "
");

            if ($lastNl !== false && $lastNl < ($chunkLen - 1)) {
                $excess = $chunkLen - $lastNl - 1;
                fseek($handle, -$excess, SEEK_CUR);
                $chunkLen = $lastNl + 1;
                $chunk = substr($chunk, 0, $chunkLen);
            }

            $pos = 0;
            while ($pos < $chunkLen) {
                $nlPos = strpos($chunk, "
", $pos);
                if ($nlPos === false) {
                    break;
                }

                $line = substr($chunk, $pos, $nlPos - $pos);
                if ($line !== '') {
                    $stmt->execute([':line' => $line, ':offset' => $offset]);
                }
                
                $offset++;
                $pos = $nlPos + 1;
            }
        }

        $db->commit();
        fclose($handle);

        $this->writeOutput($db, $outputPath);
    }

    private function writeOutput(PDO $db, string $outputPath): void
    {
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
