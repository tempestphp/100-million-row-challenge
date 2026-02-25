<?php

declare(strict_types=1);

namespace App;

use PDO;
use function fclose;
use function fgets;
use function file_put_contents;
use function fopen;

final class Parser
{
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
                PRIMARY KEY(path, date)
            )
        ");

        $stmt = $db->prepare("
            INSERT INTO aggregated_counts (path, date, cnt)
            VALUES (
                replace(substr(?1, 20, instr(?1, ',') - 20), '/', '\/'), 
                substr(?1, instr(?1, ',') + 1, 10), 
                1
            )
            ON CONFLICT(path, date) DO UPDATE SET cnt = cnt + 1
        ");

        $handle = fopen($inputPath, 'rb');

        $db->beginTransaction();

        while (($line = fgets($handle)) !== false) {
            if ($line !== "\n" && $line !== '') {
                $stmt->execute([$line]);
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
    MIN(rowid) as first_seen,
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
