<?php

namespace App;

final class Parser
{
    private const FALLBACK_SQL_VARIABLE_LIMIT = 999;
    private const STITCHER_PATH_OFFSET = 19;
    private const VISIT_INSERT_PARAM_COUNT = 3;
    private const READ_CHUNK_BYTES = 8 * 1024 * 1024;

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $db = new \PDO('sqlite::memory:');
        $db->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        $db->setAttribute(\PDO::ATTR_EMULATE_PREPARES, false);

        $sqlVariableLimit = $this->detectSqlVariableLimit($db);
        $maxVisitInsertBatchRows = max(1, intdiv($sqlVariableLimit, self::VISIT_INSERT_PARAM_COUNT));
        $configuredBatchRows = $this->readPositiveIntEnv('PARSER_INSERT_BATCH_ROWS', PHP_INT_MAX);
        $visitInsertBatchRows = min($configuredBatchRows, $maxVisitInsertBatchRows);

        $db->exec('PRAGMA journal_mode = OFF;');
        $db->exec('PRAGMA synchronous = OFF;');
        $db->exec('PRAGMA temp_store = MEMORY;');
        $db->exec('PRAGMA locking_mode = EXCLUSIVE;');
        $db->exec('PRAGMA automatic_index = OFF;');
        $db->exec('PRAGMA cache_size = -400000;');
        $db->exec('PRAGMA foreign_keys = OFF;');

        $db->exec('
            CREATE TABLE visits (
                path TEXT NOT NULL,
                visit_date INTEGER NOT NULL,
                count INTEGER NOT NULL,
                PRIMARY KEY (path, visit_date)
            ) WITHOUT ROWID
        ');

        $handle = fopen($inputPath, 'rb');

        $pathOrder = [];
        $counts = [];
        $visitInsertStatementCache = [];

        try {
            $db->beginTransaction();

            $carry = '';
            while (! feof($handle)) {
                $chunk = stream_get_contents($handle, self::READ_CHUNK_BYTES);
                if ($chunk === false) {
                    throw new \RuntimeException("Failed to read input file: {$inputPath}");
                }
                if ($chunk === '') {
                    break;
                }

                $buffer = $carry . $chunk;
                $lineStart = 0;

                while (true) {
                    $newlinePos = strpos($buffer, "\n", $lineStart);
                    if ($newlinePos === false) {
                        break;
                    }

                    $commaPos = $newlinePos - 26;
                    $pathStart = $lineStart + self::STITCHER_PATH_OFFSET;
                    $path = substr($buffer, $pathStart, $commaPos - $pathStart);

                    $dateStart = $newlinePos - 25;
                    $year =
                        ((ord($buffer[$dateStart]) - 48) * 1000) +
                        ((ord($buffer[$dateStart + 1]) - 48) * 100) +
                        ((ord($buffer[$dateStart + 2]) - 48) * 10) +
                        (ord($buffer[$dateStart + 3]) - 48);

                    $month =
                        ((ord($buffer[$dateStart + 5]) - 48) * 10) +
                        (ord($buffer[$dateStart + 6]) - 48);

                    $day =
                        ((ord($buffer[$dateStart + 8]) - 48) * 10) +
                        (ord($buffer[$dateStart + 9]) - 48);

                    $visitDate = ($year * 10000) + ($month * 100) + $day;
                    if (! isset($counts[$path])) {
                        $counts[$path] = [$visitDate => 1];
                        $pathOrder[] = $path;
                    } elseif (isset($counts[$path][$visitDate])) {
                        $counts[$path][$visitDate]++;
                    } else {
                        $counts[$path][$visitDate] = 1;
                    }
                    $lineStart = $newlinePos + 1;
                }

                $carry = substr($buffer, $lineStart);
            }

            if ($carry !== '') {
                $carryLength = strlen($carry);
                $commaPos = $carryLength - 26;
                $pathStart = self::STITCHER_PATH_OFFSET;
                $path = substr($carry, $pathStart, $commaPos - $pathStart);

                $dateStart = $carryLength - 25;
                $year =
                    ((ord($carry[$dateStart]) - 48) * 1000) +
                    ((ord($carry[$dateStart + 1]) - 48) * 100) +
                    ((ord($carry[$dateStart + 2]) - 48) * 10) +
                    (ord($carry[$dateStart + 3]) - 48);

                $month =
                    ((ord($carry[$dateStart + 5]) - 48) * 10) +
                    (ord($carry[$dateStart + 6]) - 48);

                $day =
                    ((ord($carry[$dateStart + 8]) - 48) * 10) +
                    (ord($carry[$dateStart + 9]) - 48);

                $visitDate = ($year * 10000) + ($month * 100) + $day;
                if (! isset($counts[$path])) {
                    $counts[$path] = [$visitDate => 1];
                    $pathOrder[] = $path;
                } elseif (isset($counts[$path][$visitDate])) {
                    $counts[$path][$visitDate]++;
                } else {
                    $counts[$path][$visitDate] = 1;
                }
            }

            $this->insertVisitsInBatches($db, $counts, $visitInsertBatchRows, $visitInsertStatementCache);
            $db->commit();
        } catch (\Throwable $e) {
            if ($db->inTransaction()) {
                $db->rollBack();
            }

            throw $e;
        } finally {
            fclose($handle);
        }

        $this->writeOutputStream($db, $outputPath, $pathOrder);
    }

    private function insertVisitsInBatches(
        \PDO $db,
        array $counts,
        int $insertBatchRows,
        array &$statementCache
    ): void
    {
        if ($counts === []) {
            return;
        }

        $batchRows = 0;
        $batchParams = [];

        foreach ($counts as $path => $dates) {
            foreach ($dates as $visitDate => $count) {
                $batchParams[] = $path;
                $batchParams[] = $visitDate;
                $batchParams[] = $count;
                $batchRows++;

                if ($batchRows === $insertBatchRows) {
                    $this->executeVisitInsertBatch($db, $statementCache, $batchRows, $batchParams);
                    $batchRows = 0;
                    $batchParams = [];
                }
            }
        }

        if ($batchRows > 0) {
            $this->executeVisitInsertBatch($db, $statementCache, $batchRows, $batchParams);
        }
    }

    private function executeVisitInsertBatch(
        \PDO $db,
        array &$statementCache,
        int $rowCount,
        array $params
    ): void
    {
        if (! isset($statementCache[$rowCount])) {
            $values = implode(', ', array_fill(0, $rowCount, '(?, ?, ?)'));
            $statementCache[$rowCount] = $db->prepare(
                "INSERT INTO visits (path, visit_date, count) VALUES {$values}"
            );
        }

        $statementCache[$rowCount]->execute($params);
    }

    private function readPositiveIntEnv(string $name, int $default): int
    {
        $value = getenv($name);
        if ($value === false || $value === '') {
            return $default;
        }

        $parsed = (int) $value;

        return $parsed > 0 ? $parsed : $default;
    }

    private function detectSqlVariableLimit(\PDO $db): int
    {
        $pragmaLimit = $this->readPragmaMaxVariableNumber($db);
        if ($pragmaLimit !== null && $pragmaLimit > 0) {
            return $pragmaLimit;
        }

        $compileOptionLimit = $this->readCompileOptionMaxVariableNumber($db);
        if ($compileOptionLimit !== null && $compileOptionLimit > 0) {
            return $compileOptionLimit;
        }

        return self::FALLBACK_SQL_VARIABLE_LIMIT;
    }

    private function readPragmaMaxVariableNumber(\PDO $db): ?int
    {
        try {
            $result = $db->query('PRAGMA max_variable_number');
            if ($result === false) {
                return null;
            }

            $row = $result->fetch(\PDO::FETCH_NUM);
            if (! is_array($row) || ! isset($row[0])) {
                return null;
            }

            $limit = (int) $row[0];

            return $limit > 0 ? $limit : null;
        } catch (\Throwable) {
            return null;
        }
    }

    private function readCompileOptionMaxVariableNumber(\PDO $db): ?int
    {
        try {
            $result = $db->query('PRAGMA compile_options');
            if ($result === false) {
                return null;
            }

            while ($row = $result->fetch(\PDO::FETCH_NUM)) {
                $option = (string) ($row[0] ?? '');
                if (! str_starts_with($option, 'MAX_VARIABLE_NUMBER=')) {
                    continue;
                }

                $limit = (int) substr($option, strlen('MAX_VARIABLE_NUMBER='));

                return $limit > 0 ? $limit : null;
            }

            return null;
        } catch (\Throwable) {
            return null;
        }
    }

    private function writeOutputStream(\PDO $db, string $outputPath, array $pathOrder): void
    {
        $outputHandle = fopen($outputPath, 'wb');
        if ($outputHandle === false) {
            throw new \RuntimeException("Failed to open output file: {$outputPath}");
        }

        $visitByPathStatement = $db->prepare('
            SELECT
                visit_date,
                count
            FROM visits
            WHERE path = ?
            ORDER BY visit_date ASC
        ');

        fwrite($outputHandle, "{\n");

        $isFirstPath = true;
        foreach ($pathOrder as $path) {
            $visitByPathStatement->execute([$path]);
            $row = $visitByPathStatement->fetch(\PDO::FETCH_ASSOC);
            if ($row === false) {
                $visitByPathStatement->closeCursor();
                continue;
            }

            if (! $isFirstPath) {
                fwrite($outputHandle, ",\n");
            }

            $visitDate = $this->formatVisitDateForJson($row['visit_date']);
            fwrite($outputHandle, '    ' . $this->quotePathForJson($path) . ": {\n");
            fwrite($outputHandle, '        "' . $visitDate . '": ' . (int) $row['count']);

            while ($row = $visitByPathStatement->fetch(\PDO::FETCH_ASSOC)) {
                $visitDate = $this->formatVisitDateForJson($row['visit_date']);
                fwrite($outputHandle, ",\n" . '        "' . $visitDate . '": ' . (int) $row['count']);
            }

            fwrite($outputHandle, "\n    }");
            $visitByPathStatement->closeCursor();
            $isFirstPath = false;
        }

        fwrite($outputHandle, "\n}");
        fclose($outputHandle);
    }

    private function formatVisitDateForJson(int|string $packedVisitDate): string
    {
        $value = (string) $packedVisitDate;
        if (strlen($value) !== 8 || strspn($value, '0123456789') !== 8) {
            return $value;
        }

        return substr($value, 0, 4) . '-' . substr($value, 4, 2) . '-' . substr($value, 6, 2);
    }

    private function quotePathForJson(string $path): string
    {
        $length = strlen($path);
        $needsEscaping = false;

        for ($i = 0; $i < $length; $i++) {
            $byte = ord($path[$i]);
            if ($byte < 0x20 || $byte >= 0x80) {
                $encoded = json_encode($path);

                if ($encoded === false) {
                    throw new \RuntimeException('Failed to JSON encode path');
                }

                return $encoded;
            }

            if ($path[$i] === '\\' || $path[$i] === '"' || $path[$i] === '/') {
                $needsEscaping = true;
            }
        }

        if (! $needsEscaping) {
            return '"' . $path . '"';
        }

        return '"' . str_replace(['\\', '"', '/'], ['\\\\', '\\"', '\\/'], $path) . '"';
    }
}
