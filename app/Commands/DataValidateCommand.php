<?php

namespace App\Commands;

use Tempest\Console\ConsoleCommand;
use Tempest\Console\ExitCode;
use Tempest\Console\HasConsole;

// final class DataValidateCommand
// {
//     use HasConsole;

//     #[ConsoleCommand]
//     public function __invoke(): ExitCode
//     {
//         $inputPath = __DIR__ . '/../../data/test-data.csv';
//         $actualPath = __DIR__ . '/../../data/test-data-actual.json';
//         $expectedPath = __DIR__ . '/../../data/test-data-expected.json';

//         if (is_file($actualPath)) {
//             unlink($actualPath);
//         }

//         $this->console->call('data:parse', [$inputPath, $actualPath]);

//         $actual = file_get_contents($actualPath);
//         $expected = file_get_contents($expectedPath);

//         if ($actual !== $expected) {
//             $this->console->error("Validation failed! Contents of {$actualPath} did not match {$expectedPath}");

//             return ExitCode::ERROR;
//         }

//         $this->console->success('Validation passed!');

//         return ExitCode::SUCCESS;
//     }
// }

final class DataValidateCommand
{
    use HasConsole;

    #[ConsoleCommand]
    public function __invoke(): ExitCode
    {
        $inputPath = __DIR__ . '/../../data/test-data.csv';
        $actualPath = __DIR__ . '/../../data/test-data-actual.json';
        $expectedPath = __DIR__ . '/../../data/test-data-expected.json';

        if (is_file($actualPath)) unlink($actualPath);

        $this->console->call('data:parse', [$inputPath, $actualPath]);

        $actualRaw = file_get_contents($actualPath);
        $expectedRaw = file_get_contents($expectedPath);

        if ($actualRaw === $expectedRaw) {
            $this->console->success('Validation passed!');
            return ExitCode::SUCCESS;
        }

        $actual = json_decode($actualRaw, true);
        $expected = json_decode($expectedRaw, true);

        foreach ($expected as $path => $dates) {
            if (!isset($actual[$path])) {
                $this->console->error("Missing Path: Expected '$path' but it was not found in actual output.");
                break;
            }
            foreach ($dates as $date => $count) {
                $actualCount = $actual[$path][$date] ?? 'NULL';
                if ($actualCount !== $count) {
                    $this->console->error("Mismatch at Path [$path] Date [$date]: Expected $count, got $actualCount");
                    break 2;
                }
            }
        }

        $this->console->info("Note: If the data above looks the same, check for escaped slashes or sorting.");
        return ExitCode::ERROR;
    }
}