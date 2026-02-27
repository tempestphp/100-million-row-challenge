<?php

namespace App\Commands;

use Tempest\Console\ConsoleCommand;
use Tempest\Console\ExitCode;
use Tempest\Console\HasConsole;

final class DataValidateCommand
{
    use HasConsole;

    #[ConsoleCommand]
    public function __invoke(): ExitCode
    {
        $inputPath = __DIR__ . '/../../data/test-data.csv';
        $actualPath = __DIR__ . '/../../data/test-data-actual.json';
        $expectedPath = __DIR__ . '/../../data/test-data-expected.json';

        $this->console->call('data:parse', [$inputPath, $actualPath]);

        $actual = file_get_contents($actualPath);
        $expected = file_get_contents($expectedPath);

        if ($actual !== $expected) {
            $this->console->error("Validation failed! Contents of {$actualPath} did not match {$expectedPath}");
            $this->console->writeln("Actual length: " . strlen($actual));
            $this->console->writeln("Expected length: " . strlen($expected));
            
            for ($i = 0; $i < min(strlen($actual), strlen($expected)); $i++) {
                if ($actual[$i] !== $expected[$i]) {
                    $this->console->writeln("First difference at index $i: Actual=" . ord($actual[$i]) . " Expected=" . ord($expected[$i]));
                    break;
                }
            }

            return ExitCode::ERROR;
        }

        $this->console->success('Validation passed!');

        return ExitCode::SUCCESS;
    }
}