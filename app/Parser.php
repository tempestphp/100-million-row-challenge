<?php

namespace App;

use App\Commands\Visit;
use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        shell_exec(__DIR__ . '/../emb/main ' . escapeshellarg($inputPath) . ' ' . escapeshellarg($outputPath));
    }
}
