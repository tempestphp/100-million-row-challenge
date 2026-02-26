<?php

namespace App;

use App\Solutions\Papoon;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        (new Papoon())($inputPath, $outputPath);
    }
}