<?php

namespace App;

use App\Solutions\Papoon;
use App\Solutions\SingleThread;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        (new SingleThread())($inputPath, $outputPath);
    }
}