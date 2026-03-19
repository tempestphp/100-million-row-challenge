<?php

declare(strict_types=1);

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        Handler::chain([
            new FileReadHandler($inputPath),
            new LineCutHandler(),
            new AggregateHandler(),
            new JsonPrepareHandler(),
            new FileWriteHandler($outputPath),
        ]);
    }
}
