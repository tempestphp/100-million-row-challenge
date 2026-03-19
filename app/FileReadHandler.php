<?php

declare(strict_types=1);

namespace App;

final class FileReadHandler extends Handler
{
    public function __construct(private readonly string $path) {}

    public function setUp(): void
    {
        $handle = fopen($this->path, 'r');

        if (!$handle) {
            return;
        }

        $this->downstream?->setUp();

        while (($line = fgets($handle)) !== false) {
            $this->handle($line);
        }

        fclose($handle);

        $this->tearDown();
    }
}
