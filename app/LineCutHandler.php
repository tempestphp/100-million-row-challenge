<?php

declare(strict_types=1);

namespace App;

final class LineCutHandler extends Handler
{
    private const int LEFT = 25; // https://stitcher.io/blog
    private const int RIGHT = 16; // T01:15:20+00:00 \n

    protected function handle(string $data): void
    {
        $data = substr($data, self::LEFT, -self::RIGHT);

        $this->downstream?->handle($data);
    }
}
