<?php

declare(strict_types=1);

namespace App;

use function sprintf;

final class JsonPrepareHandler extends Handler
{
    private const string KEY_PREFIX = '\\/blog\\/';
    private const string PADDING = '    ';
    private bool $first = true;

    protected function setUp(): void
    {
        $this->downstream?->setUp();

        $this->downstream?->handle('{');
    }

    protected function tearDown(): void
    {
        $this->downstream?->handle(PHP_EOL.'}');

        $this->downstream?->tearDown();
    }

    protected function handle(string $data): void
    {
        [$key, $values] = explode(',', $data, 2);

        $this->downstream?->handle(sprintf(
            '%s"%s": {%s}',
            ($this->first ? '' : ',').PHP_EOL.self::PADDING,
            self::KEY_PREFIX.$key,
            // converts '20250602,5' to '"2025-06-02": 5' (with padding)
            preg_replace('/(\d{4})(\d{2})(\d{2}),(\d+)/', PHP_EOL.self::PADDING.self::PADDING.'"$1-$2-$3": $4', $values).PHP_EOL.self::PADDING,
        ));

        $this->first = false;
    }
}
