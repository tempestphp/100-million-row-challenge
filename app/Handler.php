<?php

declare(strict_types=1);

namespace App;

abstract class Handler
{
    protected ?Handler $downstream = null;

    protected function setUp(): void
    {
        $this->downstream?->setUp();
    }

    protected function tearDown(): void
    {
        $this->downstream?->tearDown();
    }

    public function setDownstream(self $downstream): void
    {
        $this->downstream = $downstream;
    }

    /**
     * @param Handler[] $handlers
     */
    public static function chain(array $handlers): void
    {
        $lastHandler = null;
        foreach ($handlers as $handler) {
            $lastHandler?->setDownstream($handler);
            $lastHandler = $handler;
        }

        array_first($handlers)?->setUp();
    }

    protected function handle(string $data): void
    {
        $this->downstream?->handle($data);
    }
}
