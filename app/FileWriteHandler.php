<?php

declare(strict_types=1);

namespace App;

final class FileWriteHandler extends Handler
{
    /**
     * @var false|resource
     */
    private mixed $handle;

    public function __construct(private readonly string $path) {}

    public function setUp(): void
    {
        $this->handle = fopen($this->path, 'w');
    }

    protected function tearDown(): void
    {
        if ($this->handle) {
            fclose($this->handle);
        }
    }

    protected function handle(string $data): void
    {
        if ($this->handle) {
            fwrite($this->handle, $data);
        }
    }
}
