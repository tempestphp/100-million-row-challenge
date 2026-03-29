<?php

declare(strict_types=1);

namespace App\Ilex;

use JsonSerializable;

final class Visits implements JsonSerializable
{
    public function __construct(
        private array $dateVisit,
    ) {
    }

    public function add(string $data): void
    {
        if (isset($this->dateVisit[$data])) {
            $this->dateVisit[$data]++;
            return;
        }

        $this->dateVisit[$data] = 1;
    }

    public static function init(string $date): self
    {

        return new Visits([$date => 1]);
    }

    public function jsonSerialize(): array
    {
        ksort($this->dateVisit);
        return $this->dateVisit;

    }
}