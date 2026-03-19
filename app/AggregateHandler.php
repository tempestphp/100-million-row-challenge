<?php

declare(strict_types=1);

namespace App;

final class AggregateHandler extends Handler
{
    /**
     * @var array<string, array<int, int>>
     */
    protected array $data = [];

    protected function tearDown(): void
    {
        $keys = array_keys($this->data);

        sort($keys);

        foreach ($this->data as $key => $values) {
            // converts as key,date1,count,date2,count,...
            $this->downstream?->handle($key.','.self::formatValues($values));
        }

        $this->downstream?->tearDown();
    }

    protected function handle(string $data): void
    {
        [$key, $date] = explode(',', $data, 2);

        if (!isset($this->data[$key])) {
            $this->data[$key] = [];
        }

        [$y, $m, $d] = explode('-', $date);

        $intDate = (int) ($y.$m.$d);

        if (!isset($this->data[$key][$intDate])) {
            $this->data[$key][$intDate] = 1;
        } else {
            ++$this->data[$key][$intDate];
        }
    }

    /**
     * @param array<int, int> $values
     */
    protected static function formatValues(array $values): string
    {
        $keys = array_keys($values);

        sort($keys);

        return implode(
            ',',
            array_map(
                static fn ($key) => $key.','.$values[$key],
                $keys
            )
        );
    }
}
