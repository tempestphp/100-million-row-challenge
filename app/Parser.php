<?php

namespace App;

use App\Commands\Visit;
use DateTimeImmutable;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();

        $epoch = new DateTimeImmutable('2021-02-08');
        $days = 1846;
        $dateIds = [];
        for ($i = 0; $i < $days; $i++) {
            $date = $epoch->modify("+$i days")->format('Y-m-d');
            $dateIds[$date] = $i;
        }

        $slugs = array_map(fn (Visit $v) => substr($v->uri, 25), Visit::all());
        $slugCount = count($slugs);
        $slugIds = array_map(fn ($v) => $v * $days, array_flip($slugs));
        $slugSorted = [];

        $counts = array_fill(0, $slugCount * $days, 0);

        $fp = fopen($inputPath, 'r');
        while ($line = fgets($fp)) {
            $slug = substr($line, 25, -27);

            if (!isset($slugSorted[$slug])) {
                $slugSorted[$slug] = count($slugSorted);
            }

            if (count($slugSorted) === $slugCount) {
                break;
            }
        }
        fclose($fp);

        $slugSorted = array_flip($slugSorted);

        $fp = fopen($inputPath, 'r');
        while ($line = fgets($fp)) {
            $slug = substr($line, 25, -27);
            $date = substr($line, -26, 10);

            $s = $slugIds[$slug];
            $d = $dateIds[$date];

            $counts[$s + $d]++;
        }
        fclose($fp);

        $fp = fopen($outputPath, 'w');
        stream_set_write_buffer($fp, 1_048_576);
        fwrite($fp, '{');
        $firstSlug = true;
        foreach ($slugSorted as $slug) {
            $buffer = $firstSlug ? '' : ',';
            $buffer .= "\n    \"\/blog\/$slug\": {";
            $s = $slugIds[$slug];
            $firstDate = true;
            foreach ($dateIds as $date => $d) {
                $count = $counts[$s + $d];
                if ($count === 0) continue;
                $buffer .= $firstDate ? '' : ',';
                $buffer .= "\n        \"$date\": $count";

                $firstDate = false;
            }
            $buffer .= "\n    }";
            fwrite($fp, $buffer);

            $firstSlug = false;
        }
        fwrite($fp, "\n}");
        fclose($fp);
    }
}