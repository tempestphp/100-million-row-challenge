<?php

namespace App;

use Exception;
use Innmind\OperatingSystem\OperatingSystem;
use Innmind\Filesystem\{
    Name,
    File,
    File\Content,
};
use Innmind\Url\Path;
use Innmind\Immutable\{
    Sequence,
    Str,
};

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $dir = \dirname($inputPath);
        $file = \basename($inputPath);
        $os = OperatingSystem::new();
        $outputs = [];
        $_ = $os
            ->filesystem()
            ->mount(Path::of($dir.'/'))
            ->unwrap()
            ->get(Name::of($file))
            ->attempt(static fn() => new \Exception)
            ->unwrap()
            ->content()
            ->lines()
            ->map(static fn($line) => $line->str()->drop(25))
            ->exclude(static fn($line) => $line->empty())
            ->map(static fn($line) => $line->split(',')->toList())
            ->map(static fn($line) => [
                $line[0]->toString(),
                $line[1]->take(10)->toString(),
            ])
            ->foreach(
                static function($line) use (&$outputs) {
                    [$path, $date] = $line;

                    if (!\array_key_exists($path, $outputs)) {
                        $outputs[$path] = $file = \fopen('php://memory', 'w+');
                        \fwrite($file, $path."\n");
                        \fwrite($file, $date."\n");
                    } else {
                        \fwrite($outputs[$path], $date."\n");
                    }
                },
            );

        $content = Sequence::lazy(static function() use ($outputs) {
            foreach ($outputs as $output) {
                yield $output;
            }
        })
            ->flatMap(static function($output) {
                \fseek($output, 0);
                $path = \fgets($output);
                $dates = [];

                while ($date = \fgets($output)) {
                    if ($date === '') {
                        break;
                    }

                    $date = Str::of($date)->take(10)->toString();
                    $dates[$date] ??= [$date, 0];
                    $dates[$date][1]++;
                }

                \ksort($dates);

                $path = Str::of($path)
                    ->dropEnd(1)
                    ->map(static fn($string) => \json_encode('/blog/'.$string))
                    ->toString();

                return Sequence::of(...$dates)
                    ->map(static function($date) {
                        [$date, $count] = $date;

                        return '    "'.$date.'": '.$count.',';
                    })
                    ->add('},')
                    ->prepend(Sequence::of($path.': {'));
            })
            ->map(static fn($line) => '    '.$line."\n")
            ->prepend(Sequence::of('{'."\n"))
            ->add('}')
            ->aggregate(static fn($a, $b) => match ($b) {
                '}', "    },\n" => Sequence::of(
                    Str::of($a)->dropEnd(2)->append("\n")->toString(),
                    $b,
                ),
                default => Sequence::of($a, $b),
            })
            ->map(Str::of(...));

        $dir = \dirname($outputPath);
        $file = \basename($outputPath);
        $_ = $os
            ->filesystem()
            ->mount(Path::of($dir.'/'))
            ->unwrap()
            ->add(File::named(
                $file,
                Content::ofChunks($content),
            ))
            ->unwrap();
    }
}
