<?php

namespace App;

use function array_fill;
use function count;
use function fclose;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function str_replace;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use const SEEK_CUR;
use const SEEK_END;

final class Parser
{
    private const array SQUAD = [
        'which-editor-to-choose',
        'tackling_responsive_images-part_1',
        'tackling_responsive_images-part_2',
        'image_optimizers',
        'static_sites_vs_caching',
        'stitcher-alpha-4',
        'simplest-plugin-support',
        'stitcher-alpha-5',
        'php-generics-and-why-we-need-them',
        'stitcher-beta-1',
        'array-objects-with-fixed-types',
        'performance-101-building-the-better-web',
        'process-forks',
        'object-oriented-generators',
        'responsive-images-as-css-background',
        'a-programmers-cognitive-load',
        'mastering-key-bindings',
        'stitcher-beta-2',
        'phpstorm-performance',
        'optimised-uuids-in-mysql',
        'asynchronous-php',
        'mysql-import-json-binary-character-set',
        'where-a-curly-bracket-belongs',
        'mysql-query-logging',
        'mysql-show-foreign-key-errors',
        'responsive-images-done-right',
        'phpstorm-tips-for-power-users',
        'what-php-can-be',
        'phpstorm-performance-issues-on-osx',
        'dependency-injection-for-beginners',
        'liskov-and-type-safety',
        'acquisition-by-giants',
        'visual-perception-of-code',
        'service-locator-anti-pattern',
        'the-web-in-2045',
        'eloquent-mysql-views',
        'laravel-view-models',
        'laravel-view-models-vs-view-composers',
        'organise-by-domain',
        'array-merge-vs + ',
        'share-a-blog-assertchris-io',
        'phpstorm-performance-october-2018',
        'structuring-unstructured-data',
        'share-a-blog-codingwriter-com',
        'new-in-php-73',
        'share-a-blog-betterwebtype-com',
        'have-you-thought-about-casing',
        'comparing-dates',
        'share-a-blog-sebastiandedeyne-com',
        'analytics-for-developers',
        'announcing-aggregate',
        'php-jit',
        'craftsmen-know-their-tools',
        'laravel-queueable-actions',
        'php-73-upgrade-mac',
        'array-destructuring-with-list-in-php',
        'unsafe-sql-functions-in-laravel',
        'starting-a-newsletter',
        'short-closures-in-php',
        'solid-interfaces-and-final-rant-with-brent',
        'php-in-2019',
        'starting-a-podcast',
        'a-project-at-spatie',
        'what-are-objects-anyway-rant-with-brent',
        'tests-and-types',
        'typed-properties-in-php-74',
        'preloading-in-php-74',
        'things-dependency-injection-is-not-about',
        'a-letter-to-the-php-team',
        'a-letter-to-the-php-team-reply-to-joe',
        'guest-posts',
        'can-i-translate-your-blog',
        'laravel-has-many-through',
        'laravel-custom-relation-classes',
        'new-in-php-74',
        'php-74-upgrade-mac',
        'php-preload-benchmarks',
        'php-in-2020',
        'enums-without-enums',
        'bitwise-booleans-in-php',
        'event-driven-php',
        'minor-versions-breaking-changes',
        'combining-event-sourcing-and-stateful-systems',
        'array-chunk-in-php',
        'php-8-in-8-code-blocks',
        'builders-and-architects-two-types-of-programmers',
        'the-ikea-effect',
        'php-74-in-7-code-blocks',
        'improvements-on-laravel-nova',
        'type-system-in-php-survey',
        'merging-multidimensional-arrays-in-php',
        'what-is-array-plus-in-php',
        'type-system-in-php-survey-results',
        'constructor-promotion-in-php-8',
        'abstract-resources-in-laravel-nova',
        'braille-and-the-history-of-software',
        'jit-in-real-life-web-applications',
        'php-8-match-or-switch',
        'why-we-need-named-params-in-php',
        'shorthand-comparisons-in-php',
        'php-8-before-and-after',
        'php-8-named-arguments',
        'my-journey-into-event-sourcing',
        'differences',
        'annotations',
        'dont-get-stuck',
        'attributes-in-php-8',
        'the-case-for-transpiled-generics',
        'phpstorm-scopes',
        'why-light-themes-are-better-according-to-science',
        'what-a-good-pr-looks-like',
        'front-line-php',
        'php-8-jit-setup',
        'php-8-nullsafe-operator',
        'new-in-php-8',
        'php-8-upgrade-mac',
        'when-i-lost-a-few-hundred-leads',
        'websites-like-star-wars',
        'php-reimagined',
        'a-storm-in-a-glass-of-water',
        'php-enums-before-php-81',
        'php-enums',
        'dont-write-your-own-framework',
        'honesty',
        'thoughts-on-event-sourcing',
        'what-event-sourcing-is-not-about',
        'fibers-with-a-grain-of-salt',
        'php-in-2021',
        'parallel-php',
        'why-we-need-multi-line-short-closures-in-php',
        'a-new-major-version-of-laravel-event-sourcing',
        'what-about-config-builders',
        'opinion-driven-design',
        'php-version-stats-july-2021',
        'what-about-request-classes',
        'cloning-readonly-properties-in-php-81',
        'an-event-driven-mindset',
        'php-81-before-and-after',
        'optimistic-or-realistic-estimates',
        'we-dont-need-runtime-type-checks',
        'the-road-to-php',
        'why-do-i-write',
        'rational-thinking',
        'named-arguments-and-variadic-functions',
        're-on-using-psr-abstractions',
        'my-ikea-clock',
        'php-81-readonly-properties',
        'birth-and-death-of-a-framework',
        'php-81-new-in-initializers',
        'route-attributes',
        'generics-in-php-video',
        'php-81-in-8-code-blocks',
        'new-in-php-81',
        'php-81-performance-in-real-life',
        'php-81-upgrade-mac',
        'how-to-be-right-on-the-internet',
        'php-version-stats-january-2022',
        'php-in-2022',
        'how-i-plan',
        'twitter-home-made-me-miserable',
        'its-your-fault',
        'dealing-with-dependencies',
        'php-in-2021-video',
        'generics-in-php-1',
        'generics-in-php-2',
        'generics-in-php-3',
        'generics-in-php-4',
        'goodbye',
        'strategies',
        'dealing-with-deprecations',
        'attribute-usage-in-top-php-packages',
        'php-enum-style-guide',
        'clean-and-minimalistic-phpstorm',
        'stitcher-turns-5',
        'php-version-stats-july-2022',
        'evolution-of-a-php-object',
        'uncertainty-doubt-and-static-analysis',
        'road-to-php-82',
        'php-performance-across-versions',
        'light-colour-schemes-are-better',
        'deprecated-dynamic-properties-in-php-82',
        'php-reimagined-part-2',
        'thoughts-on-asymmetric-visibility',
        'uses',
        'php-82-in-8-code-blocks',
        'readonly-classes-in-php-82',
        'deprecating-spatie-dto',
        'php-82-upgrade-mac',
        'php-annotated',
        'you-cannot-find-me-on-mastodon',
        'new-in-php-82',
        'all-i-want-for-christmas',
        'upgrading-to-php-82',
        'php-version-stats-january-2023',
        'php-in-2023',
        'tabs-are-better',
        'sponsors',
        'why-curly-brackets-go-on-new-lines',
        'my-10-favourite-php-functions',
        'acronyms',
        'code-folding',
        'light-colour-schemes',
        'slashdash',
        'thank-you-kinsta',
        'cloning-readonly-properties-in-php-83',
        'limited-by-committee',
        'things-considered-harmful',
        'procedurally-generated-game-in-php',
        'dont-be-clever',
        'override-in-php-83',
        'php-version-stats-july-2023',
        'is-a-or-acts-as',
        'rfc-vote',
        'new-in-php-83',
        'i-dont-know',
        'passion-projects',
        'php-version-stats-january-2024',
        'the-framework-that-gets-out-of-your-way',
        'a-syntax-highlighter-that-doesnt-suck',
        'building-a-custom-language-in-tempest-highlight',
        'testing-patterns',
        'php-in-2024',
        'tagged-singletons',
        'twitter-exit',
        'a-vocal-minority',
        'php-version-stats-july-2024',
        'you-should',
        'new-with-parentheses-php-84',
        'html-5-in-php-84',
        'array-find-in-php-84',
        'its-all-just-text',
        'improved-lazy-loading',
        'i-dont-code-the-way-i-used-to',
        'php-84-at-least',
        'extends-vs-implements',
        'a-simple-approach-to-static-generation',
        'building-a-framework',
        'tagging-tempest-livestream',
        'things-i-learned-writing-a-fiction-novel',
        'unfair-advantage',
        'new-in-php-84',
        'php-version-stats-january-2025',
        'theoretical-engineers',
        'static-websites-with-tempest',
        'request-objects-in-tempest',
        'php-verse-2025',
        'tempest-discovery-explained',
        'php-version-stats-june-2025',
        'pipe-operator-in-php-85',
        'a-year-of-property-hooks',
        'readonly-or-private-set',
        'things-i-wish-i-knew',
        'impact-charts',
        'whats-your-motivator',
        'vendor-locked',
        'reducing-code-motion',
        'sponsoring-open-source',
        'my-wishlist-for-php-in-2026',
        'game-changing-editions',
        'new-in-php-85',
        'flooded-rss',
        'php-2026',
        'open-source-strategies',
        'not-optional',
        'processing-11-million-rows',
        'ai-induced-skepticism',
        'php-86-partial-function-application',
        '11-million-rows-in-seconds',
    ];

    public static function parse($inputPath, $outputPath)
    {
        self::lusail($inputPath, $outputPath);
    }

    public static function lusail($inputPath, $outputPath)
    {
        gc_disable();

        $scaloni = [];
        $fixtures = [];
        $matches = 0;
        for ($y = 1; $y <= 6; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => $y === 4 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $mStr = ($m < 10 ? '0' : '') . $m;
                $ymStr = $y . '-' . $mStr . '-';
                for ($d = 1; $d <= $maxD; $d++) {
                    $key = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $scaloni[$key] = $matches;
                    $fixtures[$matches] = $key;
                    $matches++;
                }
            }
        }

        $dibu = fopen($inputPath, 'rb');
        stream_set_read_buffer($dibu, 0);
        $header = fread($dibu, 181000);

        $pitch = [];
        $called = [];
        $squad = 0;
        $pos = 0;
        $headerEnd = strrpos($header, "\n") ?: 0;
        $expected = count(self::SQUAD);

        while ($pos < $headerEnd && $squad < $expected) {
            $nl = strpos($header, "\n", $pos + 52);
            if ($nl === false) break;

            $player = substr($header, $pos + 25, $nl - $pos - 51);
            if (!isset($called[$player])) {
                $pitch[$squad] = $player;
                $called[$player] = $squad * $matches;
                $squad++;
            }
            $pos = $nl + 1;
        }
        unset($header);

        foreach (self::SQUAD as $player) {
            if (!isset($called[$player])) {
                $pitch[$squad] = $player;
                $called[$player] = $squad * $matches;
                $squad++;
            }
        }
        unset($called);

        $shirt = 'https://stitcher.io/blog/';
        $cuti = 1;
        while (true) {
            $marks = [];
            $s = 0;
            while ($s < $squad) {
                $number = substr($shirt . $pitch[$s], -$cuti);
                if (isset($marks[$number])) {
                    $cuti++;
                    continue 2;
                }
                $marks[$number] = true;
                $s++;
            }
            break;
        }

        $messi = 20;
        $dePaul = (1 << $messi) - 1;
        $mbappe = 0;
        $enzo = [];
        for ($s = 0; $s < $squad; $s++) {
            $molina = strlen($pitch[$s]) + 52;
            if ($molina > $mbappe) $mbappe = $molina;
            $enzo[substr($shirt . $pitch[$s], -$cuti)] = ($molina << $messi) | ($s * $matches);
        }
        $macAllister = 26 + $cuti;
        $otamendi = ($mbappe * 10) + $macAllister;

        $goals = $squad * $matches;

        fseek($dibu, 0, SEEK_END);
        $remaining = ftell($dibu);
        fseek($dibu, 0);

        $scoreboard = array_fill(0, $goals, 0);

        while ($remaining > 0) {
            $toRead = $remaining > 524_288 ? 524_288 : $remaining;
            $play = fread($dibu, $toRead);
            $length = strlen($play);
            $remaining -= $length;

            $whistle = strrpos($play, "\n");
            if ($whistle === false) break;

            if ($offside = $length - $whistle - 1) {
                fseek($dibu, -$offside, SEEK_CUR);
                $remaining += $offside;
            }

            $i = $whistle;

            while ($i > $otamendi) {
                $v = $enzo[substr($play, $i - $macAllister, $cuti)];
                $scoreboard[($v & $dePaul) + $scaloni[substr($play, $i - 22, 7)]]++;
                $i -= $v >> $messi;

                $v = $enzo[substr($play, $i - $macAllister, $cuti)];
                $scoreboard[($v & $dePaul) + $scaloni[substr($play, $i - 22, 7)]]++;
                $i -= $v >> $messi;

                $v = $enzo[substr($play, $i - $macAllister, $cuti)];
                $scoreboard[($v & $dePaul) + $scaloni[substr($play, $i - 22, 7)]]++;
                $i -= $v >> $messi;

                $v = $enzo[substr($play, $i - $macAllister, $cuti)];
                $scoreboard[($v & $dePaul) + $scaloni[substr($play, $i - 22, 7)]]++;
                $i -= $v >> $messi;

                $v = $enzo[substr($play, $i - $macAllister, $cuti)];
                $scoreboard[($v & $dePaul) + $scaloni[substr($play, $i - 22, 7)]]++;
                $i -= $v >> $messi;

                $v = $enzo[substr($play, $i - $macAllister, $cuti)];
                $scoreboard[($v & $dePaul) + $scaloni[substr($play, $i - 22, 7)]]++;
                $i -= $v >> $messi;

                $v = $enzo[substr($play, $i - $macAllister, $cuti)];
                $scoreboard[($v & $dePaul) + $scaloni[substr($play, $i - 22, 7)]]++;
                $i -= $v >> $messi;

                $v = $enzo[substr($play, $i - $macAllister, $cuti)];
                $scoreboard[($v & $dePaul) + $scaloni[substr($play, $i - 22, 7)]]++;
                $i -= $v >> $messi;

                $v = $enzo[substr($play, $i - $macAllister, $cuti)];
                $scoreboard[($v & $dePaul) + $scaloni[substr($play, $i - 22, 7)]]++;
                $i -= $v >> $messi;

                $v = $enzo[substr($play, $i - $macAllister, $cuti)];
                $scoreboard[($v & $dePaul) + $scaloni[substr($play, $i - 22, 7)]]++;
                $i -= $v >> $messi;
            }

            while ($i >= $macAllister) {
                $v = $enzo[substr($play, $i - $macAllister, $cuti)];
                $scoreboard[($v & $dePaul) + $scaloni[substr($play, $i - 22, 7)]]++;
                $i -= $v >> $messi;
            }
        }

        fclose($dibu);

        $trophy = fopen($outputPath, 'wb');
        stream_set_write_buffer($trophy, 4_194_304);
        fwrite($trophy, '{');

        $dates = [];
        for ($d = 0; $d < $matches; $d++) {
            $dates[$d] = '        "202' . $fixtures[$d] . '": ';
        }

        $starters = [];
        for ($s = 0; $s < $squad; $s++) {
            $starters[$s] = "\n    \"\/blog\/" . str_replace('/', '\/', $pitch[$s]) . "\": {\n";
        }

        $sep = '';
        $base = 0;
        for ($s = 0; $s < $squad; $s++) {
            $d = 0;
            $idx = $base;
            while ($d < $matches && $scoreboard[$idx] === 0) {
                $d++;
                $idx++;
            }

            if ($d === $matches) {
                $base += $matches;
                continue;
            }

            $buf = $dates[$d] . $scoreboard[$idx];
            $d++;
            while ($d < $matches) {
                $idx++;
                if ($scoreboard[$idx] !== 0) {
                    $buf .= ",\n" . $dates[$d] . $scoreboard[$idx];
                }
                $d++;
            }

            fwrite($trophy, $sep . $starters[$s] . $buf . "\n    }");
            $sep = ',';
            $base += $matches;
        }

        fwrite($trophy, "\n}");
        fclose($trophy);
    }
}
