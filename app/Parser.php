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
    public static function parse($inputPath, $outputPath): void
    {
        gc_disable();

        $slugs = [
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

        $dateIds    = [];
        $dateLabels = [];
        $numDates   = 0;

        for ($year = 21; $year <= 26; $year++) {
            for ($month = 1; $month <= 12; $month++) {
                $daysInMonth = match ($month) {
                    2           => ($year % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default     => 31,
                };

                $monthStr   = $month < 10 ? '0' . $month : (string) $month;
                $datePrefix = ($year % 10) . '-' . $monthStr . '-';

                for ($day = 1; $day <= $daysInMonth; $day++) {
                    $key                   = $datePrefix . ($day < 10 ? '0' . $day : (string) $day);
                    $dateIds[$key]         = $numDates;
                    $dateLabels[$numDates] = $key;
                    $numDates++;
                }
            }
        }

        $fileHandle = fopen($inputPath, 'rb');
        stream_set_read_buffer($fileHandle, 0);
        $sample     = fread($fileHandle, 181_000);

        $slugBase    = [];
        $slugLabels  = [];
        $numSlugs    = 0;
        $numExpected = count($slugs);
        $bound       = strrpos($sample, "\n");

        for ($pos = 0; $pos < $bound;) {
            $newlinePos = strpos($sample, "\n", $pos + 52);

            if ($newlinePos === false) {
                break;
            }

            $slug = substr($sample, $pos + 25, $newlinePos - $pos - 51);

            if (!isset($slugBase[$slug])) {
                $slugBase[$slug]       = $numSlugs * $numDates;
                $slugLabels[$numSlugs] = $slug;
                $numSlugs++;

                if ($numSlugs === $numExpected) {
                    break;
                }
            }

            $pos = $newlinePos + 1;
        }

        unset($sample);

        foreach ($slugs as $slug) {
            if (!isset($slugBase[$slug])) {
                $slugBase[$slug]       = $numSlugs * $numDates;
                $slugLabels[$numSlugs] = $slug;
                $numSlugs++;
            }
        }

        $urlPfx = 'https://stitcher.io/blog/';
        $sufLen = 1;
        while (true) {
            $seen = [];
            $ok   = true;
            for ($s = 0; $s < $numSlugs; $s++) {
                $k = substr($urlPfx . $slugLabels[$s], -$sufLen);
                if (isset($seen[$k])) {
                    $ok = false;
                    break;
                }
                $seen[$k] = true;
            }
            if ($ok) break;
            $sufLen++;
        }
        unset($seen);

        $SHIFT   = 20;
        $MASK    = (1 << $SHIFT) - 1;
        $slugMap = [];
        $maxLine = 0;
        for ($s = 0; $s < $numSlugs; $s++) {
            $lineLen = strlen($slugLabels[$s]) + 52;
            if ($lineLen > $maxLine) $maxLine = $lineLen;
            $slugMap[substr($urlPfx . $slugLabels[$s], -$sufLen)] = ($lineLen << $SHIFT) | ($s * $numDates);
        }

        $sufOff = 26 + $sufLen;
        $fence  = $maxLine * 10 + $sufOff;

        $counts    = array_fill(0, $numSlugs * $numDates, 0);
        fseek($fileHandle, 0, SEEK_END);
        $remaining = ftell($fileHandle);
        fseek($fileHandle, 0);

        while ($remaining > 0) {
            $toRead = $remaining > 524_288 ? 524_288 : $remaining;
            $chunk  = fread($fileHandle, $toRead);
            $length = strlen($chunk);
            $remaining -= $length;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) break;

            if ($over = $length - $lastNl - 1) {
                fseek($fileHandle, -$over, SEEK_CUR);
                $remaining += $over;
            }

            $i = $lastNl;

            while ($i > $fence) {
                $v = $slugMap[substr($chunk, $i - $sufOff, $sufLen)];
                $counts[($v & $MASK) + $dateIds[substr($chunk, $i - 22, 7)]]++;
                $i -= $v >> $SHIFT;

                $v = $slugMap[substr($chunk, $i - $sufOff, $sufLen)];
                $counts[($v & $MASK) + $dateIds[substr($chunk, $i - 22, 7)]]++;
                $i -= $v >> $SHIFT;

                $v = $slugMap[substr($chunk, $i - $sufOff, $sufLen)];
                $counts[($v & $MASK) + $dateIds[substr($chunk, $i - 22, 7)]]++;
                $i -= $v >> $SHIFT;

                $v = $slugMap[substr($chunk, $i - $sufOff, $sufLen)];
                $counts[($v & $MASK) + $dateIds[substr($chunk, $i - 22, 7)]]++;
                $i -= $v >> $SHIFT;

                $v = $slugMap[substr($chunk, $i - $sufOff, $sufLen)];
                $counts[($v & $MASK) + $dateIds[substr($chunk, $i - 22, 7)]]++;
                $i -= $v >> $SHIFT;

                $v = $slugMap[substr($chunk, $i - $sufOff, $sufLen)];
                $counts[($v & $MASK) + $dateIds[substr($chunk, $i - 22, 7)]]++;
                $i -= $v >> $SHIFT;

                $v = $slugMap[substr($chunk, $i - $sufOff, $sufLen)];
                $counts[($v & $MASK) + $dateIds[substr($chunk, $i - 22, 7)]]++;
                $i -= $v >> $SHIFT;

                $v = $slugMap[substr($chunk, $i - $sufOff, $sufLen)];
                $counts[($v & $MASK) + $dateIds[substr($chunk, $i - 22, 7)]]++;
                $i -= $v >> $SHIFT;

                $v = $slugMap[substr($chunk, $i - $sufOff, $sufLen)];
                $counts[($v & $MASK) + $dateIds[substr($chunk, $i - 22, 7)]]++;
                $i -= $v >> $SHIFT;

                $v = $slugMap[substr($chunk, $i - $sufOff, $sufLen)];
                $counts[($v & $MASK) + $dateIds[substr($chunk, $i - 22, 7)]]++;
                $i -= $v >> $SHIFT;
            }

            while ($i >= $sufOff) {
                $v = $slugMap[substr($chunk, $i - $sufOff, $sufLen)];
                $counts[($v & $MASK) + $dateIds[substr($chunk, $i - 22, 7)]]++;
                $i -= $v >> $SHIFT;
            }
        }

        fclose($fileHandle);

        $outputHandle = fopen($outputPath, 'wb');
        stream_set_write_buffer($outputHandle, 4_194_304);

        $datePfx = [];
        for ($dateId = 0; $dateId < $numDates; $dateId++) {
            $datePfx[$dateId] = '        "202' . $dateLabels[$dateId] . '": ';
        }

        $slugOpen = [];
        for ($slugId = 0; $slugId < $numSlugs; $slugId++) {
            $slugOpen[$slugId] = "\n    \"\/blog\/" . str_replace('/', '\/', $slugLabels[$slugId]) . "\": {\n";
        }

        fwrite($outputHandle, '{');
        $slugSep = '';
        $base    = 0;

        for ($slugId = 0; $slugId < $numSlugs; $slugId++) {
            $c        = $base;
            $firstDay = -1;

            for ($d = 0; $d < $numDates; $d++) {
                if ($counts[$c]) {
                    $firstDay = $d;
                    break;
                }
                $c++;
            }

            if ($firstDay === -1) {
                $base += $numDates;
                continue;
            }

            $body = $datePfx[$firstDay] . $counts[$c];

            for ($d = $firstDay + 1; $d < $numDates; $d++) {
                $c++;
                if ($count = $counts[$c]) {
                    $body .= ",\n" . $datePfx[$d] . $count;
                }
            }

            fwrite($outputHandle, $slugSep . $slugOpen[$slugId] . $body . "\n    }");
            $slugSep  = ',';
            $base    += $numDates;
        }

        fwrite($outputHandle, "\n}");
        fclose($outputHandle);
    }
}
