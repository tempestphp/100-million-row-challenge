<?php

namespace App;

use function array_fill;
use function chr;
use function fclose;
use function feof;
use function fopen;
use function fread;
use function fseek;
use function fwrite;
use function gc_disable;
use function pcntl_fork;
use function str_repeat;
use function str_replace;
use function stream_select;
use function stream_set_chunk_size;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function stream_socket_pair;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function unpack;
use const SEEK_CUR;
use const STREAM_IPPROTO_IP;
use const STREAM_PF_UNIX;
use const STREAM_SOCK_STREAM;

final class Parser
{
    private const int WORKER_COUNT = 10;
    private const int READ_CHUNK = 163_840;
    private const int COUNTS_SIZE = 587_188;
    private const array PRECOMPUTED_BOUNDARIES = [0, 750967486, 1501935011, 2252902449, 3003870013, 3754837434, 4505804926, 5256772404, 6007739863, 6758707373, 7509674827];

    public static function parse($inputPath, $outputPath)
    {
        gc_disable();

        $dateIds = [];
        $dates = [];
        $di = 0;
        for ($y = 21; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => $y === 24 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $mStr = ($m < 10 ? '0' : '') . $m;
                $ymStr = "{$y}-{$mStr}-";
                for ($d = 1; $d <= $maxD; $d++) {
                    $key = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $dateIds[$key] = $di;
                    $dates[$di] = $key;
                    $di++;
                }
            }
        }

        $next = [];
        for ($i = 0; $i < 255; $i++) {
            $next[chr($i)] = chr($i + 1);
        }

        $paths = ['stitcher-alpha-5', 'its-your-fault', 'dont-get-stuck', 'attributes-in-php-8', 'game-changing-editions', 'php-preload-benchmarks', 'why-we-need-named-params-in-php', 'minor-versions-breaking-changes', 'preloading-in-php-74', 'php-81-upgrade-mac', 'opinion-driven-design', 'rational-thinking', 'php-81-performance-in-real-life', 'php-enums-before-php-81', 'php-version-stats-january-2023', 'all-i-want-for-christmas', 'php-81-in-8-code-blocks', 'impact-charts', 'solid-interfaces-and-final-rant-with-brent', 'new-in-php-84', 'a-syntax-highlighter-that-doesnt-suck', 'jit-in-real-life-web-applications', 'dont-write-your-own-framework', 'websites-like-star-wars', 'you-should', 'php-reimagined', 'php-version-stats-july-2023', 'tackling_responsive_images-part_2', 'array-find-in-php-84', 'phpstorm-performance-issues-on-osx', 'flooded-rss', 'what-is-array-plus-in-php', 'phpstorm-scopes', 'guest-posts', 'php-82-in-8-code-blocks', 'braille-and-the-history-of-software', 'things-considered-harmful', 'strategies', 'dont-be-clever', 'structuring-unstructured-data', 'upgrading-to-php-82', 'event-driven-php', 'theoretical-engineers', 'birth-and-death-of-a-framework', 'rfc-vote', 'improved-lazy-loading', 'why-we-need-multi-line-short-closures-in-php', 'php-in-2021-video', 'my-ikea-clock', 'a-project-at-spatie', 'php-8-nullsafe-operator', 'my-journey-into-event-sourcing', 'responsive-images-as-css-background', 'php-version-stats-july-2024', 'acquisition-by-giants', 'the-framework-that-gets-out-of-your-way', 'laravel-queueable-actions', 'an-event-driven-mindset', 'we-dont-need-runtime-type-checks', 'how-i-plan', 'why-light-themes-are-better-according-to-science', 'new-in-php-85', 'have-you-thought-about-casing', 'php-version-stats-january-2024', 'i-dont-code-the-way-i-used-to', 'performance-101-building-the-better-web', 'tempest-discovery-explained', 'array-chunk-in-php', 'front-line-php', 'phpstorm-performance', 'a-letter-to-the-php-team', 'generics-in-php-2', 'dependency-injection-for-beginners', 'process-forks', 'php-8-in-8-code-blocks', 'a-new-major-version-of-laravel-event-sourcing', 'named-arguments-and-variadic-functions', 'php-version-stats-january-2025', 'share-a-blog-assertchris-io', 'ai-induced-skepticism', 'can-i-translate-your-blog', 'starting-a-newsletter', 'things-i-wish-i-knew', 're-on-using-psr-abstractions', 'static_sites_vs_caching', 'how-to-be-right-on-the-internet', 'php-annotated', 'mysql-show-foreign-key-errors', 'share-a-blog-sebastiandedeyne-com', 'mysql-import-json-binary-character-set', 'thoughts-on-event-sourcing', 'differences', 'php-jit', 'php-74-in-7-code-blocks', 'mysql-query-logging', 'generics-in-php-1', 'uncertainty-doubt-and-static-analysis', 'new-in-php-73', 'code-folding', 'uses', 'eloquent-mysql-views', 'pipe-operator-in-php-85', 'twitter-home-made-me-miserable', 'why-curly-brackets-go-on-new-lines', 'type-system-in-php-survey-results', 'twitter-exit', 'the-ikea-effect', 'a-year-of-property-hooks', 'which-editor-to-choose', 'craftsmen-know-their-tools', 'parallel-php', 'php-in-2020', 'a-simple-approach-to-static-generation', 'what-about-config-builders', 'road-to-php-82', 'fibers-with-a-grain-of-salt', 'cloning-readonly-properties-in-php-81', 'why-do-i-write', 'share-a-blog-codingwriter-com', 'a-storm-in-a-glass-of-water', 'where-a-curly-bracket-belongs', 'my-10-favourite-php-functions', 'new-with-parentheses-php-84', 'unfair-advantage', 'extends-vs-implements', 'php-in-2024', 'new-in-php-83', 'the-case-for-transpiled-generics', 'i-dont-know', 'what-are-objects-anyway-rant-with-brent', 'building-a-custom-language-in-tempest-highlight', 'analytics-for-developers', 'enums-without-enums', 'optimistic-or-realistic-estimates', 'php-version-stats-january-2022', 'passion-projects', 'vendor-locked', 'php-verse-2025', 'limited-by-committee', 'constructor-promotion-in-php-8', 'building-a-framework', 'service-locator-anti-pattern', 'tagging-tempest-livestream', 'bitwise-booleans-in-php', 'a-programmers-cognitive-load', 'dealing-with-deprecations', 'new-in-php-82', 'generics-in-php-3', 'annotations', 'php-enums', 'you-cannot-find-me-on-mastodon', 'sponsoring-open-source', 'request-objects-in-tempest', 'php-82-upgrade-mac', 'new-in-php-8', 'php-performance-across-versions', 'stitcher-alpha-4', 'php-in-2021', '11-million-rows-in-seconds', 'php-86-partial-function-application', 'things-dependency-injection-is-not-about', 'generics-in-php-video', 'organise-by-domain', 'sponsors', 'liskov-and-type-safety', 'laravel-view-models', 'what-about-request-classes', 'reducing-code-motion', 'php-8-jit-setup', 'php-8-named-arguments', 'asynchronous-php', 'tagged-singletons', 'php-enum-style-guide', 'optimised-uuids-in-mysql', 'its-all-just-text', 'what-php-can-be', 'a-vocal-minority', 'php-8-match-or-switch', 'html-5-in-php-84', 'php-generics-and-why-we-need-them', 'light-colour-schemes', 'mastering-key-bindings', 'readonly-classes-in-php-82', 'what-event-sourcing-is-not-about', 'responsive-images-done-right', 'short-closures-in-php', 'evolution-of-a-php-object', 'what-a-good-pr-looks-like', 'honesty', 'starting-a-podcast', 'php-reimagined-part-2', 'php-2026', 'typed-properties-in-php-74', 'announcing-aggregate', 'goodbye', 'simplest-plugin-support', 'phpstorm-tips-for-power-users', 'stitcher-turns-5', 'array-merge-vs + ', 'comparing-dates', 'php-81-new-in-initializers', 'acronyms', 'my-wishlist-for-php-in-2026', 'php-84-at-least', 'object-oriented-generators', 'visual-perception-of-code', 'things-i-learned-writing-a-fiction-novel', 'shorthand-comparisons-in-php', 'attribute-usage-in-top-php-packages', 'image_optimizers', 'php-in-2019', 'not-optional', 'stitcher-beta-1', 'php-version-stats-july-2022', 'laravel-custom-relation-classes', 'deprecated-dynamic-properties-in-php-82', 'open-source-strategies', 'slashdash', 'laravel-has-many-through', 'php-in-2022', 'deprecating-spatie-dto', 'light-colour-schemes-are-better', 'tackling_responsive_images-part_1', 'merging-multidimensional-arrays-in-php', 'php-version-stats-july-2021', 'php-8-before-and-after', 'the-road-to-php', 'stitcher-beta-2', 'generics-in-php-4', 'combining-event-sourcing-and-stateful-systems', 'array-objects-with-fixed-types', 'override-in-php-83', 'php-74-upgrade-mac', 'php-8-upgrade-mac', 'testing-patterns', 'array-destructuring-with-list-in-php', 'php-73-upgrade-mac', 'when-i-lost-a-few-hundred-leads', 'is-a-or-acts-as', 'unsafe-sql-functions-in-laravel', 'php-81-readonly-properties', 'php-81-before-and-after', 'improvements-on-laravel-nova', 'thank-you-kinsta', 'php-version-stats-june-2025', 'new-in-php-74', 'type-system-in-php-survey', 'readonly-or-private-set', 'share-a-blog-betterwebtype-com', 'laravel-view-models-vs-view-composers', 'tests-and-types', 'clean-and-minimalistic-phpstorm', 'processing-11-million-rows', 'tabs-are-better', 'phpstorm-performance-october-2018', 'static-websites-with-tempest', 'php-in-2023', 'abstract-resources-in-laravel-nova', 'a-letter-to-the-php-team-reply-to-joe', 'procedurally-generated-game-in-php', 'dealing-with-dependencies', 'the-web-in-2045', 'whats-your-motivator', 'thoughts-on-asymmetric-visibility', 'new-in-php-81', 'route-attributes', 'builders-and-architects-two-types-of-programmers', 'cloning-readonly-properties-in-php-83'];
        $slugBaseMap = [];
        for ($pi = 0; $pi < 268; $pi++) {
            $slugBaseMap[$paths[$pi]] = $pi * 2191;
        }

        $boundaries = self::PRECOMPUTED_BOUNDARIES;

        $sockets = [];

        for ($w = 0; $w < self::WORKER_COUNT; $w++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_chunk_size($pair[0], self::COUNTS_SIZE);
            stream_set_chunk_size($pair[1], self::COUNTS_SIZE);
            $pid = pcntl_fork();
            if ($pid === 0) {
                fclose($pair[0]);
                $output = self::parseRange(
                    $inputPath, $boundaries[$w], $boundaries[$w + 1],
                    $slugBaseMap, $dateIds, $next,
                );
                fwrite($pair[1], $output);
                fclose($pair[1]);
                exit(0);
            }
            fclose($pair[1]);
            $sockets[$w] = $pair[0];
        }

        $counts = array_fill(0, self::COUNTS_SIZE, 0);
        $pending = [];

        while ($sockets !== []) {
            $read = $sockets;
            $write = [];
            $except = [];
            stream_select($read, $write, $except, 5);
            foreach ($read as $key => $socket) {
                $data = fread($socket, self::COUNTS_SIZE);
                if ($data !== '' && $data !== false) {
                    $pending[$key] = ($pending[$key] ?? '') . $data;
                }
                if (feof($socket)) {
                    fclose($socket);
                    unset($sockets[$key]);
                    $j = 0;
                    foreach (unpack('C*', $pending[$key] ?? '') as $v) {
                        $counts[$j] += $v;
                        $j++;
                    }
                    unset($pending[$key]);
                }
            }
        }

        self::writeJson($outputPath, $counts, $paths, $dates);
    }

    private static function parseRange(
        $inputPath, $start, $end,
        $slugBaseMap, $dateIds, $next,
    ) {
        $output = str_repeat(chr(0), self::COUNTS_SIZE);
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $toRead = $remaining > self::READ_CHUNK ? self::READ_CHUNK : $remaining;
            $chunk = fread($handle, $toRead);
            $chunkLen = strlen($chunk);
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) break;

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $p = 25;
            $fence = $lastNl - 1010;

            while ($p < $fence) {
                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
            }

            while ($p < $lastNl) {
                $sep = strpos($chunk, ',', $p);
                if ($sep === false || $sep >= $lastNl) break;
                $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 3, 8)];
                $output[$idx] = $next[$output[$idx]];
                $p = $sep + 52;
            }
        }

        fclose($handle);

        return $output;
    }

    private static function writeJson(
        $outputPath, $counts, $paths, $dates,
    ) {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);
        fwrite($out, '{');

        $datePrefixes = [];
        for ($d = 0; $d < 2191; $d++) {
            $datePrefixes[$d] = '        "20' . $dates[$d] . '": ';
        }

        $escapedPaths = [];
        for ($p = 0; $p < 268; $p++) {
            $escapedPaths[$p] = "\"\\/blog\\/" . str_replace('/', '\\/', $paths[$p]) . "\"";
        }

        $firstPath = true;

        for ($p = 0; $p < 268; $p++) {
            $base = $p * 2191;
            $firstDate = -1;
            for ($d = 0; $d < 2191; $d++) {
                if ($counts[$base + $d] !== 0) {
                    $firstDate = $d;
                    break;
                }
            }

            if ($firstDate === -1) continue;

            $buf = $firstPath ? "\n    " : ",\n    ";
            $firstPath = false;
            $buf .= $escapedPaths[$p] . ": {\n";
            $buf .= $datePrefixes[$firstDate] . $counts[$base + $firstDate];

            for ($d = $firstDate + 1; $d < 2191; $d++) {
                $count = $counts[$base + $d];
                if ($count === 0) continue;
                $buf .= ",\n" . $datePrefixes[$d] . $count;
            }

            $buf .= "\n    }";
            fwrite($out, $buf);
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
