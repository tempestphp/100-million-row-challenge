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
use const SEEK_END;
use const STREAM_IPPROTO_IP;
use const STREAM_PF_UNIX;
use const STREAM_SOCK_STREAM;

final class Parser
{
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

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $raw = fread($handle, 2_097_152);
        fclose($handle);

        $paths = [];
        $slugBaseMap = [];
        $slugTotal = 0;
        $pos = 0;
        $lastNl = strrpos($raw, "\n") ?: 0;

        while ($pos < $lastNl) {
            $nl = strpos($raw, "\n", $pos + 52);
            if ($nl === false) break;
            $slug = substr($raw, $pos + 25, $nl - $pos - 51);
            if (!isset($slugBaseMap[$slug])) {
                $paths[$slugTotal] = $slug;
                $slugBaseMap[$slug] = $slugTotal * $di;
                $slugTotal++;
            }
            $pos = $nl + 1;
        }
        unset($raw);

        $outputSize = $slugTotal * $di;

        $bh = fopen($inputPath, 'rb');
        fseek($bh, 0, SEEK_END);
        $fileSize = ftell($bh);
        $step = \intdiv($fileSize, 8);
        $boundaries = [0];
        for ($i = 1; $i < 8; $i++) {
            fseek($bh, $step * $i);
            fgets($bh);
            $boundaries[] = ftell($bh);
        }
        fclose($bh);
        $boundaries[] = $fileSize;

        $sockets = [];

        for ($w = 0; $w < 8; $w++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_chunk_size($pair[0], $outputSize);
            stream_set_chunk_size($pair[1], $outputSize);
            $pid = pcntl_fork();
            if ($pid === 0) {
                $output = self::parseRange(
                    $inputPath, $boundaries[$w], $boundaries[$w + 1],
                    $slugBaseMap, $dateIds, $next, $outputSize,
                );
                fwrite($pair[1], $output);
                exit(0);
            }
            fclose($pair[1]);
            $sockets[$w] = $pair[0];
        }

        $counts = array_fill(0, $outputSize, 0);
        $pending = [];

        while ($sockets !== []) {
            $read = $sockets;
            $write = [];
            $except = [];
            stream_select($read, $write, $except, 5);
            foreach ($read as $key => $socket) {
                $data = fread($socket, $outputSize);
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

        self::writeJson($outputPath, $counts, $paths, $dates, $di, $slugTotal);
    }

    private static function parseRange(
        $inputPath, $start, $end,
        $slugBaseMap, $dateIds, $next, $outputSize,
    ) {
        $output = str_repeat(chr(0), $outputSize);
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $toRead = $remaining > 163_840 ? 163_840 : $remaining;
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
        $outputPath, $counts, $paths, $dates, $dateCount, $slugCount,
    ) {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);
        fwrite($out, '{');

        $datePrefixes = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "20' . $dates[$d] . '": ';
        }


        $escapedPaths = [
            '\/blog\/stitcher-alpha-5', '\/blog\/its-your-fault', '\/blog\/dont-get-stuck', '\/blog\/attributes-in-php-8', '\/blog\/game-changing-editions', '\/blog\/php-preload-benchmarks', '\/blog\/why-we-need-named-params-in-php', '\/blog\/minor-versions-breaking-changes', '\/blog\/preloading-in-php-74', '\/blog\/php-81-upgrade-mac', '\/blog\/opinion-driven-design', '\/blog\/rational-thinking', '\/blog\/php-81-performance-in-real-life', '\/blog\/php-enums-before-php-81', '\/blog\/php-version-stats-january-2023', '\/blog\/all-i-want-for-christmas', '\/blog\/php-81-in-8-code-blocks', '\/blog\/impact-charts', '\/blog\/solid-interfaces-and-final-rant-with-brent', '\/blog\/new-in-php-84', '\/blog\/a-syntax-highlighter-that-doesnt-suck', '\/blog\/jit-in-real-life-web-applications', '\/blog\/dont-write-your-own-framework', '\/blog\/websites-like-star-wars', '\/blog\/you-should', '\/blog\/php-reimagined', '\/blog\/php-version-stats-july-2023', '\/blog\/tackling_responsive_images-part_2', '\/blog\/array-find-in-php-84', '\/blog\/phpstorm-performance-issues-on-osx', '\/blog\/flooded-rss', '\/blog\/what-is-array-plus-in-php', '\/blog\/phpstorm-scopes', '\/blog\/guest-posts', '\/blog\/php-82-in-8-code-blocks', '\/blog\/braille-and-the-history-of-software', '\/blog\/things-considered-harmful', '\/blog\/strategies', '\/blog\/dont-be-clever', '\/blog\/structuring-unstructured-data', '\/blog\/upgrading-to-php-82', '\/blog\/event-driven-php', '\/blog\/theoretical-engineers', '\/blog\/birth-and-death-of-a-framework', '\/blog\/rfc-vote', '\/blog\/improved-lazy-loading', '\/blog\/why-we-need-multi-line-short-closures-in-php', '\/blog\/php-in-2021-video', '\/blog\/my-ikea-clock', '\/blog\/a-project-at-spatie', '\/blog\/php-8-nullsafe-operator', '\/blog\/my-journey-into-event-sourcing', '\/blog\/responsive-images-as-css-background', '\/blog\/php-version-stats-july-2024', '\/blog\/acquisition-by-giants', '\/blog\/the-framework-that-gets-out-of-your-way', '\/blog\/laravel-queueable-actions', '\/blog\/an-event-driven-mindset', '\/blog\/we-dont-need-runtime-type-checks', '\/blog\/how-i-plan', '\/blog\/why-light-themes-are-better-according-to-science', '\/blog\/new-in-php-85', '\/blog\/have-you-thought-about-casing', '\/blog\/php-version-stats-january-2024', '\/blog\/i-dont-code-the-way-i-used-to', '\/blog\/performance-101-building-the-better-web', '\/blog\/tempest-discovery-explained', '\/blog\/array-chunk-in-php', '\/blog\/front-line-php', '\/blog\/phpstorm-performance', '\/blog\/a-letter-to-the-php-team', '\/blog\/generics-in-php-2', '\/blog\/dependency-injection-for-beginners', '\/blog\/process-forks', '\/blog\/php-8-in-8-code-blocks', '\/blog\/a-new-major-version-of-laravel-event-sourcing', '\/blog\/named-arguments-and-variadic-functions', '\/blog\/php-version-stats-january-2025', '\/blog\/share-a-blog-assertchris-io', '\/blog\/ai-induced-skepticism', '\/blog\/can-i-translate-your-blog', '\/blog\/starting-a-newsletter', '\/blog\/things-i-wish-i-knew', '\/blog\/re-on-using-psr-abstractions', '\/blog\/static_sites_vs_caching', '\/blog\/how-to-be-right-on-the-internet', '\/blog\/php-annotated', '\/blog\/mysql-show-foreign-key-errors', '\/blog\/share-a-blog-sebastiandedeyne-com', '\/blog\/mysql-import-json-binary-character-set', '\/blog\/thoughts-on-event-sourcing', '\/blog\/differences', '\/blog\/php-jit', '\/blog\/php-74-in-7-code-blocks', '\/blog\/mysql-query-logging', '\/blog\/generics-in-php-1', '\/blog\/uncertainty-doubt-and-static-analysis', '\/blog\/new-in-php-73', '\/blog\/code-folding', '\/blog\/uses', '\/blog\/eloquent-mysql-views', '\/blog\/pipe-operator-in-php-85', '\/blog\/twitter-home-made-me-miserable', '\/blog\/why-curly-brackets-go-on-new-lines', '\/blog\/type-system-in-php-survey-results', '\/blog\/twitter-exit', '\/blog\/the-ikea-effect', '\/blog\/a-year-of-property-hooks', '\/blog\/which-editor-to-choose', '\/blog\/craftsmen-know-their-tools', '\/blog\/parallel-php', '\/blog\/php-in-2020', '\/blog\/a-simple-approach-to-static-generation', '\/blog\/what-about-config-builders', '\/blog\/road-to-php-82', '\/blog\/fibers-with-a-grain-of-salt', '\/blog\/cloning-readonly-properties-in-php-81', '\/blog\/why-do-i-write', '\/blog\/share-a-blog-codingwriter-com', '\/blog\/a-storm-in-a-glass-of-water', '\/blog\/where-a-curly-bracket-belongs', '\/blog\/my-10-favourite-php-functions', '\/blog\/new-with-parentheses-php-84', '\/blog\/unfair-advantage', '\/blog\/extends-vs-implements', '\/blog\/php-in-2024', '\/blog\/new-in-php-83', '\/blog\/the-case-for-transpiled-generics', '\/blog\/i-dont-know', '\/blog\/what-are-objects-anyway-rant-with-brent', '\/blog\/building-a-custom-language-in-tempest-highlight', '\/blog\/analytics-for-developers', '\/blog\/enums-without-enums', '\/blog\/optimistic-or-realistic-estimates', '\/blog\/php-version-stats-january-2022', '\/blog\/passion-projects', '\/blog\/vendor-locked', '\/blog\/php-verse-2025', '\/blog\/limited-by-committee', '\/blog\/constructor-promotion-in-php-8', '\/blog\/building-a-framework', '\/blog\/service-locator-anti-pattern', '\/blog\/tagging-tempest-livestream', '\/blog\/bitwise-booleans-in-php', '\/blog\/a-programmers-cognitive-load', '\/blog\/dealing-with-deprecations', '\/blog\/new-in-php-82', '\/blog\/generics-in-php-3', '\/blog\/annotations', '\/blog\/php-enums', '\/blog\/you-cannot-find-me-on-mastodon', '\/blog\/sponsoring-open-source', '\/blog\/request-objects-in-tempest', '\/blog\/php-82-upgrade-mac', '\/blog\/new-in-php-8', '\/blog\/php-performance-across-versions', '\/blog\/stitcher-alpha-4', '\/blog\/php-in-2021', '\/blog\/11-million-rows-in-seconds', '\/blog\/php-86-partial-function-application', '\/blog\/things-dependency-injection-is-not-about', '\/blog\/generics-in-php-video', '\/blog\/organise-by-domain', '\/blog\/sponsors', '\/blog\/liskov-and-type-safety', '\/blog\/laravel-view-models', '\/blog\/what-about-request-classes', '\/blog\/reducing-code-motion', '\/blog\/php-8-jit-setup', '\/blog\/php-8-named-arguments', '\/blog\/asynchronous-php', '\/blog\/tagged-singletons', '\/blog\/php-enum-style-guide', '\/blog\/optimised-uuids-in-mysql', '\/blog\/its-all-just-text', '\/blog\/what-php-can-be', '\/blog\/a-vocal-minority', '\/blog\/php-8-match-or-switch', '\/blog\/html-5-in-php-84', '\/blog\/php-generics-and-why-we-need-them', '\/blog\/light-colour-schemes', '\/blog\/mastering-key-bindings', '\/blog\/readonly-classes-in-php-82', '\/blog\/what-event-sourcing-is-not-about', '\/blog\/responsive-images-done-right', '\/blog\/short-closures-in-php', '\/blog\/evolution-of-a-php-object', '\/blog\/what-a-good-pr-looks-like', '\/blog\/honesty', '\/blog\/starting-a-podcast', '\/blog\/php-reimagined-part-2', '\/blog\/php-2026', '\/blog\/typed-properties-in-php-74', '\/blog\/announcing-aggregate', '\/blog\/goodbye', '\/blog\/simplest-plugin-support', '\/blog\/phpstorm-tips-for-power-users', '\/blog\/stitcher-turns-5', '\/blog\/array-merge-vs + ', '\/blog\/comparing-dates', '\/blog\/php-81-new-in-initializers', '\/blog\/acronyms', '\/blog\/my-wishlist-for-php-in-2026', '\/blog\/php-84-at-least', '\/blog\/object-oriented-generators', '\/blog\/visual-perception-of-code', '\/blog\/things-i-learned-writing-a-fiction-novel', '\/blog\/shorthand-comparisons-in-php', '\/blog\/attribute-usage-in-top-php-packages', '\/blog\/image_optimizers', '\/blog\/php-in-2019', '\/blog\/not-optional', '\/blog\/stitcher-beta-1', '\/blog\/php-version-stats-july-2022', '\/blog\/laravel-custom-relation-classes', '\/blog\/deprecated-dynamic-properties-in-php-82', '\/blog\/open-source-strategies', '\/blog\/slashdash', '\/blog\/laravel-has-many-through', '\/blog\/php-in-2022', '\/blog\/deprecating-spatie-dto', '\/blog\/light-colour-schemes-are-better', '\/blog\/tackling_responsive_images-part_1', '\/blog\/merging-multidimensional-arrays-in-php', '\/blog\/php-version-stats-july-2021', '\/blog\/php-8-before-and-after', '\/blog\/the-road-to-php', '\/blog\/stitcher-beta-2', '\/blog\/generics-in-php-4', '\/blog\/combining-event-sourcing-and-stateful-systems', '\/blog\/array-objects-with-fixed-types', '\/blog\/override-in-php-83', '\/blog\/php-74-upgrade-mac', '\/blog\/php-8-upgrade-mac', '\/blog\/testing-patterns', '\/blog\/array-destructuring-with-list-in-php', '\/blog\/php-73-upgrade-mac', '\/blog\/when-i-lost-a-few-hundred-leads', '\/blog\/is-a-or-acts-as', '\/blog\/unsafe-sql-functions-in-laravel', '\/blog\/php-81-readonly-properties', '\/blog\/php-81-before-and-after', '\/blog\/improvements-on-laravel-nova', '\/blog\/thank-you-kinsta', '\/blog\/php-version-stats-june-2025', '\/blog\/new-in-php-74', '\/blog\/type-system-in-php-survey', '\/blog\/readonly-or-private-set', '\/blog\/share-a-blog-betterwebtype-com', '\/blog\/laravel-view-models-vs-view-composers', '\/blog\/tests-and-types', '\/blog\/clean-and-minimalistic-phpstorm', '\/blog\/processing-11-million-rows', '\/blog\/tabs-are-better', '\/blog\/phpstorm-performance-october-2018', '\/blog\/static-websites-with-tempest', '\/blog\/php-in-2023', '\/blog\/abstract-resources-in-laravel-nova', '\/blog\/a-letter-to-the-php-team-reply-to-joe', '\/blog\/procedurally-generated-game-in-php', '\/blog\/dealing-with-dependencies', '\/blog\/the-web-in-2045', '\/blog\/whats-your-motivator', '\/blog\/thoughts-on-asymmetric-visibility', '\/blog\/new-in-php-81', '\/blog\/route-attributes', '\/blog\/builders-and-architects-two-types-of-programmers', '\/blog\/cloning-readonly-properties-in-php-83'
        ];

        $firstPath = true;

        for ($p = 0; $p < $slugCount; $p++) {
            $base = $p * $dateCount;
            $firstDate = -1;
            for ($d = 0; $d < $dateCount; $d++) {
                if ($counts[$base + $d] !== 0) {
                    $firstDate = $d;
                    break;
                }
            }

            if ($firstDate === -1) continue;

            $buf = $firstPath ? "\n    " : ",\n    ";
            $firstPath = false;
            $buf .= '"' . $escapedPaths[$p] . "\": {\n" . $datePrefixes[$firstDate] . $counts[$base + $firstDate];

            for ($d = $firstDate + 1; $d < $dateCount; $d++) {
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
