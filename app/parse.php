<?php

namespace App;

function parse($i, $o)
{
    ini_set('memory_limit', '4G');
    gc_disable();

    $fileSize = filesize($i);

    $knownPaths = ['/blog/which-editor-to-choose','/blog/tackling_responsive_images-part_1','/blog/tackling_responsive_images-part_2','/blog/image_optimizers','/blog/static_sites_vs_caching','/blog/stitcher-alpha-4','/blog/simplest-plugin-support','/blog/stitcher-alpha-5','/blog/php-generics-and-why-we-need-them','/blog/stitcher-beta-1','/blog/array-objects-with-fixed-types','/blog/performance-101-building-the-better-web','/blog/process-forks','/blog/object-oriented-generators','/blog/responsive-images-as-css-background','/blog/a-programmers-cognitive-load','/blog/mastering-key-bindings','/blog/stitcher-beta-2','/blog/phpstorm-performance','/blog/optimised-uuids-in-mysql','/blog/asynchronous-php','/blog/mysql-import-json-binary-character-set','/blog/where-a-curly-bracket-belongs','/blog/mysql-query-logging','/blog/mysql-show-foreign-key-errors','/blog/responsive-images-done-right','/blog/phpstorm-tips-for-power-users','/blog/what-php-can-be','/blog/phpstorm-performance-issues-on-osx','/blog/dependency-injection-for-beginners','/blog/liskov-and-type-safety','/blog/acquisition-by-giants','/blog/visual-perception-of-code','/blog/service-locator-anti-pattern','/blog/the-web-in-2045','/blog/eloquent-mysql-views','/blog/laravel-view-models','/blog/laravel-view-models-vs-view-composers','/blog/organise-by-domain','/blog/array-merge-vs + ','/blog/share-a-blog-assertchris-io','/blog/phpstorm-performance-october-2018','/blog/structuring-unstructured-data','/blog/share-a-blog-codingwriter-com','/blog/new-in-php-73','/blog/share-a-blog-betterwebtype-com','/blog/have-you-thought-about-casing','/blog/comparing-dates','/blog/share-a-blog-sebastiandedeyne-com','/blog/analytics-for-developers','/blog/announcing-aggregate','/blog/php-jit','/blog/craftsmen-know-their-tools','/blog/laravel-queueable-actions','/blog/php-73-upgrade-mac','/blog/array-destructuring-with-list-in-php','/blog/unsafe-sql-functions-in-laravel','/blog/starting-a-newsletter','/blog/short-closures-in-php','/blog/solid-interfaces-and-final-rant-with-brent','/blog/php-in-2019','/blog/starting-a-podcast','/blog/a-project-at-spatie','/blog/what-are-objects-anyway-rant-with-brent','/blog/tests-and-types','/blog/typed-properties-in-php-74','/blog/preloading-in-php-74','/blog/things-dependency-injection-is-not-about','/blog/a-letter-to-the-php-team','/blog/a-letter-to-the-php-team-reply-to-joe','/blog/guest-posts','/blog/can-i-translate-your-blog','/blog/laravel-has-many-through','/blog/laravel-custom-relation-classes','/blog/new-in-php-74','/blog/php-74-upgrade-mac','/blog/php-preload-benchmarks','/blog/php-in-2020','/blog/enums-without-enums','/blog/bitwise-booleans-in-php','/blog/event-driven-php','/blog/minor-versions-breaking-changes','/blog/combining-event-sourcing-and-stateful-systems','/blog/array-chunk-in-php','/blog/php-8-in-8-code-blocks','/blog/builders-and-architects-two-types-of-programmers','/blog/the-ikea-effect','/blog/php-74-in-7-code-blocks','/blog/improvements-on-laravel-nova','/blog/type-system-in-php-survey','/blog/merging-multidimensional-arrays-in-php','/blog/what-is-array-plus-in-php','/blog/type-system-in-php-survey-results','/blog/constructor-promotion-in-php-8','/blog/abstract-resources-in-laravel-nova','/blog/braille-and-the-history-of-software','/blog/jit-in-real-life-web-applications','/blog/php-8-match-or-switch','/blog/why-we-need-named-params-in-php','/blog/shorthand-comparisons-in-php','/blog/php-8-before-and-after','/blog/php-8-named-arguments','/blog/my-journey-into-event-sourcing','/blog/differences','/blog/annotations','/blog/dont-get-stuck','/blog/attributes-in-php-8','/blog/the-case-for-transpiled-generics','/blog/phpstorm-scopes','/blog/why-light-themes-are-better-according-to-science','/blog/what-a-good-pr-looks-like','/blog/front-line-php','/blog/php-8-jit-setup','/blog/php-8-nullsafe-operator','/blog/new-in-php-8','/blog/php-8-upgrade-mac','/blog/when-i-lost-a-few-hundred-leads','/blog/websites-like-star-wars','/blog/php-reimagined','/blog/a-storm-in-a-glass-of-water','/blog/php-enums-before-php-81','/blog/php-enums','/blog/dont-write-your-own-framework','/blog/honesty','/blog/thoughts-on-event-sourcing','/blog/what-event-sourcing-is-not-about','/blog/fibers-with-a-grain-of-salt','/blog/php-in-2021','/blog/parallel-php','/blog/why-we-need-multi-line-short-closures-in-php','/blog/a-new-major-version-of-laravel-event-sourcing','/blog/what-about-config-builders','/blog/opinion-driven-design','/blog/php-version-stats-july-2021','/blog/what-about-request-classes','/blog/cloning-readonly-properties-in-php-81','/blog/an-event-driven-mindset','/blog/php-81-before-and-after','/blog/optimistic-or-realistic-estimates','/blog/we-dont-need-runtime-type-checks','/blog/the-road-to-php','/blog/why-do-i-write','/blog/rational-thinking','/blog/named-arguments-and-variadic-functions','/blog/re-on-using-psr-abstractions','/blog/my-ikea-clock','/blog/php-81-readonly-properties','/blog/birth-and-death-of-a-framework','/blog/php-81-new-in-initializers','/blog/route-attributes','/blog/generics-in-php-video','/blog/php-81-in-8-code-blocks','/blog/new-in-php-81','/blog/php-81-performance-in-real-life','/blog/php-81-upgrade-mac','/blog/how-to-be-right-on-the-internet','/blog/php-version-stats-january-2022','/blog/php-in-2022','/blog/how-i-plan','/blog/twitter-home-made-me-miserable','/blog/its-your-fault','/blog/dealing-with-dependencies','/blog/php-in-2021-video','/blog/generics-in-php-1','/blog/generics-in-php-2','/blog/generics-in-php-3','/blog/generics-in-php-4','/blog/goodbye','/blog/strategies','/blog/dealing-with-deprecations','/blog/attribute-usage-in-top-php-packages','/blog/php-enum-style-guide','/blog/clean-and-minimalistic-phpstorm','/blog/stitcher-turns-5','/blog/php-version-stats-july-2022','/blog/evolution-of-a-php-object','/blog/uncertainty-doubt-and-static-analysis','/blog/road-to-php-82','/blog/php-performance-across-versions','/blog/light-colour-schemes-are-better','/blog/deprecated-dynamic-properties-in-php-82','/blog/php-reimagined-part-2','/blog/thoughts-on-asymmetric-visibility','/blog/uses','/blog/php-82-in-8-code-blocks','/blog/readonly-classes-in-php-82','/blog/deprecating-spatie-dto','/blog/php-82-upgrade-mac','/blog/php-annotated','/blog/you-cannot-find-me-on-mastodon','/blog/new-in-php-82','/blog/all-i-want-for-christmas','/blog/upgrading-to-php-82','/blog/php-version-stats-january-2023','/blog/php-in-2023','/blog/tabs-are-better','/blog/sponsors','/blog/why-curly-brackets-go-on-new-lines','/blog/my-10-favourite-php-functions','/blog/acronyms','/blog/code-folding','/blog/light-colour-schemes','/blog/slashdash','/blog/thank-you-kinsta','/blog/cloning-readonly-properties-in-php-83','/blog/limited-by-committee','/blog/things-considered-harmful','/blog/procedurally-generated-game-in-php','/blog/dont-be-clever','/blog/override-in-php-83','/blog/php-version-stats-july-2023','/blog/is-a-or-acts-as','/blog/rfc-vote','/blog/new-in-php-83','/blog/i-dont-know','/blog/passion-projects','/blog/php-version-stats-january-2024','/blog/the-framework-that-gets-out-of-your-way','/blog/a-syntax-highlighter-that-doesnt-suck','/blog/building-a-custom-language-in-tempest-highlight','/blog/testing-patterns','/blog/php-in-2024','/blog/tagged-singletons','/blog/twitter-exit','/blog/a-vocal-minority','/blog/php-version-stats-july-2024','/blog/you-should','/blog/new-with-parentheses-php-84','/blog/html-5-in-php-84','/blog/array-find-in-php-84','/blog/its-all-just-text','/blog/improved-lazy-loading','/blog/i-dont-code-the-way-i-used-to','/blog/php-84-at-least','/blog/extends-vs-implements','/blog/a-simple-approach-to-static-generation','/blog/building-a-framework','/blog/tagging-tempest-livestream','/blog/things-i-learned-writing-a-fiction-novel','/blog/unfair-advantage','/blog/new-in-php-84','/blog/php-version-stats-january-2025','/blog/theoretical-engineers','/blog/static-websites-with-tempest','/blog/request-objects-in-tempest','/blog/php-verse-2025','/blog/tempest-discovery-explained','/blog/php-version-stats-june-2025','/blog/pipe-operator-in-php-85','/blog/a-year-of-property-hooks','/blog/readonly-or-private-set','/blog/things-i-wish-i-knew','/blog/impact-charts','/blog/whats-your-motivator','/blog/vendor-locked','/blog/reducing-code-motion','/blog/sponsoring-open-source','/blog/my-wishlist-for-php-in-2026','/blog/game-changing-editions','/blog/new-in-php-85','/blog/flooded-rss','/blog/php-2026','/blog/open-source-strategies','/blog/not-optional','/blog/processing-11-million-rows','/blog/ai-induced-skepticism','/blog/php-86-partial-function-application','/blog/11-million-rows-in-seconds'];

    if ($fileSize >= 10485760) {
        $pathIds = array_flip($knownPaths);
        $pathList = $knownPaths;
        $pathCount = 268;
    } else {
        $handle = fopen($i, 'rb');
        $discoverChunk = fread($handle, $fileSize);
        fclose($handle);

        $pathIds = [];
        $pathList = [];
        $pathCount = 0;

        $pos = 0;
        while (($nlPos = strpos($discoverChunk, "\n", $pos)) !== false) {
            $path = substr($discoverChunk, $pos + 19, $nlPos - $pos - 45);
            if (!isset($pathIds[$path])) {
                $pathIds[$path] = $pathCount;
                $pathList[$pathCount] = $path;
                $pathCount++;
            }
            $pos = $nlPos + 1;
        }
        unset($discoverChunk);

        foreach ($knownPaths as $path) {
            if (!isset($pathIds[$path])) {
                $pathIds[$path] = $pathCount;
                $pathList[$pathCount] = $path;
                $pathCount++;
            }
        }
    }

    $dateIds = [];
    $dateList = [];
    $dateCount = 0;
    $dateIdChars = [];
    for ($year = 20; $year <= 26; $year++) {
        for ($month = 1; $month <= 12; $month++) {
            $maxDay = ($month === 2) ? (($year % 4 === 0) ? 29 : 28) : (($month === 4 || $month === 6 || $month === 9 || $month === 11) ? 30 : 31);
            for ($day = 1; $day <= $maxDay; $day++) {
                $date8 = sprintf('%02d-%02d-%02d', $year, $month, $day);
                $dateIds[$date8] = $dateCount;
                $dateList[$dateCount] = '20' . $date8;
                $dateIdChars[$date8] = chr($dateCount & 0xFF) . chr($dateCount >> 8);
                $dateCount++;
            }
        }
    }

    $stride = $dateCount;
    $totalCells = $pathCount * $stride;
    $chunkSize = 262144;

    if ($fileSize >= 10485760) {
        $ncpu = PHP_OS_FAMILY === 'Darwin'
            ? (int)trim(shell_exec('sysctl -n hw.ncpu'))
            : (int)(trim(shell_exec('nproc 2>/dev/null') ?: '8'));
        $numWorkers = $ncpu + 4;

        $handle = fopen($i, 'rb');
        $splits = [0];
        for ($s = 1; $s < $numWorkers; $s++) {
            fseek($handle, (int)($fileSize * $s / $numWorkers));
            fgets($handle);
            $splits[] = ftell($handle);
        }
        $splits[] = $fileSize;
        fclose($handle);

        $tmpDir = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
        $tmpPrefix = $tmpDir . '/p_' . getmypid() . '_';

        $childPids = [];
        for ($w = 1; $w < $numWorkers; $w++) {
            $pid = pcntl_fork();
            if ($pid === -1) continue;
            if ($pid === 0) {
                $handle = fopen($i, 'rb');
                stream_set_read_buffer($handle, 0);
                $buckets = array_fill(0, $pathCount, '');

                fseek($handle, $splits[$w]);
                $remaining = $splits[$w + 1] - $splits[$w];

                while ($remaining > 0) {
                    $toRead = $remaining < $chunkSize ? $remaining : $chunkSize;
                    $chunk = fread($handle, $toRead);
                    if ($chunk === false || $chunk === '') break;
                    $chunkLen = strlen($chunk);
                    $remaining -= $chunkLen;

                    $lastNl = strrpos($chunk, "\n");
                    if ($lastNl === false) continue;

                    $tail = $chunkLen - $lastNl - 1;
                    if ($tail > 0) {
                        fseek($handle, -$tail, SEEK_CUR);
                        $remaining += $tail;
                    }

                    $fence = $lastNl - 720;
                    $pos = 0;
                    while ($pos < $fence) {
                        $nlPos = strpos($chunk, "\n", $pos + 54);
                        $buckets[$pathIds[substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[substr($chunk, $nlPos - 23, 8)];
                        $pos = $nlPos + 1;
                        $nlPos = strpos($chunk, "\n", $pos + 54);
                        $buckets[$pathIds[substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[substr($chunk, $nlPos - 23, 8)];
                        $pos = $nlPos + 1;
                        $nlPos = strpos($chunk, "\n", $pos + 54);
                        $buckets[$pathIds[substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[substr($chunk, $nlPos - 23, 8)];
                        $pos = $nlPos + 1;
                        $nlPos = strpos($chunk, "\n", $pos + 54);
                        $buckets[$pathIds[substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[substr($chunk, $nlPos - 23, 8)];
                        $pos = $nlPos + 1;
                        $nlPos = strpos($chunk, "\n", $pos + 54);
                        $buckets[$pathIds[substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[substr($chunk, $nlPos - 23, 8)];
                        $pos = $nlPos + 1;
                        $nlPos = strpos($chunk, "\n", $pos + 54);
                        $buckets[$pathIds[substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[substr($chunk, $nlPos - 23, 8)];
                        $pos = $nlPos + 1;
                    }
                    while ($pos < $lastNl) {
                        $nlPos = strpos($chunk, "\n", $pos + 54);
                        if ($nlPos === false || $nlPos > $lastNl) break;
                        $buckets[$pathIds[substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[substr($chunk, $nlPos - 23, 8)];
                        $pos = $nlPos + 1;
                    }
                }
                fclose($handle);

                $counts = array_fill(0, $totalCells, 0);
                for ($p = 0; $p < $pathCount; $p++) {
                    if ($buckets[$p] === '') continue;
                    $offset = $p * $stride;
                    foreach (array_count_values(unpack('v*', $buckets[$p])) as $did => $cnt) {
                        $counts[$offset + $did] += $cnt;
                    }
                }
                file_put_contents($tmpPrefix . $w, pack('V*', ...$counts));
                exit(0);
            }
            $childPids[$w] = $pid;
        }

        $handle = fopen($i, 'rb');
        stream_set_read_buffer($handle, 0);
        $buckets = array_fill(0, $pathCount, '');
        $remaining = $splits[1];

        while ($remaining > 0) {
            $toRead = $remaining < $chunkSize ? $remaining : $chunkSize;
            $chunk = fread($handle, $toRead);
            if ($chunk === false || $chunk === '') break;
            $chunkLen = strlen($chunk);
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) continue;

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $fence = $lastNl - 720;
            $pos = 0;
            while ($pos < $fence) {
                $nlPos = strpos($chunk, "\n", $pos + 54);
                $buckets[$pathIds[substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[substr($chunk, $nlPos - 23, 8)];
                $pos = $nlPos + 1;
                $nlPos = strpos($chunk, "\n", $pos + 54);
                $buckets[$pathIds[substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[substr($chunk, $nlPos - 23, 8)];
                $pos = $nlPos + 1;
                $nlPos = strpos($chunk, "\n", $pos + 54);
                $buckets[$pathIds[substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[substr($chunk, $nlPos - 23, 8)];
                $pos = $nlPos + 1;
                $nlPos = strpos($chunk, "\n", $pos + 54);
                $buckets[$pathIds[substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[substr($chunk, $nlPos - 23, 8)];
                $pos = $nlPos + 1;
                $nlPos = strpos($chunk, "\n", $pos + 54);
                $buckets[$pathIds[substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[substr($chunk, $nlPos - 23, 8)];
                $pos = $nlPos + 1;
                $nlPos = strpos($chunk, "\n", $pos + 54);
                $buckets[$pathIds[substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[substr($chunk, $nlPos - 23, 8)];
                $pos = $nlPos + 1;
            }
            while ($pos < $lastNl) {
                $nlPos = strpos($chunk, "\n", $pos + 54);
                if ($nlPos === false || $nlPos > $lastNl) break;
                $buckets[$pathIds[substr($chunk, $pos + 19, $nlPos - $pos - 45)]] .= $dateIdChars[substr($chunk, $nlPos - 23, 8)];
                $pos = $nlPos + 1;
            }
        }
        fclose($handle);

        $counts = array_fill(0, $totalCells, 0);
        for ($p = 0; $p < $pathCount; $p++) {
            if ($buckets[$p] === '') continue;
            $offset = $p * $stride;
            foreach (array_count_values(unpack('v*', $buckets[$p])) as $did => $cnt) {
                $counts[$offset + $did] += $cnt;
            }
        }
        unset($buckets);

        $escapedPaths = [];
        $datePrefixes = [];
        for ($p = 0; $p < $pathCount; $p++) {
            $escapedPaths[$p] = "\n    \"" . str_replace('/', '\\/', $pathList[$p]) . "\": {";
        }
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = "\n        \"" . $dateList[$d] . "\": ";
        }

        $remaining = count($childPids);
        while ($remaining > 0) {
            $pid = pcntl_wait($status);
            $w = array_search($pid, $childPids);
            if ($w === false) continue;
            $raw = file_get_contents($tmpPrefix . $w);
            @unlink($tmpPrefix . $w);
            $j = 0;
            foreach (unpack('V*', $raw) as $v) {
                $counts[$j] += $v;
                $j++;
            }
            $remaining--;
        }

        $fp = fopen($o, 'wb');
        stream_set_write_buffer($fp, 1048576);
        $buf = '{';
        $firstPath = true;
        for ($p = 0; $p < $pathCount; $p++) {
            $offset = $p * $stride;
            $firstD = -1;
            for ($d = 0; $d < $dateCount; $d++) {
                if ($counts[$offset + $d] > 0) { $firstD = $d; break; }
            }
            if ($firstD === -1) continue;
            if (!$firstPath) $buf .= ',';
            $firstPath = false;
            $buf .= $escapedPaths[$p];
            $buf .= $datePrefixes[$firstD] . $counts[$offset + $firstD];
            for ($d = $firstD + 1; $d < $dateCount; $d++) {
                if ($counts[$offset + $d] === 0) continue;
                $buf .= ',' . $datePrefixes[$d] . $counts[$offset + $d];
            }
            $buf .= "\n    }";
            if (strlen($buf) > 65536) { fwrite($fp, $buf); $buf = ''; }
        }
        $buf .= "\n}";
        fwrite($fp, $buf);
        fclose($fp);
        return;
    }

    $pathOffsets = [];
    foreach ($pathIds as $path => $id) {
        $pathOffsets[$path] = $id * $stride;
    }

    $handle = fopen($i, 'rb');
    stream_set_read_buffer($handle, 0);
    $counts = array_fill(0, $totalCells, 0);
    $leftover = '';
    $remaining = $fileSize;

    while ($remaining > 0) {
        $toRead = $remaining < $chunkSize ? $remaining : $chunkSize;
        $chunk = fread($handle, $toRead);
        if ($chunk === false || $chunk === '') break;
        $remaining -= strlen($chunk);
        $pos = 0;
        if ($leftover !== '') {
            $nlPos = strpos($chunk, "\n");
            if ($nlPos === false) { $leftover .= $chunk; continue; }
            $fullLine = $leftover . substr($chunk, 0, $nlPos);
            $lineLen = strlen($fullLine);
            $path = substr($fullLine, 19, $lineLen - 45);
            $date = substr($fullLine, $lineLen - 23, 8);
            if (isset($pathOffsets[$path]) && isset($dateIds[$date])) {
                $counts[$pathOffsets[$path] + $dateIds[$date]]++;
            }
            $pos = $nlPos + 1;
        }
        while (true) {
            $nlPos = strpos($chunk, "\n", $pos);
            if ($nlPos === false) { $leftover = substr($chunk, $pos); break; }
            $counts[$pathOffsets[substr($chunk, $pos + 19, $nlPos - $pos - 45)] + $dateIds[substr($chunk, $nlPos - 23, 8)]]++;
            $pos = $nlPos + 1;
            $nlPos = strpos($chunk, "\n", $pos);
            if ($nlPos === false) { $leftover = substr($chunk, $pos); break; }
            $counts[$pathOffsets[substr($chunk, $pos + 19, $nlPos - $pos - 45)] + $dateIds[substr($chunk, $nlPos - 23, 8)]]++;
            $pos = $nlPos + 1;
            $nlPos = strpos($chunk, "\n", $pos);
            if ($nlPos === false) { $leftover = substr($chunk, $pos); break; }
            $counts[$pathOffsets[substr($chunk, $pos + 19, $nlPos - $pos - 45)] + $dateIds[substr($chunk, $nlPos - 23, 8)]]++;
            $pos = $nlPos + 1;
            $nlPos = strpos($chunk, "\n", $pos);
            if ($nlPos === false) { $leftover = substr($chunk, $pos); break; }
            $counts[$pathOffsets[substr($chunk, $pos + 19, $nlPos - $pos - 45)] + $dateIds[substr($chunk, $nlPos - 23, 8)]]++;
            $pos = $nlPos + 1;
            $nlPos = strpos($chunk, "\n", $pos);
            if ($nlPos === false) { $leftover = substr($chunk, $pos); break; }
            $counts[$pathOffsets[substr($chunk, $pos + 19, $nlPos - $pos - 45)] + $dateIds[substr($chunk, $nlPos - 23, 8)]]++;
            $pos = $nlPos + 1;
            $nlPos = strpos($chunk, "\n", $pos);
            if ($nlPos === false) { $leftover = substr($chunk, $pos); break; }
            $counts[$pathOffsets[substr($chunk, $pos + 19, $nlPos - $pos - 45)] + $dateIds[substr($chunk, $nlPos - 23, 8)]]++;
            $pos = $nlPos + 1;
        }
    }
    if ($leftover !== '') {
        $lineLen = strlen($leftover);
        if ($lineLen >= 46) {
            $path = substr($leftover, 19, $lineLen - 45);
            $date = substr($leftover, $lineLen - 23, 8);
            if (isset($pathOffsets[$path]) && isset($dateIds[$date])) {
                $counts[$pathOffsets[$path] + $dateIds[$date]]++;
            }
        }
    }
    fclose($handle);

    $escapedPaths = [];
    $datePrefixes = [];
    for ($p = 0; $p < $pathCount; $p++) {
        $escapedPaths[$p] = "\n    \"" . str_replace('/', '\\/', $pathList[$p]) . "\": {";
    }
    for ($d = 0; $d < $dateCount; $d++) {
        $datePrefixes[$d] = "\n        \"" . $dateList[$d] . "\": ";
    }
    $fp = fopen($o, 'wb');
    stream_set_write_buffer($fp, 1048576);
    $buf = '{';
    $firstPath = true;
    for ($p = 0; $p < $pathCount; $p++) {
        $offset = $p * $stride;
        $hasAny = false;
        for ($d = 0; $d < $dateCount; $d++) {
            if ($counts[$offset + $d] > 0) { $hasAny = true; break; }
        }
        if (!$hasAny) continue;
        if (!$firstPath) $buf .= ',';
        $firstPath = false;
        $buf .= $escapedPaths[$p];
        $firstDate = true;
        for ($d = 0; $d < $dateCount; $d++) {
            $count = $counts[$offset + $d];
            if ($count === 0) continue;
            if (!$firstDate) $buf .= ',';
            $firstDate = false;
            $buf .= $datePrefixes[$d] . $count;
        }
        $buf .= "\n    }";
        if (strlen($buf) > 65536) { fwrite($fp, $buf); $buf = ''; }
    }
    $buf .= "\n}";
    fwrite($fp, $buf);
    fclose($fp);
}
