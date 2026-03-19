<?php

final class Parser
{
    static $FIRST_READ_CHUNK = 165_000;
    static $READ_CHUNK = 165_000;
    static $CORES = 9;

    static public function partParse($input, $start, $length, $dates, $paths, $fullCount, $n) {
        $read = 0;

        $output = \str_repeat(\chr(0), $fullCount);

        $file = \fopen($input, 'r');
        \stream_set_read_buffer($file, 0);
        \fseek($file, $start);

        $orderOutput = "";
        // Reading + determine page order
        if($start == 0) {
            $order = [];
            $orderI = 0;

            $lenAsked = Parser::$FIRST_READ_CHUNK;
            $b = \fread($file, $lenAsked);

            if(\substr($b, -1) != \PHP_EOL) {
                $extra = \fgets($file);
                $lenAsked += \strlen($extra);
                $b .= $extra;
            }

            $pos = $lenAsked - 23;
            $read += $lenAsked;

            while($pos > 800) {
                $p = $paths[\substr($b, $pos - 17, 13)] ?? $paths[\substr($b, $pos - 26, 7)];
                $i = $dates[\substr($b, $pos, 7)]+($p & 511);
                $output[$i] = $n[$output[$i]];
                $pos -= $p >> 9;

                $order[$p & 511] = $orderI++;

                $p = $paths[\substr($b, $pos - 17, 13)] ?? $paths[\substr($b, $pos - 26, 7)];
                $i = $dates[\substr($b, $pos, 7)]+($p & 511);
                $output[$i] = $n[$output[$i]];
                $pos -= $p >> 9;

                $order[$p & 511] = $orderI++;

                $p = $paths[\substr($b, $pos - 17, 13)] ?? $paths[\substr($b, $pos - 26, 7)];
                $i = $dates[\substr($b, $pos, 7)]+($p & 511);
                $output[$i] = $n[$output[$i]];
                $pos -= $p >> 9;

                $order[$p & 511] = $orderI++;

                $p = $paths[\substr($b, $pos - 17, 13)] ?? $paths[\substr($b, $pos - 26, 7)];
                $i = $dates[\substr($b, $pos, 7)]+($p & 511);
                $output[$i] = $n[$output[$i]];
                $pos -= $p >> 9;

                $order[$p & 511] = $orderI++;

                $p = $paths[\substr($b, $pos - 17, 13)] ?? $paths[\substr($b, $pos - 26, 7)];
                $i = $dates[\substr($b, $pos, 7)]+($p & 511);
                $output[$i] = $n[$output[$i]];
                $pos -= $p >> 9;

                $order[$p & 511] = $orderI++;

                $p = $paths[\substr($b, $pos - 17, 13)] ?? $paths[\substr($b, $pos - 26, 7)];
                $i = $dates[\substr($b, $pos, 7)]+($p & 511);
                $output[$i] = $n[$output[$i]];
                $pos -= $p >> 9;

                $order[$p & 511] = $orderI++;

                $p = $paths[\substr($b, $pos - 17, 13)] ?? $paths[\substr($b, $pos - 26, 7)];
                $i = $dates[\substr($b, $pos, 7)]+($p & 511);
                $output[$i] = $n[$output[$i]];
                $pos -= $p >> 9;

                $order[$p & 511] = $orderI++;

                $p = $paths[\substr($b, $pos - 17, 13)] ?? $paths[\substr($b, $pos - 26, 7)];
                $i = $dates[\substr($b, $pos, 7)]+($p & 511);
                $output[$i] = $n[$output[$i]];
                $pos -= $p >> 9;

                $order[$p & 511] = $orderI++;

                $p = $paths[\substr($b, $pos - 17, 13)] ?? $paths[\substr($b, $pos - 26, 7)];
                $i = $dates[\substr($b, $pos, 7)]+($p & 511);
                $output[$i] = $n[$output[$i]];
                $pos -= $p >> 9;

                $order[$p & 511] = $orderI++;

                $p = $paths[\substr($b, $pos - 17, 13)] ?? $paths[\substr($b, $pos - 26, 7)];
                $i = $dates[\substr($b, $pos, 7)]+($p & 511);
                $output[$i] = $n[$output[$i]];
                $pos -= $p >> 9;

                $order[$p & 511] = $orderI++;
            }

            while($pos > 10) {
                $p = $paths[\substr($b, $pos - 17, 13)] ?? $paths[\substr($b, $pos - 26, 7)];
                $i = $dates[\substr($b, $pos, 7)]+($p & 511);
                $output[$i] = $n[$output[$i]];
                $pos -= $p >> 9;

                $order[$p & 511] = $orderI++;
            }

            $order = \array_flip($order);
            \krsort($order);
            $orderOutput = \pack("v*", ...$order);
        }

        // Fast reading
        while (!\feof($file) && $read < $length) {
            $lenAsked = $read + Parser::$READ_CHUNK >= $length ? $length - $read : Parser::$READ_CHUNK;
            $b = \fread($file, $lenAsked);

            if(\substr($b, -1) != \PHP_EOL) {
                $extra = \fgets($file);
                $lenAsked += \strlen($extra);
                $b .= $extra;
            }

            $pos = $lenAsked - 23;
            $read += $lenAsked;

            while($pos > 800) {
                $p = $paths[\substr($b, $pos - 17, 13)] ?? $paths[\substr($b, $pos - 26, 7)];
                $i = $dates[\substr($b, $pos, 7)]+($p & 511);
                $output[$i] = $n[$output[$i]];
                $pos -= $p >> 9;

                $p = $paths[\substr($b, $pos - 17, 13)] ?? $paths[\substr($b, $pos - 26, 7)];
                $i = $dates[\substr($b, $pos, 7)]+($p & 511);
                $output[$i] = $n[$output[$i]];
                $pos -= $p >> 9;                

                $p = $paths[\substr($b, $pos - 17, 13)] ?? $paths[\substr($b, $pos - 26, 7)];
                $i = $dates[\substr($b, $pos, 7)]+($p & 511);
                $output[$i] = $n[$output[$i]];
                $pos -= $p >> 9;

                $p = $paths[\substr($b, $pos - 17, 13)] ?? $paths[\substr($b, $pos - 26, 7)];
                $i = $dates[\substr($b, $pos, 7)]+($p & 511);
                $output[$i] = $n[$output[$i]];
                $pos -= $p >> 9;

                $p = $paths[\substr($b, $pos - 17, 13)] ?? $paths[\substr($b, $pos - 26, 7)];
                $i = $dates[\substr($b, $pos, 7)]+($p & 511);
                $output[$i] = $n[$output[$i]];
                $pos -= $p >> 9;

                $p = $paths[\substr($b, $pos - 17, 13)] ?? $paths[\substr($b, $pos - 26, 7)];
                $i = $dates[\substr($b, $pos, 7)]+($p & 511);
                $output[$i] = $n[$output[$i]];
                $pos -= $p >> 9;

                $p = $paths[\substr($b, $pos - 17, 13)] ?? $paths[\substr($b, $pos - 26, 7)];
                $i = $dates[\substr($b, $pos, 7)]+($p & 511);
                $output[$i] = $n[$output[$i]];
                $pos -= $p >> 9;

                $p = $paths[\substr($b, $pos - 17, 13)] ?? $paths[\substr($b, $pos - 26, 7)];
                $i = $dates[\substr($b, $pos, 7)]+($p & 511);
                $output[$i] = $n[$output[$i]];
                $pos -= $p >> 9;

                $p = $paths[\substr($b, $pos - 17, 13)] ?? $paths[\substr($b, $pos - 26, 7)];
                $i = $dates[\substr($b, $pos, 7)]+($p & 511);
                $output[$i] = $n[$output[$i]];
                $pos -= $p >> 9;

                $p = $paths[\substr($b, $pos - 17, 13)] ?? $paths[\substr($b, $pos - 26, 7)];
                $i = $dates[\substr($b, $pos, 7)]+($p & 511);
                $output[$i] = $n[$output[$i]];
                $pos -= $p >> 9;
            }

            while($pos > 10) {
                $p = $paths[\substr($b, $pos - 17, 13)] ?? $paths[\substr($b, $pos - 26, 7)];
                $i = $dates[\substr($b, $pos, 7)]+($p & 511);
                $output[$i] = $n[$output[$i]];
                $pos -= $p >> 9;
            }
        }
        return $output.$orderOutput;
    }

    static public function parse(string $input, string $outputPath): void
    {
        \gc_disable();

        // Prepare arrays
        $m2d = [0, 32, 30, 32, 31, 32, 31, 32, 32, 31, 32, 31, 32];
        $numbers = ['', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31'];
        $pages = [
            '.io/blog/which-editor-to-choose',
            '.io/blog/tackling_responsive_images-part_1',
            '.io/blog/tackling_responsive_images-part_2',
            '.io/blog/image_optimizers',
            '.io/blog/static_sites_vs_caching',
            '.io/blog/stitcher-alpha-4',
            '.io/blog/simplest-plugin-support',
            '.io/blog/stitcher-alpha-5',
            '.io/blog/php-generics-and-why-we-need-them',
            '.io/blog/stitcher-beta-1',
            '.io/blog/array-objects-with-fixed-types',
            '.io/blog/performance-101-building-the-better-web',
            '.io/blog/process-forks',
            '.io/blog/object-oriented-generators',
            '.io/blog/responsive-images-as-css-background',
            '.io/blog/a-programmers-cognitive-load',
            '.io/blog/mastering-key-bindings',
            '.io/blog/stitcher-beta-2',
            '.io/blog/phpstorm-performance',
            '.io/blog/optimised-uuids-in-mysql',
            '.io/blog/asynchronous-php',
            '.io/blog/mysql-import-json-binary-character-set',
            '.io/blog/where-a-curly-bracket-belongs',
            '.io/blog/mysql-query-logging',
            '.io/blog/mysql-show-foreign-key-errors',
            '.io/blog/responsive-images-done-right',
            '.io/blog/phpstorm-tips-for-power-users',
            '.io/blog/what-php-can-be',
            '.io/blog/phpstorm-performance-issues-on-osx',
            '.io/blog/dependency-injection-for-beginners',
            '.io/blog/liskov-and-type-safety',
            '.io/blog/acquisition-by-giants',
            '.io/blog/visual-perception-of-code',
            '.io/blog/service-locator-anti-pattern',
            '.io/blog/the-web-in-2045',
            '.io/blog/eloquent-mysql-views',
            '.io/blog/laravel-view-models',
            '.io/blog/laravel-view-models-vs-view-composers',
            '.io/blog/organise-by-domain',
            '.io/blog/array-merge-vs + ',
            '.io/blog/share-a-blog-assertchris-io',
            '.io/blog/phpstorm-performance-october-2018',
            '.io/blog/structuring-unstructured-data',
            '.io/blog/share-a-blog-codingwriter-com',
            '.io/blog/new-in-php-73',
            '.io/blog/share-a-blog-betterwebtype-com',
            '.io/blog/have-you-thought-about-casing',
            '.io/blog/comparing-dates',
            '.io/blog/share-a-blog-sebastiandedeyne-com',
            '.io/blog/analytics-for-developers',
            '.io/blog/announcing-aggregate',
            '.io/blog/php-jit',
            '.io/blog/craftsmen-know-their-tools',
            '.io/blog/laravel-queueable-actions',
            '.io/blog/php-73-upgrade-mac',
            '.io/blog/array-destructuring-with-list-in-php',
            '.io/blog/unsafe-sql-functions-in-laravel',
            '.io/blog/starting-a-newsletter',
            '.io/blog/short-closures-in-php',
            '.io/blog/solid-interfaces-and-final-rant-with-brent',
            '.io/blog/php-in-2019',
            '.io/blog/starting-a-podcast',
            '.io/blog/a-project-at-spatie',
            '.io/blog/what-are-objects-anyway-rant-with-brent',
            '.io/blog/tests-and-types',
            '.io/blog/typed-properties-in-php-74',
            '.io/blog/preloading-in-php-74',
            '.io/blog/things-dependency-injection-is-not-about',
            '.io/blog/a-letter-to-the-php-team',
            '.io/blog/a-letter-to-the-php-team-reply-to-joe',
            '.io/blog/guest-posts',
            '.io/blog/can-i-translate-your-blog',
            '.io/blog/laravel-has-many-through',
            '.io/blog/laravel-custom-relation-classes',
            '.io/blog/new-in-php-74',
            '.io/blog/php-74-upgrade-mac',
            '.io/blog/php-preload-benchmarks',
            '.io/blog/php-in-2020',
            '.io/blog/enums-without-enums',
            '.io/blog/bitwise-booleans-in-php',
            '.io/blog/event-driven-php',
            '.io/blog/minor-versions-breaking-changes',
            '.io/blog/combining-event-sourcing-and-stateful-systems',
            '.io/blog/array-chunk-in-php',
            '.io/blog/php-8-in-8-code-blocks',
            '.io/blog/builders-and-architects-two-types-of-programmers',
            '.io/blog/the-ikea-effect',
            '.io/blog/php-74-in-7-code-blocks',
            '.io/blog/improvements-on-laravel-nova',
            '.io/blog/type-system-in-php-survey',
            '.io/blog/merging-multidimensional-arrays-in-php',
            '.io/blog/what-is-array-plus-in-php',
            '.io/blog/type-system-in-php-survey-results',
            '.io/blog/constructor-promotion-in-php-8',
            '.io/blog/abstract-resources-in-laravel-nova',
            '.io/blog/braille-and-the-history-of-software',
            '.io/blog/jit-in-real-life-web-applications',
            '.io/blog/php-8-match-or-switch',
            '.io/blog/why-we-need-named-params-in-php',
            '.io/blog/shorthand-comparisons-in-php',
            '.io/blog/php-8-before-and-after',
            '.io/blog/php-8-named-arguments',
            '.io/blog/my-journey-into-event-sourcing',
            '.io/blog/differences',
            '.io/blog/annotations',
            '.io/blog/dont-get-stuck',
            '.io/blog/attributes-in-php-8',
            '.io/blog/the-case-for-transpiled-generics',
            '.io/blog/phpstorm-scopes',
            '.io/blog/why-light-themes-are-better-according-to-science',
            '.io/blog/what-a-good-pr-looks-like',
            '.io/blog/front-line-php',
            '.io/blog/php-8-jit-setup',
            '.io/blog/php-8-nullsafe-operator',
            '.io/blog/new-in-php-8',
            '.io/blog/php-8-upgrade-mac',
            '.io/blog/when-i-lost-a-few-hundred-leads',
            '.io/blog/websites-like-star-wars',
            '.io/blog/php-reimagined',
            '.io/blog/a-storm-in-a-glass-of-water',
            '.io/blog/php-enums-before-php-81',
            '.io/blog/php-enums',
            '.io/blog/dont-write-your-own-framework',
            '.io/blog/honesty',
            '.io/blog/thoughts-on-event-sourcing',
            '.io/blog/what-event-sourcing-is-not-about',
            '.io/blog/fibers-with-a-grain-of-salt',
            '.io/blog/php-in-2021',
            '.io/blog/parallel-php',
            '.io/blog/why-we-need-multi-line-short-closures-in-php',
            '.io/blog/a-new-major-version-of-laravel-event-sourcing',
            '.io/blog/what-about-config-builders',
            '.io/blog/opinion-driven-design',
            '.io/blog/php-version-stats-july-2021',
            '.io/blog/what-about-request-classes',
            '.io/blog/cloning-readonly-properties-in-php-81',
            '.io/blog/an-event-driven-mindset',
            '.io/blog/php-81-before-and-after',
            '.io/blog/optimistic-or-realistic-estimates',
            '.io/blog/we-dont-need-runtime-type-checks',
            '.io/blog/the-road-to-php',
            '.io/blog/why-do-i-write',
            '.io/blog/rational-thinking',
            '.io/blog/named-arguments-and-variadic-functions',
            '.io/blog/re-on-using-psr-abstractions',
            '.io/blog/my-ikea-clock',
            '.io/blog/php-81-readonly-properties',
            '.io/blog/birth-and-death-of-a-framework',
            '.io/blog/php-81-new-in-initializers',
            '.io/blog/route-attributes',
            '.io/blog/generics-in-php-video',
            '.io/blog/php-81-in-8-code-blocks',
            '.io/blog/new-in-php-81',
            '.io/blog/php-81-performance-in-real-life',
            '.io/blog/php-81-upgrade-mac',
            '.io/blog/how-to-be-right-on-the-internet',
            '.io/blog/php-version-stats-january-2022',
            '.io/blog/php-in-2022',
            '.io/blog/how-i-plan',
            '.io/blog/twitter-home-made-me-miserable',
            '.io/blog/its-your-fault',
            '.io/blog/dealing-with-dependencies',
            '.io/blog/php-in-2021-video',
            '.io/blog/generics-in-php-1',
            '.io/blog/generics-in-php-2',
            '.io/blog/generics-in-php-3',
            '.io/blog/generics-in-php-4',
            '.io/blog/goodbye',
            '.io/blog/strategies',
            '.io/blog/dealing-with-deprecations',
            '.io/blog/attribute-usage-in-top-php-packages',
            '.io/blog/php-enum-style-guide',
            '.io/blog/clean-and-minimalistic-phpstorm',
            '.io/blog/stitcher-turns-5',
            '.io/blog/php-version-stats-july-2022',
            '.io/blog/evolution-of-a-php-object',
            '.io/blog/uncertainty-doubt-and-static-analysis',
            '.io/blog/road-to-php-82',
            '.io/blog/php-performance-across-versions',
            '.io/blog/light-colour-schemes-are-better',
            '.io/blog/deprecated-dynamic-properties-in-php-82',
            '.io/blog/php-reimagined-part-2',
            '.io/blog/thoughts-on-asymmetric-visibility',
            '.io/blog/uses',
            '.io/blog/php-82-in-8-code-blocks',
            '.io/blog/readonly-classes-in-php-82',
            '.io/blog/deprecating-spatie-dto',
            '.io/blog/php-82-upgrade-mac',
            '.io/blog/php-annotated',
            '.io/blog/you-cannot-find-me-on-mastodon',
            '.io/blog/new-in-php-82',
            '.io/blog/all-i-want-for-christmas',
            '.io/blog/upgrading-to-php-82',
            '.io/blog/php-version-stats-january-2023',
            '.io/blog/php-in-2023',
            '.io/blog/tabs-are-better',
            '.io/blog/sponsors',
            '.io/blog/why-curly-brackets-go-on-new-lines',
            '.io/blog/my-10-favourite-php-functions',
            '.io/blog/acronyms',
            '.io/blog/code-folding',
            '.io/blog/light-colour-schemes',
            '.io/blog/slashdash',
            '.io/blog/thank-you-kinsta',
            '.io/blog/cloning-readonly-properties-in-php-83',
            '.io/blog/limited-by-committee',
            '.io/blog/things-considered-harmful',
            '.io/blog/procedurally-generated-game-in-php',
            '.io/blog/dont-be-clever',
            '.io/blog/override-in-php-83',
            '.io/blog/php-version-stats-july-2023',
            '.io/blog/is-a-or-acts-as',
            '.io/blog/rfc-vote',
            '.io/blog/new-in-php-83',
            '.io/blog/i-dont-know',
            '.io/blog/passion-projects',
            '.io/blog/php-version-stats-january-2024',
            '.io/blog/the-framework-that-gets-out-of-your-way',
            '.io/blog/a-syntax-highlighter-that-doesnt-suck',
            '.io/blog/building-a-custom-language-in-tempest-highlight',
            '.io/blog/testing-patterns',
            '.io/blog/php-in-2024',
            '.io/blog/tagged-singletons',
            '.io/blog/twitter-exit',
            '.io/blog/a-vocal-minority',
            '.io/blog/php-version-stats-july-2024',
            '.io/blog/you-should',
            '.io/blog/new-with-parentheses-php-84',
            '.io/blog/html-5-in-php-84',
            '.io/blog/array-find-in-php-84',
            '.io/blog/its-all-just-text',
            '.io/blog/improved-lazy-loading',
            '.io/blog/i-dont-code-the-way-i-used-to',
            '.io/blog/php-84-at-least',
            '.io/blog/extends-vs-implements',
            '.io/blog/a-simple-approach-to-static-generation',
            '.io/blog/building-a-framework',
            '.io/blog/tagging-tempest-livestream',
            '.io/blog/things-i-learned-writing-a-fiction-novel',
            '.io/blog/unfair-advantage',
            '.io/blog/new-in-php-84',
            '.io/blog/php-version-stats-january-2025',
            '.io/blog/theoretical-engineers',
            '.io/blog/static-websites-with-tempest',
            '.io/blog/request-objects-in-tempest',
            '.io/blog/php-verse-2025',
            '.io/blog/tempest-discovery-explained',
            '.io/blog/php-version-stats-june-2025',
            '.io/blog/pipe-operator-in-php-85',
            '.io/blog/a-year-of-property-hooks',
            '.io/blog/readonly-or-private-set',
            '.io/blog/things-i-wish-i-knew',
            '.io/blog/impact-charts',
            '.io/blog/whats-your-motivator',
            '.io/blog/vendor-locked',
            '.io/blog/reducing-code-motion',
            '.io/blog/sponsoring-open-source',
            '.io/blog/my-wishlist-for-php-in-2026',
            '.io/blog/game-changing-editions',
            '.io/blog/new-in-php-85',
            '.io/blog/flooded-rss',
            '.io/blog/php-2026',
            '.io/blog/open-source-strategies',
            '.io/blog/not-optional',
            '.io/blog/processing-11-million-rows',
            '.io/blog/ai-induced-skepticism',
            '.io/blog/php-86-partial-function-application',
            '.io/blog/11-million-rows-in-seconds',
        ];

        $pathCount = 0;
        $exclude = array (
            '.io/blog/what-are-objects-anyway-rant-with-brent' => true,
            '.io/blog/solid-interfaces-and-final-rant-with-brent' => true,
            '.io/blog/abstract-resources-in-laravel-nova' => true,
            '.io/blog/improvements-on-laravel-nova' => true,
            '.io/blog/thoughts-on-event-sourcing' => true,
            '.io/blog/my-journey-into-event-sourcing' => true,
            '.io/blog/what-event-sourcing-is-not-about' => true,
            '.io/blog/things-dependency-injection-is-not-about' => true,
            '.io/blog/why-we-need-multi-line-short-closures-in-php' => true,
            '.io/blog/short-closures-in-php' => true,
            '.io/blog/a-new-major-version-of-laravel-event-sourcing' => true,
            '.io/blog/php-81-before-and-after' => true,
            '.io/blog/php-8-before-and-after' => true,
            '.io/blog/php-81-in-8-code-blocks' => true,
            '.io/blog/php-8-in-8-code-blocks' => true,
            '.io/blog/php-82-in-8-code-blocks' => true,
        );
        foreach($pages as $page) {
            if(isset($exclude[$page])) 
                $paths[\substr($page, -22, 7)] = ((\strlen($page)+43) << 9) | $pathCount++;
            else
                $paths[\substr($page, -13)] = ((\strlen($page)+43) << 9) | $pathCount++;
        }

        $dates = [];
        $dateCount = 0;
        for($y=1; $y!=6; $y++) {
            for($m=1; $m!=13; $m++) {
                $max = $m2d[$m];
                for($d=1; $d!=$max; $d++) {
                    $date = $y.'-'.$numbers[$m].'-'.$numbers[$d];
                    $dates[$date] = $pathCount*$dateCount++;
                }
            }
        }

        for($m=1; $m!=4; $m++) {
            $max = $m2d[$m];
            for($d=1; $d!=$max; $d++) {
                $date = '6-'.$numbers[$m].'-'.$numbers[$d];
                $dates[$date] = $pathCount*$dateCount++;
            }
        }

        $fullCount = $pathCount*$dateCount;

        // Determine ranges
        $ranges = [];
        $start = 0;
        $file = \fopen($input, 'r');
        \stream_set_read_buffer($file, 0);
        \fseek($file, 0, SEEK_END);
        $filesize = \ftell($file);
        $length = \ceil($filesize/Parser::$CORES);
        for($i=0; $i!=Parser::$CORES; $i++) {
            \fseek($file, $length*$i+$length);
            \fgets($file);
            $end = \ftell($file);
            $ranges[$i] = [$start, $end];
            $start = $end;
        }
        $ranges[$i-1][1] = $filesize;
        
        $streams = [];
        for($i=0; $i!=Parser::$CORES; $i++) {
            $streams[$i]  = \stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            $threads[$i] = $streams[$i][0];
            \stream_set_chunk_size($streams[$i][0], $fullCount*2);
            \stream_set_chunk_size($streams[$i][1], $fullCount*2);
        }

        // Start threads
        $next = [];
        for($i=0; $i!=120;$i++) {
            $next[\chr($i)] = \chr($i+1);
        }

        $pid = \pcntl_fork(); // 0.2
        if ($pid == 0) {
            \fclose($streams[4][1]);
            \fclose($streams[5][1]);
            \fclose($streams[6][1]);
            \fclose($streams[7][1]);  
            \fclose($streams[8][1]);              
            $pid = \pcntl_fork(); // 0.4
            if ($pid == 0) {
                \fclose($streams[2][1]);
                \fclose($streams[3][1]);
                $pid = \pcntl_fork(); // 0.6
                if ($pid == 0) {
                    \fclose($streams[1][1]);
                    $output = Parser::partParse($input, $ranges[0][0], $ranges[0][1]-$ranges[0][0], $dates, $paths, $fullCount, $next);
                    \fwrite($streams[0][1], $output);
                    \fflush($streams[0][1]);
                    \fclose($streams[0][1]);
                    exit();
                }
                \fclose($streams[0][1]);
                $output = Parser::partParse($input, $ranges[1][0], $ranges[1][1]-$ranges[1][0], $dates, $paths, $fullCount, $next);
                \fwrite($streams[1][1], $output);
                \fflush($streams[1][1]);
                \fclose($streams[1][1]);
                exit();
            }
            \fclose($streams[0][1]);
            \fclose($streams[1][1]);
            $pid = \pcntl_fork(); // 0.6
            if ($pid == 0) {
                \fclose($streams[3][1]);
                $output = Parser::partParse($input, $ranges[2][0], $ranges[2][1]-$ranges[2][0], $dates, $paths, $fullCount, $next);
                \fwrite($streams[2][1], $output);
                \fflush($streams[2][1]);
                \fclose($streams[2][1]);
                exit();
            }
            \fclose($streams[2][1]);
            $output = Parser::partParse($input, $ranges[3][0], $ranges[3][1]-$ranges[3][0], $dates, $paths, $fullCount, $next);
            \fwrite($streams[3][1], $output);
            \fflush($streams[3][1]);
            \fclose($streams[3][1]);
            exit();
        }

        \fclose($streams[0][1]);
        \fclose($streams[1][1]);
        \fclose($streams[2][1]);
        \fclose($streams[3][1]);
        $pid = \pcntl_fork(); // 0.4
        if ($pid == 0) {
            \fclose($streams[6][1]);
            \fclose($streams[7][1]);
            \fclose($streams[8][1]);
            $pid = \pcntl_fork(); // 0.6
            if ($pid == 0) {
                \fclose($streams[5][1]);
                $output = Parser::partParse($input, $ranges[4][0], $ranges[4][1]-$ranges[4][0], $dates, $paths, $fullCount, $next);
                \fwrite($streams[4][1], $output);
                \fflush($streams[4][1]);
                \fclose($streams[4][1]);
                exit();
            }
            \fclose($streams[4][1]);
            $output = Parser::partParse($input, $ranges[5][0], $ranges[5][1]-$ranges[5][0], $dates, $paths, $fullCount, $next);
            \fwrite($streams[5][1], $output);
            \fflush($streams[5][1]);
            \fclose($streams[5][1]);
            exit();
        }

        \fclose($streams[4][1]);
        \fclose($streams[5][1]);
        $pid = \pcntl_fork(); // 0.6
        if ($pid == 0) {
            \fclose($streams[8][1]);   
            
            $pid = \pcntl_fork(); // 0.8
            if ($pid == 0) {
                \fclose($streams[7][1]);
                $output = Parser::partParse($input, $ranges[6][0], $ranges[6][1]-$ranges[6][0], $dates, $paths, $fullCount, $next);
                \fwrite($streams[6][1], $output);
                \fflush($streams[6][1]);
                \fclose($streams[6][1]);
                exit();
            }
            \fclose($streams[6][1]);
            $output = Parser::partParse($input, $ranges[7][0], $ranges[7][1]-$ranges[7][0], $dates, $paths, $fullCount, $next);
            \fwrite($streams[7][1], $output);
            \fflush($streams[7][1]);
            \fclose($streams[7][1]);
            exit();
        }

        \fclose($streams[6][1]);
        \fclose($streams[7][1]); 
        
        $pid = \pcntl_fork(); // 0.8
        if ($pid == 0) {
            $output = Parser::partParse($input, $ranges[8][0], $ranges[8][1]-$ranges[8][0], $dates, $paths, $fullCount, $next);
            \fwrite($streams[8][1], $output);
            \fflush($streams[8][1]);
            \fclose($streams[8][1]);
            exit();
        }

        \fclose($streams[8][1]);

        // Precompute while waiting
        $datesJson = [];
        foreach($dates as $date => $dateI) {
            $datesJson[$dateI] = ",\n        \"202".$date.'": ';
        }

        $pathsJson = [];
        foreach($pages as $page) {
            $key = $paths[substr($page, -13)] ?? $paths[substr($page, -22, 7)];
            $pathsJson[$key & 511] = "\n    },\n    \"\\/blog\\/".substr($page,9).'": {';
        }

        $output = \str_repeat("\0", $fullCount*2);

        // Read threads
        $read = []; $write = []; $except = [];
        while(\count($threads) != 0) {
            $read = $threads;
            \stream_select($read, $write, $except, 5);
            foreach($read as $i => $thread) {
                $b = \fread($thread, Parser::$READ_CHUNK);
                while(!\feof($thread)) {
                    $b .= \fread($thread, Parser::$READ_CHUNK);
                }
                
                if($i == 0) {
                    \sodium_add($output, \chunk_split(\substr($b, 0, $fullCount), 1, "\0"));
                    $sortedPaths = \unpack("v*", \substr($b, $fullCount));
                    $pathsJson[$sortedPaths[1]] = \substr($pathsJson[$sortedPaths[1]], 7);
                }
                else {
                    \sodium_add($output, \chunk_split($b, 1, "\0"));
                }
                unset($threads[$i]);
            }
        }

        $output = unpack("v*", $output);

        // Merge
        $b = '{';
        $max = $pathCount+1;
        for($i=1; $i!=$max; $i++) {
            $pathI = $sortedPaths[$i]+1;
            $b .= $pathsJson[$pathI-1];  
            for($j=$pathI; $j<$fullCount; $j+=$pathCount) {
                if($output[$j] != 0) {
                    $b .= \substr($datesJson[$j-$pathI].$output[$j], 1);
                    $j+=$pathCount;
                    break;
                }
            }

            for(; $j<$fullCount; $j+=$pathCount) {
                if($output[$j] != 0) {
                    $b .= $datesJson[$j-$pathI].$output[$j];
                }
            }
        }
        $b .= "\n    }\n}";
        \file_put_contents($outputPath, $b);
    }
}