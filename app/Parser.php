<?php

final class Parser
{
    static $FIRST_READ_CHUNK = 165_000;
    static $READ_CHUNK = 165_000;
    static $CORES = 9;

    static public function partParse(string $inputPath, int $start, int $length, $dates, $paths, $fullCount, $next) {
        $read = 0;

        $output = \str_repeat(\chr(0), $fullCount);

        $file = \fopen($inputPath, 'r');
        \stream_set_read_buffer($file, 0);
        \fseek($file, $start);

        $orderOutput = "";
        // Reading + determine page order
        if($start == 0) {
            $order = [];

            $lenAsked = Parser::$FIRST_READ_CHUNK;
            $buffer = \fread($file, $lenAsked);

            if(\substr($buffer, -1) != \PHP_EOL) {
                $extra = \fgets($file);
                $lenAsked += \strlen($extra);
                $buffer .= $extra;
            }

            $lenAsked -= 10;
            $lenAskedBatch = $lenAsked - 1900;

            $nextPos = -1;
            $pos = -1;
            while($nextPos < $lenAskedBatch) {
                $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                $pathId = $paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                $index = $dates[\substr($buffer, $pos - 22, 7)]+$pathId;
                $output[$index] = $next[$output[$index]];

                $order[$pathId] = true;

                $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                $pathId = $paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$pathId;
                $output[$index] = $next[$output[$index]];

                $order[$pathId] = true;

                $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                $pathId = $paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                $index = $dates[\substr($buffer, $pos - 22, 7)]+$pathId;
                $output[$index] = $next[$output[$index]];

                $order[$pathId] = true;

                $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                $pathId = $paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$pathId;
                $output[$index] = $next[$output[$index]];

                $order[$pathId] = true;

                $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                $pathId = $paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                $index = $dates[\substr($buffer, $pos - 22, 7)]+$pathId;
                $output[$index] = $next[$output[$index]];

                $order[$pathId] = true;

                $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                $pathId = $paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$pathId;
                $output[$index] = $next[$output[$index]];

                $order[$pathId] = true;

                $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                $pathId = $paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                $index = $dates[\substr($buffer, $pos - 22, 7)]+$pathId;
                $output[$index] = $next[$output[$index]];

                $order[$pathId] = true;

                $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                $pathId = $paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$pathId;
                $output[$index] = $next[$output[$index]];

                $order[$pathId] = true;
            }

            while($nextPos < $lenAsked) {
                $pos = $nextPos;
                $nextPos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                $pathId = $paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$pathId;
                $output[$index] = $next[$output[$index]];
                
                $order[$pathId] = true;
            }

            $read += $lenAsked+10;
            $orderOutput = \pack("v*", ...\array_keys($order));
        }

        // Fast reading
        while (!\feof($file) && $read < $length) {
            $lenAsked = $read + Parser::$READ_CHUNK >= $length ? $length - $read : Parser::$READ_CHUNK;
            $buffer = \fread($file, $lenAsked);

            if(\substr($buffer, -1) != \PHP_EOL) {
                $extra = \fgets($file);
                $lenAsked += \strlen($extra);
                $buffer .= $extra;
            }

            $lenAsked -= 10;
            $lenAskedBatch = $lenAsked - 1900;

            $nextPos = -1;
            $pos = -1;
            while($nextPos < $lenAskedBatch) {
                $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                $index = $dates[\substr($buffer, $pos - 22, 7)]+$paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                $output[$index] = $next[$output[$index]];

                $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                $output[$index] = $next[$output[$index]];

                $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                $index = $dates[\substr($buffer, $pos - 22, 7)]+$paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                $output[$index] = $next[$output[$index]];

                $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                $output[$index] = $next[$output[$index]];

                $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                $index = $dates[\substr($buffer, $pos - 22, 7)]+$paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                $output[$index] = $next[$output[$index]];

                $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                $output[$index] = $next[$output[$index]];

                $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                $index = $dates[\substr($buffer, $pos - 22, 7)]+$paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                $output[$index] = $next[$output[$index]];

                $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                $output[$index] = $next[$output[$index]];

                $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                $index = $dates[\substr($buffer, $pos - 22, 7)]+$paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                $output[$index] = $next[$output[$index]];

                $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                $output[$index] = $next[$output[$index]];

                $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                $index = $dates[\substr($buffer, $pos - 22, 7)]+$paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                $output[$index] = $next[$output[$index]];

                $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                $output[$index] = $next[$output[$index]];

                $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                $index = $dates[\substr($buffer, $pos - 22, 7)]+$paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                $output[$index] = $next[$output[$index]];

                $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                $output[$index] = $next[$output[$index]];

                $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                $index = $dates[\substr($buffer, $pos - 22, 7)]+$paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                $output[$index] = $next[$output[$index]];

                $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                $output[$index] = $next[$output[$index]];

                $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                $index = $dates[\substr($buffer, $pos - 22, 7)]+$paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                $output[$index] = $next[$output[$index]];

                $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                $output[$index] = $next[$output[$index]];

                $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                $index = $dates[\substr($buffer, $pos - 22, 7)]+$paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                $output[$index] = $next[$output[$index]];

                $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                $output[$index] = $next[$output[$index]];

                $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                $index = $dates[\substr($buffer, $pos - 22, 7)]+$paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                $output[$index] = $next[$output[$index]];

                $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                $output[$index] = $next[$output[$index]];

                $pos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                $index = $dates[\substr($buffer, $pos - 22, 7)]+$paths[\substr($buffer, $nextPos + 30, $pos - $nextPos - 56)];
                $output[$index] = $next[$output[$index]];

                $nextPos = \strpos($buffer, \PHP_EOL, $pos + 56);
                $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                $output[$index] = $next[$output[$index]];
            }

            while($nextPos < $lenAsked) {
                $pos = $nextPos;
                $nextPos = \strpos($buffer, \PHP_EOL, $nextPos + 56);
                $index = $dates[\substr($buffer, $nextPos - 22, 7)]+$paths[\substr($buffer, $pos + 30, $nextPos - $pos - 56)];
                $output[$index] = $next[$output[$index]];
            }

            $read += $lenAsked+10;
        }

        return $output.$orderOutput;
    }

    static public function partParallel(string $inputPath, $dates, $paths, $fullCount, $ranges, $streams) {
        $next = [];
        for($i=0; $i!=120;$i++) {
            $next[\chr($i)] = \chr($i+1);
        }

        $pid = \pcntl_fork(); // 0.2
        if ($pid == 0) {
            fclose($streams[4][1]);
            fclose($streams[5][1]);
            fclose($streams[6][1]);
            fclose($streams[7][1]);                
            $pid = \pcntl_fork(); // 0.4
            if ($pid == 0) {
                fclose($streams[2][1]);
                fclose($streams[3][1]);
                $pid = \pcntl_fork(); // 0.6
                if ($pid == 0) {
                    fclose($streams[1][1]);
                    Parser::partParallelGo($inputPath, $dates, $paths, $fullCount, $ranges, $streams, 0, $next);
                    exit();
                }
                fclose($streams[0][1]);
                Parser::partParallelGo($inputPath, $dates, $paths, $fullCount, $ranges, $streams, 1, $next);
                exit();
            }
            fclose($streams[0][1]);
            fclose($streams[1][1]);
            $pid = \pcntl_fork(); // 0.6
            if ($pid == 0) {
                fclose($streams[3][1]);
                Parser::partParallelGo($inputPath, $dates, $paths, $fullCount, $ranges, $streams, 2, $next);
                exit();
            }
            fclose($streams[2][1]);
            Parser::partParallelGo($inputPath, $dates, $paths, $fullCount, $ranges, $streams, 3, $next);
            exit();
        }

        fclose($streams[0][1]);
        fclose($streams[1][1]);
        fclose($streams[2][1]);
        fclose($streams[3][1]);
        $pid = \pcntl_fork(); // 0.4
        if ($pid == 0) {
            fclose($streams[6][1]);
            fclose($streams[7][1]);
            $pid = \pcntl_fork(); // 0.6
            if ($pid == 0) {
                fclose($streams[5][1]);
                Parser::partParallelGo($inputPath, $dates, $paths, $fullCount, $ranges, $streams, 4, $next);
                exit();
            }
            fclose($streams[4][1]);
            Parser::partParallelGo($inputPath, $dates, $paths, $fullCount, $ranges, $streams, 5, $next);
            exit();
        }

        fclose($streams[4][1]);
        fclose($streams[5][1]);
        $pid = \pcntl_fork(); // 0.6
        if ($pid == 0) {
            $pid = \pcntl_fork(); // 0.8
            if ($pid == 0) {
                fclose($streams[7][1]);
                Parser::partParallelGo($inputPath, $dates, $paths, $fullCount, $ranges, $streams, 6, $next);
                exit();
            }
            fclose($streams[6][1]);
            Parser::partParallelGo($inputPath, $dates, $paths, $fullCount, $ranges, $streams, 7, $next);
            exit();
        }

        fclose($streams[6][1]);  
        fclose($streams[7][1]);

        $output = \array_fill(0, $fullCount, 0);
        $j = 0;
        foreach(\unpack('C*', Parser::partParse($inputPath, $ranges[8][0], $ranges[8][1]-$ranges[8][0], $dates, $paths, $fullCount, $next)) as $data) {
            $output[$j++] += $data;
        }
        return $output;
    }

    static public function partParallelGo(string $inputPath, $dates, $paths, $fullCount, $ranges, $streams, $i, $next) {
        $output = Parser::partParse($inputPath, $ranges[$i][0], $ranges[$i][1]-$ranges[$i][0], $dates, $paths, $fullCount, $next);
        \fwrite($streams[$i][1], $output);
        \fflush($streams[$i][1]);
        \fclose($streams[$i][1]);
        exit();
    }

    static public function parse(string $inputPath, string $outputPath): void
    {
        \gc_disable();

        $filesize = \filesize($inputPath);

        // Prepare arrays
        $m2d = [0, 32, 30, 32, 31, 32, 31, 32, 32, 31, 32, 31, 32];
        $numbers = ['', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31'];
        $pages = [
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

        $paths = [];
        $pathCount = 0;
        foreach($pages as $page) {
            $paths[\substr($page, 4)] = $pathCount++;
        }

        $dates = [];
        $dateCount = 0;
        for($y=0; $y!=6; $y++) {
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
        $file = \fopen($inputPath, 'r');
        \stream_set_read_buffer($file, 0);
        $length = \ceil($filesize/Parser::$CORES*1.05);
        for($i=0; $i!=Parser::$CORES; $i++) {
            \fseek($file, $length*$i+$length);
            \fgets($file);
            $end = \ftell($file);
            $ranges[$i] = [$start, $end];
            $start = $end;
        }
        $ranges[$i-1][1] = $filesize;
        \fclose($file);

        $streams = [];
        for($i=0; $i!=Parser::$CORES-1; $i++) {
            $streams[$i]  = \stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            $threads[$i] = $streams[$i][0];
            \stream_set_chunk_size($streams[$i][0], $fullCount*2);
            \stream_set_chunk_size($streams[$i][1], $fullCount*2);
        }

        // Start threads
        $output = Parser::partParallel($inputPath, $dates, $paths, $fullCount, $ranges, $streams);

        // Precompute while waiting
        $datesJson = [];
        foreach($dates as $date => $dateI) {
            $datesJson[$dateI] = ",\n        \"202".$date.'": ';
        }

        $pathsJson = [];
        foreach($pages as $page) {
            $short = \substr($page, 4);
            $pathsJson[$paths[$short]] = "\n    },\n    \"\\/blog\\/".$page.'": {';
        }

        $output = \array_fill(0, $fullCount, 0);

        // Read threads
        $read = []; $write = []; $except = []; $outputs = [0,0,0,0,0,0,0,0,0]; $output0 = "";
        while(\count($threads) != 0) {
            $read = $threads;
            \stream_select($read, $write, $except, 5);
            foreach($read as $i => $thread) {
                if($i == 0) {
                    $output0 .= \fread($thread, Parser::$READ_CHUNK);
                }
                else {
                    $j = $outputs[$i];
                    foreach(\unpack('C*', \fread($thread, Parser::$READ_CHUNK)) as $data) {
                        $output[$j++] += $data;
                    }
                    $outputs[$i] = $j;
                }

                if(\feof($thread)) {
                    if($i == 0) {
                        $data = \unpack('C*', \substr($output0, 0, $fullCount));
                        for($j=0; $j!=$fullCount; $j++) {
                            $output[$j] += $data[$j+1];
                        }
                        $sortedPaths = \unpack("v*", \substr($output0, $fullCount));
                        $pathsJson[$sortedPaths[1]] = \substr($pathsJson[$sortedPaths[1]], 7);
                        unset($output0);
                    }
                    unset($threads[$i]);
                }


            }
        }

        // Merge
        $buffer = '{';
        $max = $pathCount+1;
        for($i=1; $i!=$max; $i++) {
            $pathI = $sortedPaths[$i];
            $buffer .= $pathsJson[$pathI];  
            for($j=$pathI; $j<$fullCount; $j+=$pathCount) {
                if($output[$j] != 0) {
                    $buffer .= \substr($datesJson[$j-$pathI].$output[$j], 1);
                    $j+=$pathCount;
                    break;
                }
            }

            for(; $j<$fullCount; $j+=$pathCount) {
                if($output[$j] != 0) {
                    $buffer .= $datesJson[$j-$pathI].$output[$j];
                }
            }
        }
        $buffer .= "\n    }\n}";
        \file_put_contents($outputPath, $buffer);
    }
}