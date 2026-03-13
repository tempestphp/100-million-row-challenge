<?php

namespace App;

final class Parser
{
    private const int BUFFER_SIZE = 262144;
    private const int CHUNK_SIZE = 262144;
    private const int OUTPUT_BUFFER_SIZE = 1048576;

    private static $keepAlive;

    private static function initialize()
    {
        self::$keepAlive = [];

        $allPartialIds = [
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


        $dateIds = [];
        $id = 0;
        for($year = 1; $year <= 6; $year++) {
            for($month = 1; $month <= 12; $month++) {
                $yearMonthS = $year.'-'.($month < 10 ? '0'.$month : (string) $month);

                $dateIds[$yearMonthS.'-01'] = $id++;
                $dateIds[$yearMonthS.'-02'] = $id++;
                $dateIds[$yearMonthS.'-03'] = $id++;
                $dateIds[$yearMonthS.'-04'] = $id++;
                $dateIds[$yearMonthS.'-05'] = $id++;
                $dateIds[$yearMonthS.'-06'] = $id++;
                $dateIds[$yearMonthS.'-07'] = $id++;
                $dateIds[$yearMonthS.'-08'] = $id++;
                $dateIds[$yearMonthS.'-09'] = $id++;
                $dateIds[$yearMonthS.'-10'] = $id++;
                $dateIds[$yearMonthS.'-11'] = $id++;
                $dateIds[$yearMonthS.'-12'] = $id++;
                $dateIds[$yearMonthS.'-13'] = $id++;
                $dateIds[$yearMonthS.'-14'] = $id++;
                $dateIds[$yearMonthS.'-15'] = $id++;
                $dateIds[$yearMonthS.'-16'] = $id++;
                $dateIds[$yearMonthS.'-17'] = $id++;
                $dateIds[$yearMonthS.'-18'] = $id++;
                $dateIds[$yearMonthS.'-19'] = $id++;
                $dateIds[$yearMonthS.'-20'] = $id++;
                $dateIds[$yearMonthS.'-21'] = $id++;
                $dateIds[$yearMonthS.'-22'] = $id++;
                $dateIds[$yearMonthS.'-23'] = $id++;
                $dateIds[$yearMonthS.'-24'] = $id++;
                $dateIds[$yearMonthS.'-25'] = $id++;
                $dateIds[$yearMonthS.'-26'] = $id++;
                $dateIds[$yearMonthS.'-27'] = $id++;
                $dateIds[$yearMonthS.'-28'] = $id++;

                if (2 === $month && 4 !== $year) {
                    $id += 3;
                    continue;
                }

                $dateIds[$yearMonthS.'-29'] = $id++;
                if (2 === $month) {
                    $id += 2;
                    continue;
                }

                $dateIds[$yearMonthS.'-30'] = $id++;
                switch($month) {
                    case 4:
                    case 6:
                    case 9:
                    case 11:
                        ++$id;
                        continue 2;
                }

                $dateIds[$yearMonthS.'-31'] = $id++;
            }
        }

        $partialIds = [];
        $uriIds = [];
        $sequence = [];
        $id = 0;
        foreach($allPartialIds as $partial) {
            $partialIds[$partial] = $id;

            $sequence[$id] = null;
            $uriIds[$id] = $partial;

            $id += 2232;
        }
        $counts = \array_fill(0, $id, 0);

        return [$partialIds, $sequence, $uriIds, $dateIds, $counts];
    }

    private static function writeJson(&$fo, &$sequence, $uriIds, $counts)
    {
        \asort($sequence, \SORT_NUMERIC);

        \fwrite($fo, "{\n");

        $fu = false;
        \ob_start();
        foreach($sequence as $id => $sequenceNo) {
            if (null === $sequenceNo) continue;

            $j = ($fu ? ",\n".'    "\\/blog\\/'.$uriIds[$id]."\": {\n" : '    "\\/blog\\/'.$uriIds[$id]."\": {\n");

            $year = 2021;
            --$id;
o1:
            if ($counts[++$id]) {
                echo '        "',$year,'-01-01": ',$counts[$id];
                goto oy1;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-01-02": ',$counts[$id];
                goto oy2;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-01-03": ',$counts[$id];
                goto oy3;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-01-04": ',$counts[$id];
                goto oy4;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-01-05": ',$counts[$id];
                goto oy5;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-01-06": ',$counts[$id];
                goto oy6;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-01-07": ',$counts[$id];
                goto oy7;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-01-08": ',$counts[$id];
                goto oy8;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-01-09": ',$counts[$id];
                goto oy9;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-01-10": ',$counts[$id];
                goto oy10;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-01-11": ',$counts[$id];
                goto oy11;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-01-12": ',$counts[$id];
                goto oy12;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-01-13": ',$counts[$id];
                goto oy13;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-01-14": ',$counts[$id];
                goto oy14;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-01-15": ',$counts[$id];
                goto oy15;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-01-16": ',$counts[$id];
                goto oy16;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-01-17": ',$counts[$id];
                goto oy17;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-01-18": ',$counts[$id];
                goto oy18;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-01-19": ',$counts[$id];
                goto oy19;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-01-20": ',$counts[$id];
                goto oy20;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-01-21": ',$counts[$id];
                goto oy21;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-01-22": ',$counts[$id];
                goto oy22;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-01-23": ',$counts[$id];
                goto oy23;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-01-24": ',$counts[$id];
                goto oy24;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-01-25": ',$counts[$id];
                goto oy25;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-01-26": ',$counts[$id];
                goto oy26;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-01-27": ',$counts[$id];
                goto oy27;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-01-28": ',$counts[$id];
                goto oy28;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-01-29": ',$counts[$id];
                goto oy29;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-01-30": ',$counts[$id];
                goto oy30;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-01-31": ',$counts[$id];
                goto oy31;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-02-01": ',$counts[$id];
                goto oy32;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-02-02": ',$counts[$id];
                goto oy33;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-02-03": ',$counts[$id];
                goto oy34;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-02-04": ',$counts[$id];
                goto oy35;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-02-05": ',$counts[$id];
                goto oy36;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-02-06": ',$counts[$id];
                goto oy37;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-02-07": ',$counts[$id];
                goto oy38;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-02-08": ',$counts[$id];
                goto oy39;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-02-09": ',$counts[$id];
                goto oy40;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-02-10": ',$counts[$id];
                goto oy41;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-02-11": ',$counts[$id];
                goto oy42;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-02-12": ',$counts[$id];
                goto oy43;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-02-13": ',$counts[$id];
                goto oy44;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-02-14": ',$counts[$id];
                goto oy45;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-02-15": ',$counts[$id];
                goto oy46;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-02-16": ',$counts[$id];
                goto oy47;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-02-17": ',$counts[$id];
                goto oy48;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-02-18": ',$counts[$id];
                goto oy49;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-02-19": ',$counts[$id];
                goto oy50;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-02-20": ',$counts[$id];
                goto oy51;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-02-21": ',$counts[$id];
                goto oy52;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-02-22": ',$counts[$id];
                goto oy53;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-02-23": ',$counts[$id];
                goto oy54;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-02-24": ',$counts[$id];
                goto oy55;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-02-25": ',$counts[$id];
                goto oy56;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-02-26": ',$counts[$id];
                goto oy57;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-02-27": ',$counts[$id];
                goto oy58;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-02-28": ',$counts[$id];
                goto oy59;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-02-29": ',$counts[$id];
                goto oy60;
            }
            $id += 2;
            if ($counts[++$id]) {
                echo '        "',$year,'-03-01": ',$counts[$id];
                goto oy63;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-03-02": ',$counts[$id];
                goto oy64;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-03-03": ',$counts[$id];
                goto oy65;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-03-04": ',$counts[$id];
                goto oy66;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-03-05": ',$counts[$id];
                goto oy67;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-03-06": ',$counts[$id];
                goto oy68;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-03-07": ',$counts[$id];
                goto oy69;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-03-08": ',$counts[$id];
                goto oy70;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-03-09": ',$counts[$id];
                goto oy71;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-03-10": ',$counts[$id];
                goto oy72;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-03-11": ',$counts[$id];
                goto oy73;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-03-12": ',$counts[$id];
                goto oy74;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-03-13": ',$counts[$id];
                goto oy75;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-03-14": ',$counts[$id];
                goto oy76;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-03-15": ',$counts[$id];
                goto oy77;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-03-16": ',$counts[$id];
                goto oy78;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-03-17": ',$counts[$id];
                goto oy79;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-03-18": ',$counts[$id];
                goto oy80;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-03-19": ',$counts[$id];
                goto oy81;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-03-20": ',$counts[$id];
                goto oy82;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-03-21": ',$counts[$id];
                goto oy83;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-03-22": ',$counts[$id];
                goto oy84;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-03-23": ',$counts[$id];
                goto oy85;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-03-24": ',$counts[$id];
                goto oy86;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-03-25": ',$counts[$id];
                goto oy87;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-03-26": ',$counts[$id];
                goto oy88;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-03-27": ',$counts[$id];
                goto oy89;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-03-28": ',$counts[$id];
                goto oy90;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-03-29": ',$counts[$id];
                goto oy91;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-03-30": ',$counts[$id];
                goto oy92;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-03-31": ',$counts[$id];
                goto oy93;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-04-01": ',$counts[$id];
                goto oy94;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-04-02": ',$counts[$id];
                goto oy95;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-04-03": ',$counts[$id];
                goto oy96;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-04-04": ',$counts[$id];
                goto oy97;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-04-05": ',$counts[$id];
                goto oy98;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-04-06": ',$counts[$id];
                goto oy99;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-04-07": ',$counts[$id];
                goto oy100;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-04-08": ',$counts[$id];
                goto oy101;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-04-09": ',$counts[$id];
                goto oy102;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-04-10": ',$counts[$id];
                goto oy103;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-04-11": ',$counts[$id];
                goto oy104;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-04-12": ',$counts[$id];
                goto oy105;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-04-13": ',$counts[$id];
                goto oy106;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-04-14": ',$counts[$id];
                goto oy107;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-04-15": ',$counts[$id];
                goto oy108;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-04-16": ',$counts[$id];
                goto oy109;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-04-17": ',$counts[$id];
                goto oy110;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-04-18": ',$counts[$id];
                goto oy111;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-04-19": ',$counts[$id];
                goto oy112;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-04-20": ',$counts[$id];
                goto oy113;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-04-21": ',$counts[$id];
                goto oy114;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-04-22": ',$counts[$id];
                goto oy115;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-04-23": ',$counts[$id];
                goto oy116;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-04-24": ',$counts[$id];
                goto oy117;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-04-25": ',$counts[$id];
                goto oy118;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-04-26": ',$counts[$id];
                goto oy119;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-04-27": ',$counts[$id];
                goto oy120;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-04-28": ',$counts[$id];
                goto oy121;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-04-29": ',$counts[$id];
                goto oy122;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-04-30": ',$counts[$id];
                goto oy123;
            }
            ++$id;
            if ($counts[++$id]) {
                echo '        "',$year,'-05-01": ',$counts[$id];
                goto oy125;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-05-02": ',$counts[$id];
                goto oy126;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-05-03": ',$counts[$id];
                goto oy127;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-05-04": ',$counts[$id];
                goto oy128;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-05-05": ',$counts[$id];
                goto oy129;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-05-06": ',$counts[$id];
                goto oy130;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-05-07": ',$counts[$id];
                goto oy131;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-05-08": ',$counts[$id];
                goto oy132;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-05-09": ',$counts[$id];
                goto oy133;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-05-10": ',$counts[$id];
                goto oy134;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-05-11": ',$counts[$id];
                goto oy135;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-05-12": ',$counts[$id];
                goto oy136;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-05-13": ',$counts[$id];
                goto oy137;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-05-14": ',$counts[$id];
                goto oy138;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-05-15": ',$counts[$id];
                goto oy139;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-05-16": ',$counts[$id];
                goto oy140;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-05-17": ',$counts[$id];
                goto oy141;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-05-18": ',$counts[$id];
                goto oy142;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-05-19": ',$counts[$id];
                goto oy143;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-05-20": ',$counts[$id];
                goto oy144;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-05-21": ',$counts[$id];
                goto oy145;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-05-22": ',$counts[$id];
                goto oy146;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-05-23": ',$counts[$id];
                goto oy147;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-05-24": ',$counts[$id];
                goto oy148;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-05-25": ',$counts[$id];
                goto oy149;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-05-26": ',$counts[$id];
                goto oy150;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-05-27": ',$counts[$id];
                goto oy151;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-05-28": ',$counts[$id];
                goto oy152;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-05-29": ',$counts[$id];
                goto oy153;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-05-30": ',$counts[$id];
                goto oy154;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-05-31": ',$counts[$id];
                goto oy155;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-06-01": ',$counts[$id];
                goto oy156;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-06-02": ',$counts[$id];
                goto oy157;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-06-03": ',$counts[$id];
                goto oy158;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-06-04": ',$counts[$id];
                goto oy159;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-06-05": ',$counts[$id];
                goto oy160;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-06-06": ',$counts[$id];
                goto oy161;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-06-07": ',$counts[$id];
                goto oy162;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-06-08": ',$counts[$id];
                goto oy163;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-06-09": ',$counts[$id];
                goto oy164;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-06-10": ',$counts[$id];
                goto oy165;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-06-11": ',$counts[$id];
                goto oy166;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-06-12": ',$counts[$id];
                goto oy167;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-06-13": ',$counts[$id];
                goto oy168;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-06-14": ',$counts[$id];
                goto oy169;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-06-15": ',$counts[$id];
                goto oy170;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-06-16": ',$counts[$id];
                goto oy171;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-06-17": ',$counts[$id];
                goto oy172;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-06-18": ',$counts[$id];
                goto oy173;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-06-19": ',$counts[$id];
                goto oy174;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-06-20": ',$counts[$id];
                goto oy175;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-06-21": ',$counts[$id];
                goto oy176;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-06-22": ',$counts[$id];
                goto oy177;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-06-23": ',$counts[$id];
                goto oy178;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-06-24": ',$counts[$id];
                goto oy179;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-06-25": ',$counts[$id];
                goto oy180;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-06-26": ',$counts[$id];
                goto oy181;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-06-27": ',$counts[$id];
                goto oy182;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-06-28": ',$counts[$id];
                goto oy183;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-06-29": ',$counts[$id];
                goto oy184;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-06-30": ',$counts[$id];
                goto oy185;
            }
            ++$id;
            if ($counts[++$id]) {
                echo '        "',$year,'-07-01": ',$counts[$id];
                goto oy187;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-07-02": ',$counts[$id];
                goto oy188;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-07-03": ',$counts[$id];
                goto oy189;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-07-04": ',$counts[$id];
                goto oy190;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-07-05": ',$counts[$id];
                goto oy191;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-07-06": ',$counts[$id];
                goto oy192;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-07-07": ',$counts[$id];
                goto oy193;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-07-08": ',$counts[$id];
                goto oy194;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-07-09": ',$counts[$id];
                goto oy195;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-07-10": ',$counts[$id];
                goto oy196;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-07-11": ',$counts[$id];
                goto oy197;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-07-12": ',$counts[$id];
                goto oy198;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-07-13": ',$counts[$id];
                goto oy199;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-07-14": ',$counts[$id];
                goto oy200;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-07-15": ',$counts[$id];
                goto oy201;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-07-16": ',$counts[$id];
                goto oy202;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-07-17": ',$counts[$id];
                goto oy203;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-07-18": ',$counts[$id];
                goto oy204;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-07-19": ',$counts[$id];
                goto oy205;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-07-20": ',$counts[$id];
                goto oy206;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-07-21": ',$counts[$id];
                goto oy207;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-07-22": ',$counts[$id];
                goto oy208;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-07-23": ',$counts[$id];
                goto oy209;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-07-24": ',$counts[$id];
                goto oy210;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-07-25": ',$counts[$id];
                goto oy211;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-07-26": ',$counts[$id];
                goto oy212;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-07-27": ',$counts[$id];
                goto oy213;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-07-28": ',$counts[$id];
                goto oy214;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-07-29": ',$counts[$id];
                goto oy215;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-07-30": ',$counts[$id];
                goto oy216;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-07-31": ',$counts[$id];
                goto oy217;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-08-01": ',$counts[$id];
                goto oy218;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-08-02": ',$counts[$id];
                goto oy219;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-08-03": ',$counts[$id];
                goto oy220;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-08-04": ',$counts[$id];
                goto oy221;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-08-05": ',$counts[$id];
                goto oy222;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-08-06": ',$counts[$id];
                goto oy223;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-08-07": ',$counts[$id];
                goto oy224;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-08-08": ',$counts[$id];
                goto oy225;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-08-09": ',$counts[$id];
                goto oy226;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-08-10": ',$counts[$id];
                goto oy227;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-08-11": ',$counts[$id];
                goto oy228;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-08-12": ',$counts[$id];
                goto oy229;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-08-13": ',$counts[$id];
                goto oy230;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-08-14": ',$counts[$id];
                goto oy231;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-08-15": ',$counts[$id];
                goto oy232;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-08-16": ',$counts[$id];
                goto oy233;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-08-17": ',$counts[$id];
                goto oy234;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-08-18": ',$counts[$id];
                goto oy235;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-08-19": ',$counts[$id];
                goto oy236;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-08-20": ',$counts[$id];
                goto oy237;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-08-21": ',$counts[$id];
                goto oy238;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-08-22": ',$counts[$id];
                goto oy239;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-08-23": ',$counts[$id];
                goto oy240;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-08-24": ',$counts[$id];
                goto oy241;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-08-25": ',$counts[$id];
                goto oy242;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-08-26": ',$counts[$id];
                goto oy243;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-08-27": ',$counts[$id];
                goto oy244;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-08-28": ',$counts[$id];
                goto oy245;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-08-29": ',$counts[$id];
                goto oy246;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-08-30": ',$counts[$id];
                goto oy247;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-08-31": ',$counts[$id];
                goto oy248;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-09-01": ',$counts[$id];
                goto oy249;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-09-02": ',$counts[$id];
                goto oy250;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-09-03": ',$counts[$id];
                goto oy251;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-09-04": ',$counts[$id];
                goto oy252;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-09-05": ',$counts[$id];
                goto oy253;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-09-06": ',$counts[$id];
                goto oy254;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-09-07": ',$counts[$id];
                goto oy255;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-09-08": ',$counts[$id];
                goto oy256;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-09-09": ',$counts[$id];
                goto oy257;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-09-10": ',$counts[$id];
                goto oy258;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-09-11": ',$counts[$id];
                goto oy259;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-09-12": ',$counts[$id];
                goto oy260;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-09-13": ',$counts[$id];
                goto oy261;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-09-14": ',$counts[$id];
                goto oy262;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-09-15": ',$counts[$id];
                goto oy263;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-09-16": ',$counts[$id];
                goto oy264;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-09-17": ',$counts[$id];
                goto oy265;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-09-18": ',$counts[$id];
                goto oy266;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-09-19": ',$counts[$id];
                goto oy267;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-09-20": ',$counts[$id];
                goto oy268;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-09-21": ',$counts[$id];
                goto oy269;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-09-22": ',$counts[$id];
                goto oy270;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-09-23": ',$counts[$id];
                goto oy271;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-09-24": ',$counts[$id];
                goto oy272;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-09-25": ',$counts[$id];
                goto oy273;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-09-26": ',$counts[$id];
                goto oy274;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-09-27": ',$counts[$id];
                goto oy275;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-09-28": ',$counts[$id];
                goto oy276;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-09-29": ',$counts[$id];
                goto oy277;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-09-30": ',$counts[$id];
                goto oy278;
            }
            ++$id;
            if ($counts[++$id]) {
                echo '        "',$year,'-10-01": ',$counts[$id];
                goto oy280;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-10-02": ',$counts[$id];
                goto oy281;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-10-03": ',$counts[$id];
                goto oy282;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-10-04": ',$counts[$id];
                goto oy283;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-10-05": ',$counts[$id];
                goto oy284;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-10-06": ',$counts[$id];
                goto oy285;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-10-07": ',$counts[$id];
                goto oy286;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-10-08": ',$counts[$id];
                goto oy287;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-10-09": ',$counts[$id];
                goto oy288;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-10-10": ',$counts[$id];
                goto oy289;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-10-11": ',$counts[$id];
                goto oy290;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-10-12": ',$counts[$id];
                goto oy291;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-10-13": ',$counts[$id];
                goto oy292;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-10-14": ',$counts[$id];
                goto oy293;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-10-15": ',$counts[$id];
                goto oy294;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-10-16": ',$counts[$id];
                goto oy295;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-10-17": ',$counts[$id];
                goto oy296;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-10-18": ',$counts[$id];
                goto oy297;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-10-19": ',$counts[$id];
                goto oy298;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-10-20": ',$counts[$id];
                goto oy299;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-10-21": ',$counts[$id];
                goto oy300;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-10-22": ',$counts[$id];
                goto oy301;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-10-23": ',$counts[$id];
                goto oy302;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-10-24": ',$counts[$id];
                goto oy303;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-10-25": ',$counts[$id];
                goto oy304;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-10-26": ',$counts[$id];
                goto oy305;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-10-27": ',$counts[$id];
                goto oy306;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-10-28": ',$counts[$id];
                goto oy307;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-10-29": ',$counts[$id];
                goto oy308;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-10-30": ',$counts[$id];
                goto oy309;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-10-31": ',$counts[$id];
                goto oy310;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-11-01": ',$counts[$id];
                goto oy311;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-11-02": ',$counts[$id];
                goto oy312;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-11-03": ',$counts[$id];
                goto oy313;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-11-04": ',$counts[$id];
                goto oy314;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-11-05": ',$counts[$id];
                goto oy315;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-11-06": ',$counts[$id];
                goto oy316;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-11-07": ',$counts[$id];
                goto oy317;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-11-08": ',$counts[$id];
                goto oy318;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-11-09": ',$counts[$id];
                goto oy319;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-11-10": ',$counts[$id];
                goto oy320;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-11-11": ',$counts[$id];
                goto oy321;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-11-12": ',$counts[$id];
                goto oy322;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-11-13": ',$counts[$id];
                goto oy323;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-11-14": ',$counts[$id];
                goto oy324;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-11-15": ',$counts[$id];
                goto oy325;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-11-16": ',$counts[$id];
                goto oy326;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-11-17": ',$counts[$id];
                goto oy327;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-11-18": ',$counts[$id];
                goto oy328;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-11-19": ',$counts[$id];
                goto oy329;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-11-20": ',$counts[$id];
                goto oy330;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-11-21": ',$counts[$id];
                goto oy331;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-11-22": ',$counts[$id];
                goto oy332;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-11-23": ',$counts[$id];
                goto oy333;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-11-24": ',$counts[$id];
                goto oy334;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-11-25": ',$counts[$id];
                goto oy335;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-11-26": ',$counts[$id];
                goto oy336;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-11-27": ',$counts[$id];
                goto oy337;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-11-28": ',$counts[$id];
                goto oy338;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-11-29": ',$counts[$id];
                goto oy339;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-11-30": ',$counts[$id];
                goto oy340;
            }
            ++$id;
            if ($counts[++$id]) {
                echo '        "',$year,'-12-01": ',$counts[$id];
                goto oy342;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-12-02": ',$counts[$id];
                goto oy343;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-12-03": ',$counts[$id];
                goto oy344;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-12-04": ',$counts[$id];
                goto oy345;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-12-05": ',$counts[$id];
                goto oy346;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-12-06": ',$counts[$id];
                goto oy347;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-12-07": ',$counts[$id];
                goto oy348;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-12-08": ',$counts[$id];
                goto oy349;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-12-09": ',$counts[$id];
                goto oy350;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-12-10": ',$counts[$id];
                goto oy351;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-12-11": ',$counts[$id];
                goto oy352;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-12-12": ',$counts[$id];
                goto oy353;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-12-13": ',$counts[$id];
                goto oy354;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-12-14": ',$counts[$id];
                goto oy355;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-12-15": ',$counts[$id];
                goto oy356;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-12-16": ',$counts[$id];
                goto oy357;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-12-17": ',$counts[$id];
                goto oy358;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-12-18": ',$counts[$id];
                goto oy359;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-12-19": ',$counts[$id];
                goto oy360;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-12-20": ',$counts[$id];
                goto oy361;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-12-21": ',$counts[$id];
                goto oy362;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-12-22": ',$counts[$id];
                goto oy363;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-12-23": ',$counts[$id];
                goto oy364;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-12-24": ',$counts[$id];
                goto oy365;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-12-25": ',$counts[$id];
                goto oy366;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-12-26": ',$counts[$id];
                goto oy367;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-12-27": ',$counts[$id];
                goto oy368;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-12-28": ',$counts[$id];
                goto oy369;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-12-29": ',$counts[$id];
                goto oy370;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-12-30": ',$counts[$id];
                goto oy371;
            }
            if ($counts[++$id]) {
                echo '        "',$year,'-12-31": ',$counts[$id];
                goto oy372;
            }

            if(++$year <= 2026) goto o1;
            continue;

o2:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-01-01": ',$counts[$id];
            }
            oy1:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-01-02": ',$counts[$id];
            }
            oy2:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-01-03": ',$counts[$id];
            }
            oy3:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-01-04": ',$counts[$id];
            }
            oy4:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-01-05": ',$counts[$id];
            }
            oy5:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-01-06": ',$counts[$id];
            }
            oy6:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-01-07": ',$counts[$id];
            }
            oy7:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-01-08": ',$counts[$id];
            }
            oy8:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-01-09": ',$counts[$id];
            }
            oy9:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-01-10": ',$counts[$id];
            }
            oy10:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-01-11": ',$counts[$id];
            }
            oy11:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-01-12": ',$counts[$id];
            }
            oy12:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-01-13": ',$counts[$id];
            }
            oy13:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-01-14": ',$counts[$id];
            }
            oy14:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-01-15": ',$counts[$id];
            }
            oy15:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-01-16": ',$counts[$id];
            }
            oy16:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-01-17": ',$counts[$id];
            }
            oy17:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-01-18": ',$counts[$id];
            }
            oy18:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-01-19": ',$counts[$id];
            }
            oy19:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-01-20": ',$counts[$id];
            }
            oy20:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-01-21": ',$counts[$id];
            }
            oy21:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-01-22": ',$counts[$id];
            }
            oy22:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-01-23": ',$counts[$id];
            }
            oy23:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-01-24": ',$counts[$id];
            }
            oy24:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-01-25": ',$counts[$id];
            }
            oy25:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-01-26": ',$counts[$id];
            }
            oy26:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-01-27": ',$counts[$id];
            }
            oy27:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-01-28": ',$counts[$id];
            }
            oy28:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-01-29": ',$counts[$id];
            }
            oy29:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-01-30": ',$counts[$id];
            }
            oy30:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-01-31": ',$counts[$id];
            }
            oy31:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-02-01": ',$counts[$id];
            }
            oy32:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-02-02": ',$counts[$id];
            }
            oy33:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-02-03": ',$counts[$id];
            }
            oy34:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-02-04": ',$counts[$id];
            }
            oy35:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-02-05": ',$counts[$id];
            }
            oy36:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-02-06": ',$counts[$id];
            }
            oy37:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-02-07": ',$counts[$id];
            }
            oy38:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-02-08": ',$counts[$id];
            }
            oy39:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-02-09": ',$counts[$id];
            }
            oy40:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-02-10": ',$counts[$id];
            }
            oy41:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-02-11": ',$counts[$id];
            }
            oy42:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-02-12": ',$counts[$id];
            }
            oy43:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-02-13": ',$counts[$id];
            }
            oy44:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-02-14": ',$counts[$id];
            }
            oy45:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-02-15": ',$counts[$id];
            }
            oy46:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-02-16": ',$counts[$id];
            }
            oy47:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-02-17": ',$counts[$id];
            }
            oy48:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-02-18": ',$counts[$id];
            }
            oy49:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-02-19": ',$counts[$id];
            }
            oy50:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-02-20": ',$counts[$id];
            }
            oy51:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-02-21": ',$counts[$id];
            }
            oy52:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-02-22": ',$counts[$id];
            }
            oy53:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-02-23": ',$counts[$id];
            }
            oy54:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-02-24": ',$counts[$id];
            }
            oy55:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-02-25": ',$counts[$id];
            }
            oy56:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-02-26": ',$counts[$id];
            }
            oy57:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-02-27": ',$counts[$id];
            }
            oy58:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-02-28": ',$counts[$id];
            }
            oy59:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-02-29": ',$counts[$id];
            }
            oy60:
            $id += 2;
            oy62:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-03-01": ',$counts[$id];
            }
            oy63:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-03-02": ',$counts[$id];
            }
            oy64:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-03-03": ',$counts[$id];
            }
            oy65:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-03-04": ',$counts[$id];
            }
            oy66:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-03-05": ',$counts[$id];
            }
            oy67:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-03-06": ',$counts[$id];
            }
            oy68:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-03-07": ',$counts[$id];
            }
            oy69:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-03-08": ',$counts[$id];
            }
            oy70:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-03-09": ',$counts[$id];
            }
            oy71:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-03-10": ',$counts[$id];
            }
            oy72:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-03-11": ',$counts[$id];
            }
            oy73:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-03-12": ',$counts[$id];
            }
            oy74:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-03-13": ',$counts[$id];
            }
            oy75:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-03-14": ',$counts[$id];
            }
            oy76:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-03-15": ',$counts[$id];
            }
            oy77:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-03-16": ',$counts[$id];
            }
            oy78:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-03-17": ',$counts[$id];
            }
            oy79:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-03-18": ',$counts[$id];
            }
            oy80:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-03-19": ',$counts[$id];
            }
            oy81:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-03-20": ',$counts[$id];
            }
            oy82:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-03-21": ',$counts[$id];
            }
            oy83:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-03-22": ',$counts[$id];
            }
            oy84:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-03-23": ',$counts[$id];
            }
            oy85:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-03-24": ',$counts[$id];
            }
            oy86:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-03-25": ',$counts[$id];
            }
            oy87:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-03-26": ',$counts[$id];
            }
            oy88:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-03-27": ',$counts[$id];
            }
            oy89:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-03-28": ',$counts[$id];
            }
            oy90:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-03-29": ',$counts[$id];
            }
            oy91:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-03-30": ',$counts[$id];
            }
            oy92:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-03-31": ',$counts[$id];
            }
            oy93:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-04-01": ',$counts[$id];
            }
            oy94:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-04-02": ',$counts[$id];
            }
            oy95:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-04-03": ',$counts[$id];
            }
            oy96:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-04-04": ',$counts[$id];
            }
            oy97:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-04-05": ',$counts[$id];
            }
            oy98:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-04-06": ',$counts[$id];
            }
            oy99:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-04-07": ',$counts[$id];
            }
            oy100:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-04-08": ',$counts[$id];
            }
            oy101:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-04-09": ',$counts[$id];
            }
            oy102:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-04-10": ',$counts[$id];
            }
            oy103:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-04-11": ',$counts[$id];
            }
            oy104:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-04-12": ',$counts[$id];
            }
            oy105:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-04-13": ',$counts[$id];
            }
            oy106:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-04-14": ',$counts[$id];
            }
            oy107:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-04-15": ',$counts[$id];
            }
            oy108:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-04-16": ',$counts[$id];
            }
            oy109:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-04-17": ',$counts[$id];
            }
            oy110:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-04-18": ',$counts[$id];
            }
            oy111:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-04-19": ',$counts[$id];
            }
            oy112:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-04-20": ',$counts[$id];
            }
            oy113:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-04-21": ',$counts[$id];
            }
            oy114:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-04-22": ',$counts[$id];
            }
            oy115:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-04-23": ',$counts[$id];
            }
            oy116:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-04-24": ',$counts[$id];
            }
            oy117:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-04-25": ',$counts[$id];
            }
            oy118:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-04-26": ',$counts[$id];
            }
            oy119:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-04-27": ',$counts[$id];
            }
            oy120:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-04-28": ',$counts[$id];
            }
            oy121:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-04-29": ',$counts[$id];
            }
            oy122:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-04-30": ',$counts[$id];
            }
            oy123:
            ++$id;
            oy124:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-05-01": ',$counts[$id];
            }
            oy125:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-05-02": ',$counts[$id];
            }
            oy126:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-05-03": ',$counts[$id];
            }
            oy127:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-05-04": ',$counts[$id];
            }
            oy128:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-05-05": ',$counts[$id];
            }
            oy129:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-05-06": ',$counts[$id];
            }
            oy130:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-05-07": ',$counts[$id];
            }
            oy131:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-05-08": ',$counts[$id];
            }
            oy132:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-05-09": ',$counts[$id];
            }
            oy133:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-05-10": ',$counts[$id];
            }
            oy134:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-05-11": ',$counts[$id];
            }
            oy135:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-05-12": ',$counts[$id];
            }
            oy136:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-05-13": ',$counts[$id];
            }
            oy137:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-05-14": ',$counts[$id];
            }
            oy138:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-05-15": ',$counts[$id];
            }
            oy139:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-05-16": ',$counts[$id];
            }
            oy140:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-05-17": ',$counts[$id];
            }
            oy141:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-05-18": ',$counts[$id];
            }
            oy142:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-05-19": ',$counts[$id];
            }
            oy143:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-05-20": ',$counts[$id];
            }
            oy144:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-05-21": ',$counts[$id];
            }
            oy145:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-05-22": ',$counts[$id];
            }
            oy146:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-05-23": ',$counts[$id];
            }
            oy147:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-05-24": ',$counts[$id];
            }
            oy148:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-05-25": ',$counts[$id];
            }
            oy149:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-05-26": ',$counts[$id];
            }
            oy150:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-05-27": ',$counts[$id];
            }
            oy151:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-05-28": ',$counts[$id];
            }
            oy152:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-05-29": ',$counts[$id];
            }
            oy153:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-05-30": ',$counts[$id];
            }
            oy154:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-05-31": ',$counts[$id];
            }
            oy155:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-06-01": ',$counts[$id];
            }
            oy156:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-06-02": ',$counts[$id];
            }
            oy157:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-06-03": ',$counts[$id];
            }
            oy158:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-06-04": ',$counts[$id];
            }
            oy159:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-06-05": ',$counts[$id];
            }
            oy160:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-06-06": ',$counts[$id];
            }
            oy161:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-06-07": ',$counts[$id];
            }
            oy162:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-06-08": ',$counts[$id];
            }
            oy163:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-06-09": ',$counts[$id];
            }
            oy164:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-06-10": ',$counts[$id];
            }
            oy165:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-06-11": ',$counts[$id];
            }
            oy166:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-06-12": ',$counts[$id];
            }
            oy167:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-06-13": ',$counts[$id];
            }
            oy168:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-06-14": ',$counts[$id];
            }
            oy169:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-06-15": ',$counts[$id];
            }
            oy170:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-06-16": ',$counts[$id];
            }
            oy171:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-06-17": ',$counts[$id];
            }
            oy172:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-06-18": ',$counts[$id];
            }
            oy173:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-06-19": ',$counts[$id];
            }
            oy174:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-06-20": ',$counts[$id];
            }
            oy175:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-06-21": ',$counts[$id];
            }
            oy176:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-06-22": ',$counts[$id];
            }
            oy177:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-06-23": ',$counts[$id];
            }
            oy178:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-06-24": ',$counts[$id];
            }
            oy179:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-06-25": ',$counts[$id];
            }
            oy180:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-06-26": ',$counts[$id];
            }
            oy181:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-06-27": ',$counts[$id];
            }
            oy182:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-06-28": ',$counts[$id];
            }
            oy183:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-06-29": ',$counts[$id];
            }
            oy184:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-06-30": ',$counts[$id];
            }
            oy185:
            ++$id;
            oy186:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-07-01": ',$counts[$id];
            }
            oy187:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-07-02": ',$counts[$id];
            }
            oy188:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-07-03": ',$counts[$id];
            }
            oy189:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-07-04": ',$counts[$id];
            }
            oy190:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-07-05": ',$counts[$id];
            }
            oy191:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-07-06": ',$counts[$id];
            }
            oy192:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-07-07": ',$counts[$id];
            }
            oy193:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-07-08": ',$counts[$id];
            }
            oy194:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-07-09": ',$counts[$id];
            }
            oy195:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-07-10": ',$counts[$id];
            }
            oy196:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-07-11": ',$counts[$id];
            }
            oy197:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-07-12": ',$counts[$id];
            }
            oy198:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-07-13": ',$counts[$id];
            }
            oy199:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-07-14": ',$counts[$id];
            }
            oy200:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-07-15": ',$counts[$id];
            }
            oy201:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-07-16": ',$counts[$id];
            }
            oy202:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-07-17": ',$counts[$id];
            }
            oy203:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-07-18": ',$counts[$id];
            }
            oy204:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-07-19": ',$counts[$id];
            }
            oy205:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-07-20": ',$counts[$id];
            }
            oy206:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-07-21": ',$counts[$id];
            }
            oy207:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-07-22": ',$counts[$id];
            }
            oy208:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-07-23": ',$counts[$id];
            }
            oy209:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-07-24": ',$counts[$id];
            }
            oy210:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-07-25": ',$counts[$id];
            }
            oy211:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-07-26": ',$counts[$id];
            }
            oy212:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-07-27": ',$counts[$id];
            }
            oy213:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-07-28": ',$counts[$id];
            }
            oy214:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-07-29": ',$counts[$id];
            }
            oy215:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-07-30": ',$counts[$id];
            }
            oy216:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-07-31": ',$counts[$id];
            }
            oy217:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-08-01": ',$counts[$id];
            }
            oy218:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-08-02": ',$counts[$id];
            }
            oy219:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-08-03": ',$counts[$id];
            }
            oy220:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-08-04": ',$counts[$id];
            }
            oy221:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-08-05": ',$counts[$id];
            }
            oy222:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-08-06": ',$counts[$id];
            }
            oy223:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-08-07": ',$counts[$id];
            }
            oy224:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-08-08": ',$counts[$id];
            }
            oy225:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-08-09": ',$counts[$id];
            }
            oy226:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-08-10": ',$counts[$id];
            }
            oy227:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-08-11": ',$counts[$id];
            }
            oy228:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-08-12": ',$counts[$id];
            }
            oy229:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-08-13": ',$counts[$id];
            }
            oy230:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-08-14": ',$counts[$id];
            }
            oy231:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-08-15": ',$counts[$id];
            }
            oy232:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-08-16": ',$counts[$id];
            }
            oy233:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-08-17": ',$counts[$id];
            }
            oy234:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-08-18": ',$counts[$id];
            }
            oy235:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-08-19": ',$counts[$id];
            }
            oy236:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-08-20": ',$counts[$id];
            }
            oy237:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-08-21": ',$counts[$id];
            }
            oy238:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-08-22": ',$counts[$id];
            }
            oy239:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-08-23": ',$counts[$id];
            }
            oy240:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-08-24": ',$counts[$id];
            }
            oy241:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-08-25": ',$counts[$id];
            }
            oy242:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-08-26": ',$counts[$id];
            }
            oy243:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-08-27": ',$counts[$id];
            }
            oy244:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-08-28": ',$counts[$id];
            }
            oy245:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-08-29": ',$counts[$id];
            }
            oy246:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-08-30": ',$counts[$id];
            }
            oy247:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-08-31": ',$counts[$id];
            }
            oy248:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-09-01": ',$counts[$id];
            }
            oy249:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-09-02": ',$counts[$id];
            }
            oy250:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-09-03": ',$counts[$id];
            }
            oy251:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-09-04": ',$counts[$id];
            }
            oy252:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-09-05": ',$counts[$id];
            }
            oy253:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-09-06": ',$counts[$id];
            }
            oy254:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-09-07": ',$counts[$id];
            }
            oy255:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-09-08": ',$counts[$id];
            }
            oy256:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-09-09": ',$counts[$id];
            }
            oy257:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-09-10": ',$counts[$id];
            }
            oy258:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-09-11": ',$counts[$id];
            }
            oy259:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-09-12": ',$counts[$id];
            }
            oy260:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-09-13": ',$counts[$id];
            }
            oy261:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-09-14": ',$counts[$id];
            }
            oy262:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-09-15": ',$counts[$id];
            }
            oy263:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-09-16": ',$counts[$id];
            }
            oy264:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-09-17": ',$counts[$id];
            }
            oy265:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-09-18": ',$counts[$id];
            }
            oy266:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-09-19": ',$counts[$id];
            }
            oy267:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-09-20": ',$counts[$id];
            }
            oy268:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-09-21": ',$counts[$id];
            }
            oy269:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-09-22": ',$counts[$id];
            }
            oy270:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-09-23": ',$counts[$id];
            }
            oy271:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-09-24": ',$counts[$id];
            }
            oy272:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-09-25": ',$counts[$id];
            }
            oy273:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-09-26": ',$counts[$id];
            }
            oy274:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-09-27": ',$counts[$id];
            }
            oy275:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-09-28": ',$counts[$id];
            }
            oy276:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-09-29": ',$counts[$id];
            }
            oy277:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-09-30": ',$counts[$id];
            }
            oy278:
            ++$id;
            oy279:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-10-01": ',$counts[$id];
            }
            oy280:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-10-02": ',$counts[$id];
            }
            oy281:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-10-03": ',$counts[$id];
            }
            oy282:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-10-04": ',$counts[$id];
            }
            oy283:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-10-05": ',$counts[$id];
            }
            oy284:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-10-06": ',$counts[$id];
            }
            oy285:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-10-07": ',$counts[$id];
            }
            oy286:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-10-08": ',$counts[$id];
            }
            oy287:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-10-09": ',$counts[$id];
            }
            oy288:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-10-10": ',$counts[$id];
            }
            oy289:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-10-11": ',$counts[$id];
            }
            oy290:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-10-12": ',$counts[$id];
            }
            oy291:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-10-13": ',$counts[$id];
            }
            oy292:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-10-14": ',$counts[$id];
            }
            oy293:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-10-15": ',$counts[$id];
            }
            oy294:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-10-16": ',$counts[$id];
            }
            oy295:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-10-17": ',$counts[$id];
            }
            oy296:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-10-18": ',$counts[$id];
            }
            oy297:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-10-19": ',$counts[$id];
            }
            oy298:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-10-20": ',$counts[$id];
            }
            oy299:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-10-21": ',$counts[$id];
            }
            oy300:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-10-22": ',$counts[$id];
            }
            oy301:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-10-23": ',$counts[$id];
            }
            oy302:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-10-24": ',$counts[$id];
            }
            oy303:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-10-25": ',$counts[$id];
            }
            oy304:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-10-26": ',$counts[$id];
            }
            oy305:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-10-27": ',$counts[$id];
            }
            oy306:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-10-28": ',$counts[$id];
            }
            oy307:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-10-29": ',$counts[$id];
            }
            oy308:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-10-30": ',$counts[$id];
            }
            oy309:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-10-31": ',$counts[$id];
            }
            oy310:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-11-01": ',$counts[$id];
            }
            oy311:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-11-02": ',$counts[$id];
            }
            oy312:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-11-03": ',$counts[$id];
            }
            oy313:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-11-04": ',$counts[$id];
            }
            oy314:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-11-05": ',$counts[$id];
            }
            oy315:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-11-06": ',$counts[$id];
            }
            oy316:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-11-07": ',$counts[$id];
            }
            oy317:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-11-08": ',$counts[$id];
            }
            oy318:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-11-09": ',$counts[$id];
            }
            oy319:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-11-10": ',$counts[$id];
            }
            oy320:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-11-11": ',$counts[$id];
            }
            oy321:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-11-12": ',$counts[$id];
            }
            oy322:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-11-13": ',$counts[$id];
            }
            oy323:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-11-14": ',$counts[$id];
            }
            oy324:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-11-15": ',$counts[$id];
            }
            oy325:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-11-16": ',$counts[$id];
            }
            oy326:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-11-17": ',$counts[$id];
            }
            oy327:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-11-18": ',$counts[$id];
            }
            oy328:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-11-19": ',$counts[$id];
            }
            oy329:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-11-20": ',$counts[$id];
            }
            oy330:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-11-21": ',$counts[$id];
            }
            oy331:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-11-22": ',$counts[$id];
            }
            oy332:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-11-23": ',$counts[$id];
            }
            oy333:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-11-24": ',$counts[$id];
            }
            oy334:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-11-25": ',$counts[$id];
            }
            oy335:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-11-26": ',$counts[$id];
            }
            oy336:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-11-27": ',$counts[$id];
            }
            oy337:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-11-28": ',$counts[$id];
            }
            oy338:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-11-29": ',$counts[$id];
            }
            oy339:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-11-30": ',$counts[$id];
            }
            oy340:
            ++$id;
            oy341:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-12-01": ',$counts[$id];
            }
            oy342:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-12-02": ',$counts[$id];
            }
            oy343:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-12-03": ',$counts[$id];
            }
            oy344:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-12-04": ',$counts[$id];
            }
            oy345:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-12-05": ',$counts[$id];
            }
            oy346:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-12-06": ',$counts[$id];
            }
            oy347:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-12-07": ',$counts[$id];
            }
            oy348:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-12-08": ',$counts[$id];
            }
            oy349:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-12-09": ',$counts[$id];
            }
            oy350:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-12-10": ',$counts[$id];
            }
            oy351:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-12-11": ',$counts[$id];
            }
            oy352:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-12-12": ',$counts[$id];
            }
            oy353:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-12-13": ',$counts[$id];
            }
            oy354:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-12-14": ',$counts[$id];
            }
            oy355:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-12-15": ',$counts[$id];
            }
            oy356:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-12-16": ',$counts[$id];
            }
            oy357:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-12-17": ',$counts[$id];
            }
            oy358:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-12-18": ',$counts[$id];
            }
            oy359:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-12-19": ',$counts[$id];
            }
            oy360:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-12-20": ',$counts[$id];
            }
            oy361:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-12-21": ',$counts[$id];
            }
            oy362:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-12-22": ',$counts[$id];
            }
            oy363:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-12-23": ',$counts[$id];
            }
            oy364:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-12-24": ',$counts[$id];
            }
            oy365:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-12-25": ',$counts[$id];
            }
            oy366:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-12-26": ',$counts[$id];
            }
            oy367:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-12-27": ',$counts[$id];
            }
            oy368:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-12-28": ',$counts[$id];
            }
            oy369:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-12-29": ',$counts[$id];
            }
            oy370:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-12-30": ',$counts[$id];
            }
            oy371:
            if ($counts[++$id]) {
                echo ",\n        \"",$year,'-12-31": ',$counts[$id];
            }

            oy372:
            if(++$year <= 2026) goto o2;

            echo "\n    }";

            \fwrite($fo, $j);
            \fwrite($fo, \ob_get_contents());
            \ob_clean();

            $fu = true;
        }
        \ob_end_clean();

        \fwrite($fo, "\n}");
   }

    public final static function parse(string $inputPath, string $outputPath): void
    {
        \gc_disable();
        \ini_set('memory_limit', '-1');
        \set_time_limit(0);

        list($partialIds, $sequence, $uriIds, $dateIds, $counts) = self::initialize();

        $fo = \fopen($outputPath, 'wb');
        if ($fo === false) throw new \Exception('Output file could not be created: '.$outputPath);
        \stream_set_write_buffer($fo, self::OUTPUT_BUFFER_SIZE);
        self::$keepAlive[] = &$fo;

        $f = \fopen($inputPath, 'rb');
        if (false === $f) throw new \Exception('Input file could not be opened: '.$inputPath);
        \stream_set_read_buffer($f, 0);
        \stream_set_chunk_size($f, self::CHUNK_SIZE);
        self::$keepAlive[] = &$f;

        $b = \fread($f, self::BUFFER_SIZE);
        $bp = 25;

        $sequenceRem = \count($sequence);
        $sequenceId = 0;

s1:
        $bm = \strrpos($b, "\n");
        do {
            $i = \strpos($b, ',', $bp);
            ++$counts[($id = $partialIds[\substr($b, $bp, $i-$bp)]) + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
            if (!isset($sequence[$id])) {
                $sequence[$id] = $sequenceId++;
                if (0 === (--$sequenceRem)) {
                    if ($bp>=$bm) goto l1e;
                    goto l1s;
                }
            }
        } while ($bp<$bm);

s1e:
        if ($bp === (25+\strlen($b))) {
            if (0 === \strlen($b = \fread($f, self::BUFFER_SIZE))) goto o0;
            $bp = 25;
            goto s1;
        }

        $bRem = \substr($b, $bp-25);
        $b = \fread($f, self::BUFFER_SIZE);
        $bp = \strpos($b, "\n", 0);
        $bRem .= \substr($b, 0, $bp);
        $bp += 26;

        ++$counts[($id = $partialIds[\substr($bRem, 25, \strlen($bRem)-51)]) + $dateIds[\substr($bRem, \strlen($bRem)-22, 7)]];
        if (!isset($sequence[$id])) {
            $sequence[$id] = $sequenceId++;
            if (0 === (--$sequenceRem)) {
                $bm = \strrpos($b, "\n");
                if ($bp>=$bm) goto l1e;
                goto l1s;
            }
        }

        goto s1;

l1:
        $bm = \strrpos($b, "\n");
l1s:
        do {
            $i = \strpos($b, ',', $bp);
            ++$counts[$partialIds[\substr($b, $bp, $i-$bp)] + $dateIds[\substr($b, 4+$i, 7)]];
            $bp = 52 + $i;
        } while ($bp<$bm);

l1e:
        if ($bp === (25+\strlen($b))) {
            if (0 === \strlen($b = \fread($f, self::BUFFER_SIZE))) goto o0;
            $bp = 25;
            goto l1;
        }

        $bRem = \substr($b, $bp-25);
        $b = \fread($f, self::BUFFER_SIZE);
        $bp = \strpos($b, "\n", 0);
        $bRem .= \substr($b, 0, $bp);
        $bp += 26;

        ++$counts[$partialIds[\substr($bRem, 25, \strlen($bRem)-51)] + $dateIds[\substr($bRem, \strlen($bRem)-22, 7)]];

        goto l1;

o0:
        self::writeJson($fo, $sequence, $uriIds, $counts);

        \fclose($fo);
        \exit();
    }
}