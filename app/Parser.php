<?php

namespace App;

use function ceil;
use function date;
use function fclose;
use function fgets;
use function file_get_contents;
use function file_put_contents;
use function filesize;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function getmypid;
use function igbinary_serialize;
use function igbinary_unserialize;
use function intdiv;
use function ksort;
use function min;
use function pcntl_fork;
use function pcntl_waitpid;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function sys_get_temp_dir;
use function time;
use function unlink;

final class Parser
{
    // Number of parallel worker processes (one per core on M1)
    private const THREADS = 8;

    // strlen('https://stitcher.io/blog/') — fixed URL prefix skipped in hot loop
    private const URL_PREFIX_LEN = 25;

    // fread chunk size in bytes — tune this for M1 memory bandwidth
    private const BUFFER_SIZE = 2_097_152;

    // strlen('yyyy-mm-dd') — date portion of the flat key
    private const DATE_LEN = 10;

    // strlen(',yyyy-mm-dd') — key suffix: comma + date appended to slug in hot loop
    private const DATE_KEY_LEN = 11;

    // Offset from commaPos to the start of the next line:
    // comma(1) + datetime(25) + \n(1) = 27
    private const LINE_ADVANCE = 27;

    // Multiplier for integer key: pathId * PATH_ID_MULTIPLIER + dateId
    // Must exceed max pre-warmed dates; 6-year window = ~2191 days, 2200 gives headroom
    private const PATH_ID_MULTIPLIER = 2200;

    private const array URLS = [
        'shorthand-comparisons-in-php',
        'vendor-locked',
        'front-line-php',
        'light-colour-schemes-are-better',
        'starting-a-newsletter',
        'tackling_responsive_images-part_1',
        'generics-in-php-2',
        'thank-you-kinsta',
        'mysql-show-foreign-key-errors',
        'php-reimagined',
        'php-enum-style-guide',
        'craftsmen-know-their-tools',
        'pipe-operator-in-php-85',
        'php-81-in-8-code-blocks',
        'php-in-2022',
        'improvements-on-laravel-nova',
        'php-8-match-or-switch',
        'merging-multidimensional-arrays-in-php',
        'things-dependency-injection-is-not-about',
        'why-we-need-multi-line-short-closures-in-php',
        'what-about-config-builders',
        'responsive-images-done-right',
        'laravel-queueable-actions',
        'preloading-in-php-74',
        'php-jit',
        'things-considered-harmful',
        'the-case-for-transpiled-generics',
        'mysql-query-logging',
        'the-framework-that-gets-out-of-your-way',
        'service-locator-anti-pattern',
        'share-a-blog-codingwriter-com',
        'type-system-in-php-survey-results',
        'jit-in-real-life-web-applications',
        'php-8-in-8-code-blocks',
        'have-you-thought-about-casing',
        'generics-in-php-3',
        'php-enums',
        'generics-in-php-video',
        'ai-induced-skepticism',
        'new-in-php-82',
        'slashdash',
        'php-version-stats-july-2021',
        'an-event-driven-mindset',
        'php-8-jit-setup',
        'why-do-i-write',
        'php-preload-benchmarks',
        'php-in-2021',
        'acronyms',
        'new-in-php-74',
        'php-in-2020',
        'starting-a-podcast',
        'php-in-2023',
        'a-letter-to-the-php-team-reply-to-joe',
        'laravel-view-models',
        'i-dont-know',
        'override-in-php-83',
        'the-road-to-php',
        'why-we-need-named-params-in-php',
        'the-ikea-effect',
        'analytics-for-developers',
        'whats-your-motivator',
        'array-destructuring-with-list-in-php',
        'optimised-uuids-in-mysql',
        'eloquent-mysql-views',
        'sponsors',
        'php-74-in-7-code-blocks',
        'generics-in-php-1',
        'optimistic-or-realistic-estimates',
        'tabs-are-better',
        'rfc-vote',
        'solid-interfaces-and-final-rant-with-brent',
        'php-8-nullsafe-operator',
        'goodbye',
        'road-to-php-82',
        'my-wishlist-for-php-in-2026',
        'building-a-custom-language-in-tempest-highlight',
        'sponsoring-open-source',
        'dealing-with-dependencies',
        'a-syntax-highlighter-that-doesnt-suck',
        'where-a-curly-bracket-belongs',
        'laravel-view-models-vs-view-composers',
        'php-verse-2025',
        'attribute-usage-in-top-php-packages',
        'php-version-stats-july-2022',
        'php-in-2019',
        'phpstorm-tips-for-power-users',
        'we-dont-need-runtime-type-checks',
        'how-i-plan',
        'new-with-parentheses-php-84',
        '11-million-rows-in-seconds',
        'websites-like-star-wars',
        'new-in-php-81',
        'route-attributes',
        'static-websites-with-tempest',
        'array-find-in-php-84',
        'php-8-upgrade-mac',
        'strategies',
        'what-event-sourcing-is-not-about',
        'deprecating-spatie-dto',
        'you-cannot-find-me-on-mastodon',
        'building-a-framework',
        'tempest-discovery-explained',
        'fibers-with-a-grain-of-salt',
        'simplest-plugin-support',
        'game-changing-editions',
        'organise-by-domain',
        'upgrading-to-php-82',
        'limited-by-committee',
        'cloning-readonly-properties-in-php-81',
        'new-in-php-83',
        'visual-perception-of-code',
        'its-your-fault',
        'php-version-stats-january-2022',
        'stitcher-turns-5',
        'performance-101-building-the-better-web',
        'request-objects-in-tempest',
        'dealing-with-deprecations',
        'php-version-stats-june-2025',
        'stitcher-beta-2',
        'constructor-promotion-in-php-8',
        'announcing-aggregate',
        'laravel-has-many-through',
        'a-year-of-property-hooks',
        'twitter-exit',
        'new-in-php-84',
        'new-in-php-8',
        'flooded-rss',
        'why-curly-brackets-go-on-new-lines',
        'things-i-wish-i-knew',
        'procedurally-generated-game-in-php',
        'php-version-stats-july-2023',
        'a-letter-to-the-php-team',
        'php-version-stats-january-2023',
        'php-generics-and-why-we-need-them',
        'liskov-and-type-safety',
        'evolution-of-a-php-object',
        'cloning-readonly-properties-in-php-83',
        'the-web-in-2045',
        'stitcher-beta-1',
        'tagged-singletons',
        'php-version-stats-january-2024',
        'comparing-dates',
        'php-73-upgrade-mac',
        'new-in-php-85',
        'attributes-in-php-8',
        'php-version-stats-july-2024',
        'unsafe-sql-functions-in-laravel',
        'dont-write-your-own-framework',
        'opinion-driven-design',
        'php-74-upgrade-mac',
        'php-81-new-in-initializers',
        'a-vocal-minority',
        'named-arguments-and-variadic-functions',
        'a-storm-in-a-glass-of-water',
        'event-driven-php',
        'deprecated-dynamic-properties-in-php-82',
        'php-81-performance-in-real-life',
        'readonly-or-private-set',
        'clean-and-minimalistic-phpstorm',
        'mastering-key-bindings',
        'php-2026',
        'its-all-just-text',
        'twitter-home-made-me-miserable',
        'a-programmers-cognitive-load',
        'things-i-learned-writing-a-fiction-novel',
        'type-system-in-php-survey',
        'rational-thinking',
        'php-version-stats-january-2025',
        'my-ikea-clock',
        'thoughts-on-asymmetric-visibility',
        'php-81-upgrade-mac',
        'builders-and-architects-two-types-of-programmers',
        'improved-lazy-loading',
        'short-closures-in-php',
        'a-new-major-version-of-laravel-event-sourcing',
        'how-to-be-right-on-the-internet',
        'phpstorm-performance-issues-on-osx',
        'code-folding',
        'guest-posts',
        'tests-and-types',
        'html-5-in-php-84',
        'array-chunk-in-php',
        'can-i-translate-your-blog',
        'what-are-objects-anyway-rant-with-brent',
        'uncertainty-doubt-and-static-analysis',
        'readonly-classes-in-php-82',
        'php-82-upgrade-mac',
        'process-forks',
        'impact-charts',
        'php-performance-across-versions',
        'object-oriented-generators',
        'unfair-advantage',
        'minor-versions-breaking-changes',
        'acquisition-by-giants',
        'image_optimizers',
        'stitcher-alpha-5',
        'dont-get-stuck',
        'testing-patterns',
        'static_sites_vs_caching',
        'why-light-themes-are-better-according-to-science',
        'array-merge-vs + ',
        'which-editor-to-choose',
        'php-reimagined-part-2',
        'share-a-blog-assertchris-io',
        'a-project-at-spatie',
        'my-journey-into-event-sourcing',
        'when-i-lost-a-few-hundred-leads',
        'you-should',
        'structuring-unstructured-data',
        'share-a-blog-betterwebtype-com',
        'not-optional',
        'tackling_responsive_images-part_2',
        'annotations',
        'passion-projects',
        'braille-and-the-history-of-software',
        'honesty',
        'thoughts-on-event-sourcing',
        'array-objects-with-fixed-types',
        'what-a-good-pr-looks-like',
        'theoretical-engineers',
        'laravel-custom-relation-classes',
        'php-8-before-and-after',
        'is-a-or-acts-as',
        'what-php-can-be',
        'responsive-images-as-css-background',
        'combining-event-sourcing-and-stateful-systems',
        'mysql-import-json-binary-character-set',
        'tagging-tempest-livestream',
        'processing-11-million-rows',
        'phpstorm-scopes',
        'enums-without-enums',
        'bitwise-booleans-in-php',
        'share-a-blog-sebastiandedeyne-com',
        'typed-properties-in-php-74',
        'differences',
        'parallel-php',
        'a-simple-approach-to-static-generation',
        'new-in-php-73',
        'birth-and-death-of-a-framework',
        'all-i-want-for-christmas',
        'asynchronous-php',
        'stitcher-alpha-4',
        'i-dont-code-the-way-i-used-to',
        're-on-using-psr-abstractions',
        'php-in-2024',
        'php-81-readonly-properties',
        'php-81-before-and-after',
        'light-colour-schemes',
        'php-8-named-arguments',
        'php-enums-before-php-81',
        'php-in-2021-video',
        'reducing-code-motion',
        'php-82-in-8-code-blocks',
        'dependency-injection-for-beginners',
        'php-86-partial-function-application',
        'generics-in-php-4',
        'abstract-resources-in-laravel-nova',
        'dont-be-clever',
        'extends-vs-implements',
        'uses',
        'what-about-request-classes',
        'phpstorm-performance-october-2018',
        'phpstorm-performance',
        'what-is-array-plus-in-php',
        'my-10-favourite-php-functions',
        'open-source-strategies',
        'php-84-at-least',
        'php-annotated',
    ];

    public function parse(string $inputPath, string $outputPath): void
    {
        $filesize = filesize($inputPath);
        $tmpDir = sys_get_temp_dir();
        $uid = getmypid();

        $chunkSize = (int) ceil($filesize / self::THREADS);
        $pids = [];

        for ($i = 0; $i < self::THREADS; $i++) {
            $pid = pcntl_fork();
            if ($pid === -1)
                exit('Fork failed');

            if ($pid === 0) {
                // --- CHILD PROCESS ---
                $startByte = $i * $chunkSize;
                $endByte = min(($i + 1) * $chunkSize, $filesize);

                $fp = fopen($inputPath, 'rb');
                fseek($fp, $startByte);
                if ($i > 0) {
                    fseek($fp, $startByte - 1);
                    if (fread($fp, 1) !== "\n")
                        fgets($fp); // skip partial line only if mid-line
                }

                $bytesRemaining = $endByte - ftell($fp);

                // Pre-warm slug map from hardcoded URLS list (ordered to match expected output)
                $slugMap = [];
                $slugRevMap = [];
                foreach (self::URLS as $slugId => $url) {
                    $slugMap[$url] = $slugId;
                    $slugRevMap[$slugId] = $url;
                }

                // Pre-warm date map by enumerating all days in a 6-year window
                // (data generated today spans at most ~1825 days; 6 years gives a safe margin)
                $dateMap = [];
                $dateRevMap = [];
                $dateCount = 0;
                $rangeStart = time() - (6 * 365 * 86400);
                $rangeEnd = time() + 86400;
                for ($ts = $rangeStart; $ts <= $rangeEnd; $ts += 86400) {
                    $d = date('Y-m-d', $ts);
                    if (! isset($dateMap[$d])) {
                        $dateMap[$d] = $dateCount;
                        $dateRevMap[$dateCount] = $d;
                        $dateCount++;
                    }
                }

                // Pre-warm results with all slug×date combinations set to 0
                // so the hot loop can use $results[$intKey]++ without any isset check
                $results = [];
                $slugCount = count($slugMap);
                for ($s = 0; $s < $slugCount; $s++) {
                    for ($d = 0; $d < $dateCount; $d++) {
                        $results[($s * self::PATH_ID_MULTIPLIER) + $d] = 0;
                    }
                }

                $buffer = '';

                while ($bytesRemaining > 0) {
                    $chunk = fread($fp, min(self::BUFFER_SIZE, $bytesRemaining));
                    if ($chunk === false || $chunk === '')
                        break;
                    $bytesRemaining -= strlen($chunk);

                    if ($buffer !== '') {
                        $chunk = $buffer.$chunk;
                        $buffer = '';
                    }

                    $lastNl = strrpos($chunk, "\n");
                    if ($lastNl === false) {
                        $buffer = $chunk;
                        continue;
                    }

                    if ($lastNl < (strlen($chunk) - 1)) {
                        $buffer = substr($chunk, $lastNl + 1);
                    }

                    // All rows: https://stitcher.io/blog/SLUG,yyyy-mm-ddT00:00:00+00:00
                    // Skip URL_PREFIX_LEN chars; integer key = pathId * PATH_ID_MULTIPLIER + dateId
                    // Next line is always at commaPos + LINE_ADVANCE
                    $pos = 0;
                    while ($pos < $lastNl) {
                        $commaPos = strpos($chunk, ',', $pos + self::URL_PREFIX_LEN);
                        $slug = substr($chunk, $pos + self::URL_PREFIX_LEN, $commaPos - $pos - self::URL_PREFIX_LEN);
                        $date = substr($chunk, $commaPos + 1, self::DATE_LEN);

                        $results[($slugMap[$slug] * self::PATH_ID_MULTIPLIER) + $dateMap[$date]]++;
                        $pos = $commaPos + self::LINE_ADVANCE;
                    }
                }

                // Handle remaining partial line at chunk boundary
                if ($buffer !== '') {
                    $rest = fgets($fp);
                    if ($rest !== false)
                        $buffer .= $rest;
                    if (strlen($buffer) > self::URL_PREFIX_LEN) {
                        $commaPos = strpos($buffer, ',', self::URL_PREFIX_LEN);
                        if ($commaPos !== false) {
                            $slug = substr($buffer, self::URL_PREFIX_LEN, $commaPos - self::URL_PREFIX_LEN);
                            $date = substr($buffer, $commaPos + 1, self::DATE_LEN);

                            $results[($slugMap[$slug] * self::PATH_ID_MULTIPLIER) + $dateMap[$date]]++;
                        }
                    }
                }

                fclose($fp);

                // Convert integer keys back to string keys for IPC, skipping pre-warmed zeros
                $stringResults = [];
                foreach ($results as $intKey => $count) {
                    if ($count === 0)
                        continue;
                    $slug = $slugRevMap[intdiv($intKey, self::PATH_ID_MULTIPLIER)];
                    $date = $dateRevMap[$intKey % self::PATH_ID_MULTIPLIER];
                    $stringResults[$slug.','.$date] = $count;
                }

                file_put_contents("{$tmpDir}/csv_{$uid}_{$i}.dat", igbinary_serialize($stringResults));
                exit(0);
            }

            $pids[] = $pid;
        }

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $merged = [];
        for ($i = 0; $i < self::THREADS; $i++) {
            $tempFile = "{$tmpDir}/csv_{$uid}_{$i}.dat";
            /** @var array<string, int> $partial */
            $partial = igbinary_unserialize(file_get_contents($tempFile));
            unlink($tempFile);

            foreach ($partial as $key => $count) {
                if (isset($merged[$key])) {
                    $merged[$key] += $count;
                } else {
                    $merged[$key] = $count;
                }
            }
        }

        $output = [];
        foreach ($merged as $key => $count) {
            $output[substr($key, 0, -self::DATE_KEY_LEN)][substr($key, -self::DATE_LEN)] = $count;
        }

        foreach ($output as &$dates) {
            ksort($dates);
        }

        $this->jsonOutput($outputPath, $output);
    }

    private function jsonOutput(string $outputPath, array $results): void
    {
        $output = "{\n";

        $firstPath = true;
        foreach ($results as $path => $dates) {
            if (! $firstPath) {
                $output .= ",\n";
            }
            $firstPath = false;

            $output .= "    \"\/blog\/$path\": {\n";

            $firstDate = true;
            foreach ($dates as $date => $count) {
                if (! $firstDate) {
                    $output .= ",\n";
                }
                $firstDate = false;
                $output .= "        \"$date\": $count";
            }
            $output .= "\n    }";
        }
        $output .= "\n}";
        file_put_contents($outputPath, $output);
    }
}
