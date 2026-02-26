<?php

namespace App;

use function array_fill;
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
use function fwrite;
use function gc_disable;
use function getmypid;
use function ksort;
use function min;
use function pack;
use function pcntl_fork;
use function pcntl_waitpid;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function sys_get_temp_dir;
use function time;
use function unlink;
use function unpack;

final class Parser
{
    // Number of parallel worker processes (one per core on M1)
    private const int THREADS = 8;

    // strlen('https://stitcher.io/blog/') — fixed URL prefix skipped in hot loop
    private const int URL_PREFIX_LEN = 25;

    // fread chunk size in bytes — tune this for M1 memory bandwidth
    private const int BUFFER_SIZE = 2_097_152;

    // strlen('yyyy-mm-dd') — date portion of the flat key
    private const int DATE_LEN = 10;

    // Offset from commaPos to the start of the next line:
    // comma(1) + datetime(25) + \n(1) = 27
    private const int LINE_ADVANCE = 27;

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
        gc_disable();

        $boundaries = $this->calculateBoundaries($inputPath);
        [$dateMap, $dateIndex, $dateCount] = $this->buildDateMap();
        $pathMap = $this->buildPathMap($dateCount);
        $pathCount = count(self::URLS);
        $parentPid = getmypid();
        $childPids = [];

        for ($i = 0; $i < self::THREADS; $i++) {
            $childPid = pcntl_fork();
            if ($childPid === -1)
                exit('Fork failed');

            if ($childPid === 0) {
                gc_disable();
                [$start, $end] = $boundaries[$i];
                $counts = $this->processChunk($inputPath, $start, $end, $pathMap, $dateMap, $pathCount, $dateCount);
                file_put_contents(sys_get_temp_dir()."/csv_{$parentPid}_{$i}.dat", pack('V*', ...$counts));
                exit(0);
            }

            $childPids[] = $childPid;
        }

        foreach ($childPids as $childPid) {
            pcntl_waitpid($childPid, $status);
        }

        $totals = $this->mergePartials($parentPid, $pathCount, $dateCount);
        $result = $this->buildOutput($totals, $dateIndex, $dateCount);
        $this->writeJson($outputPath, $result);
    }

    private function calculateBoundaries(string $inputPath): array
    {
        $filesize = filesize($inputPath);
        $chunkSize = (int) ceil($filesize / self::THREADS);
        $boundaries = [];
        $start = 0;

        $fp = fopen($inputPath, 'rb');
        stream_set_read_buffer($fp, 0);

        for ($i = 0; $i < (self::THREADS - 1); $i++) {
            fseek($fp, $start + $chunkSize);
            fgets($fp);
            $end = ftell($fp);
            $boundaries[] = [$start, $end];
            $start = $end;
        }

        $boundaries[] = [$start, $filesize];
        fclose($fp);

        return $boundaries;
    }

    private function buildDateMap(): array
    {
        $dateMap = [];
        $dateIndex = [];
        $count = 0;

        $from = time() - (6 * 365 * 86400);
        $to = time() + 86400;

        for ($ts = $from; $ts <= $to; $ts += 86400) {
            $date = date('Y-m-d', $ts);
            if (isset($dateMap[$date]))
                continue;

            $dateMap[$date] = $count;
            $dateIndex[$count] = $date;
            $count++;
        }

        return [$dateMap, $dateIndex, $count];
    }

    private function buildPathMap(int $dateCount): array
    {
        $pathMap = [];
        foreach (self::URLS as $id => $slug) {
            $pathMap[$slug] = $id * $dateCount;
        }

        return $pathMap;
    }

    private function processChunk(
        string $inputPath,
        int $start,
        int $end,
        array $pathMap,
        array $dateMap,
        int $pathCount,
        int $dateCount,
    ): array {
        $fp = fopen($inputPath, 'rb');
        stream_set_read_buffer($fp, 0);
        fseek($fp, $start);

        $remaining = $end - $start;
        $counts = array_fill(0, $pathCount * $dateCount, 0);

        while ($remaining > 0) {
            $chunk = fread($fp, min(self::BUFFER_SIZE, $remaining));
            if ($chunk === false || $chunk === '')
                break;

            $lastNewline = strrpos($chunk, "\n");
            $tail = strlen($chunk) - $lastNewline - 1;
            if ($tail > 0)
                fseek($fp, -$tail, SEEK_CUR);
            $remaining -= $lastNewline + 1;

            $pos = 0;
            while ($pos < $lastNewline) {
                $comma = strpos($chunk, ',', $pos + self::URL_PREFIX_LEN);
                $slug = substr($chunk, $pos + self::URL_PREFIX_LEN, $comma - $pos - self::URL_PREFIX_LEN);
                $date = substr($chunk, $comma + 1, self::DATE_LEN);
                $counts[$pathMap[$slug] + $dateMap[$date]]++;
                $pos = $comma + self::LINE_ADVANCE;
            }
        }

        fclose($fp);

        return $counts;
    }

    private function mergePartials(int $parentPid, int $pathCount, int $dateCount): array
    {
        $totals = array_fill(0, $pathCount * $dateCount, 0);
        $tmpDir = sys_get_temp_dir();

        for ($i = 0; $i < self::THREADS; $i++) {
            $file = "{$tmpDir}/csv_{$parentPid}_{$i}.dat";
            $partial = unpack('V*', file_get_contents($file));
            unlink($file);
            foreach ($partial as $j => $count) {
                $totals[$j - 1] += $count;
            }
        }

        return $totals;
    }

    private function buildOutput(array $totals, array $dateIndex, int $dateCount): array
    {
        $result = [];

        foreach (self::URLS as $pathId => $slug) {
            $base = $pathId * $dateCount;
            $dates = [];

            for ($d = 0; $d < $dateCount; $d++) {
                if ($totals[$base + $d] > 0) {
                    $dates[$dateIndex[$d]] = $totals[$base + $d];
                }
            }

            if ($dates !== []) {
                ksort($dates);
                $result[$slug] = $dates;
            }
        }

        return $result;
    }

    private function writeJson(string $outputPath, array $result): void
    {
        $fh = fopen($outputPath, 'wb');
        stream_set_write_buffer($fh, 0);
        $buf = "{\n";
        $firstPath = true;

        foreach ($result as $path => $dates) {
            if (! $firstPath)
                $buf .= ",\n";
            $firstPath = false;

            $buf .= "    \"\/blog\/$path\": {\n";
            $firstDate = true;

            foreach ($dates as $date => $count) {
                if (! $firstDate)
                    $buf .= ",\n";
                $firstDate = false;
                $buf .= "        \"$date\": $count";
            }

            $buf .= "\n    }";

            if (strlen($buf) > 65536) {
                fwrite($fh, $buf);
                $buf = '';
            }
        }

        fwrite($fh, $buf."\n}");
        fclose($fh);
    }
}
