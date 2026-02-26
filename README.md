> [!IMPORTANT]
> The 100-million-row challenge is now **live**. You have until March 15, 11:59PM CET to submit your entry! Check out [leaderboard.csv](./leaderboard.csv) for actual results. Comment on your PR with `/bench` to request a re-run!
    
Welcome to the 100-million-row challenge in PHP! Your goal is to parse a data set of page visits into a JSON file. This repository contains all you need to get started locally. Submitting an entry is as easy as sending a pull request to this repository. This competition will run for two weeks: from Feb 24 to March 15, 2026. When it's done, the top three fastest solutions will win a prize; there's also a dedicated prize for a single-core solution, and a participation prize that everyone can win! 

## Getting started

To submit a solution, you'll have to [fork this repository](https://github.com/tempestphp/100-million-row-challenge/fork), and clone it locally. Once done, install the project dependencies and generate a dataset for local development:

```sh
composer install
php tempest data:generate
```

By default, the `data:generate` command will generate a dataset of 1,000,000 visits. The real benchmark will use 100,000,000 visits. You can adjust the number of visits as well by running `php tempest data:generate 100_000_000`.

Also, the generator will use a seeded randomizer so that, for local development, you work on the same dataset as others. You can overwrite the seed with the `data:generate --seed=123456` parameter, and you can also pass in the `data:generate --no-seed` parameter for an unseeded random data set. The real data set was generated without a seed and is secret.

Next, implement your solution in `app/Parser.php`:

```php
final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        throw new Exception('TODO');
    }
}
```

You can always run your implementation to check your work:

```sh
php tempest data:parse
```

Furthermore, you can validate whether your output file is formatted correctly by running the `data:validate` command. This command will run on a small dataset with a predetermined expected output. If validation succeeds, you can be sure you implemented a working solution:

```sh
php tempest data:validate
```

## Output formatting rules

You'll be parsing millions of CSV lines into a JSON file, with the following rules in mind:

- Each entry in the generated JSON file should be a key-value pair with the page's URL path as the key and an array with the number of visits per day as the value.
- Visits should be sorted by date in ascending order.
- The output should be encoded as a pretty JSON string (as generated with `JSON_PRETTY_PRINT`).

As an example, take the following input:

```csv
https://stitcher.io/blog/11-million-rows-in-seconds,2026-01-24T01:16:58+00:00
https://stitcher.io/blog/php-enums,2024-01-24T01:16:58+00:00
https://stitcher.io/blog/11-million-rows-in-seconds,2026-01-24T01:12:11+00:00
https://stitcher.io/blog/11-million-rows-in-seconds,2025-01-24T01:15:20+00:00
```

Your parser should store the following output in `$outputPath` as a JSON file:

```json
{
    "\/blog\/11-million-rows-in-seconds": {
        "2025-01-24": 1,
        "2026-01-24": 2
    },
    "\/blog\/php-enums": {
        "2024-01-24": 1
    }
}
```

## Submitting your solution

Send a pull request to this repository with your solution. The title of your pull request should simply be your GitHub's username. If your solution validates, we'll run it on the benchmark server and store your time in [leaderboard.csv](./leaderboard.csv). You can continue to improve your solution, but keep in mind that benchmarks are manually triggered, and you might need to wait a while before your results are published.

> [!IMPORTANT]
> You can request a re-run by writing a comment on your PR saying `/bench`. We'll still manually verify whether your submission can be run.

If you rather want to participate in the single-thread challenge, please mention this in your PR so that we can add the [🚂 single thread](https://github.com/tempestphp/100-million-row-challenge/issues?q=state%3Aopen%20label%3A%22%F0%9F%9A%82%20single%20thread%22) label.

## A note on copying other branches

You might be tempted to look for inspiration from other competitors. While we have no means of preventing you from doing that, we will remove submissions that have clearly been copied from other submissions. We validate each submission by hand up front and ask you to come up with an original solution of your own.

## FAQ

#### What can I win?

Prizes are sponsored by [PhpStorm](https://www.jetbrains.com/phpstorm/) and [Tideways](https://tideways.com/). The winners will be determined based on the fastest entries submitted, if two equally fast entries are registered, time of submission will be taken into account.

All entries must be submitted before March 16, 2026 (so you have until March 15, 11:59PM CET to submit). Any entries submitted after the cutoff date won't be taken into account.

**🥇 First place** will get:

- One PhpStorm Elephpant
- One Tideways Elephpant
- One-year JetBrains all-products pack license
- Three-month JetBrains AI Ultimate license
- One-year Tideways Team license

**🥈 Second place** will get:

- One PhpStorm Elephpant
- One Tideways Elephpant
- One-year JetBrains all-products pack license
- Three-month JetBrains AI Ultimate license

**🥉 Third place** will get:

- One PhpStorm Elephpant
- One Tideways Elephpant
- One-year JetBrains all-products pack license

 **🚂 Fastest single-core submission**:

- One PhpStorm Elephpant
- One-year JetBrains all-products pack license

**🏅 Participation prize** — a random winner picked from all entries who will get:

- One PhpStorm Elephpant
- One-year JetBrains all-products pack license

#### Where can I see the results?

The benchmark results of each run are stored in [leaderboard.csv](./leaderboard.csv). 

#### What kind of server is used for the benchmark?

The benchmark runs on a Mac Mini M1 with 12GB of RAM of available memory. These PHP extensions are available:

```txt
bcmath, bz2, calendar, Core, ctype, curl, date, dba, dom, exif, fileinfo, filter, ftp, gd, gettext, gmp, hash, iconv, igbinary, intl, json, ldap, lexbor, libxml, mbstring, mysqli, mysqlnd, odbc, openssl, pcntl, pcre, PDO, pdo_dblib, pdo_mysql, PDO_ODBC, pdo_pgsql, pdo_sqlite, pgsql, Phar, posix, random, readline, Reflection, session, shmop, SimpleXML, snmp, soap, sockets, sodium, SPL, sqlite3, standard, sysvmsg, sysvsem, sysvshm, tidy, tokenizer, uri, xml, xmlreader, xmlwriter, xsl, Zend OPcache, zip, zlib, Zend OPcache
```

#### How to ensure fair results?

Each submission will be manually verified before its benchmark is run on the benchmark server. We'll also only ever run one single submission at a time to prevent any bias in the results. Additionally, we'll use a consistent, dedicated server to run benchmarks on to ensure that the results are comparable.

If needed, multiple runs will be performed for the top submissions, and their average will be compared. When the challenge is done, the top-5 results will be run multiple times, and we'll take their average result to determine the final score. 

Finally, everyone is asked to respect other participant's entries. You can look at others for inspiration (simply because there's no way we can prevent that from happening), but straight-up copying other entries is prohibited. We'll try our best to watch over this. If you run into any issues, feel free to tag @brendt or @xHeaven in the PR comments.

#### Why not one billion?

This challenge was inspired by the [1 billion row challenge in Java](https://github.com/gunnarmorling/1brc). The reason we're using only 100 million rows is because this version has a lot more complexity compared to the Java version (date parsing, JSON encoding, array sorting).

#### What about the JIT?

While testing this challenge, the JIT didn't seem to offer any significant performance boost. Furthermore, on occasion it caused segfaults. This led to the decision for the JIT to be disabled for this challenge.

#### Can I use FFI?

The point of this challenge is to push PHP to its limits. That's why you're not allowed to use FFI.

#### How long should I wait for benchmark results to come in?

We manually verify each submission before running it on the benchmark sever. Depending on our availability, this means possible waiting times. You can mark your PR as ready for a run by adding a comment saying `/bench`.
