**The challenge isn't currently live, the leaderboard will reset when it officially starts.**
    
Welcome to the 100-million-row challenge in PHP! Your goal is to parse a data set of page visits into a JSON file. This repository contains all you need to get started locally. Submitting an entry is as easy as sending a pull request to this repository. This competition will run for two weeks: from X to Y. When it's done, the top three fastest solutions will win a prize! 

## Getting started

To submit a solution, you'll have to [fork this repository](https://github.com/tempestphp/100-million-row-challenge/fork), and clone it locally. Once done, install the project dependencies and generate a dataset for local development:

```sh
composer install
php tempest data:generate
```

By default, the `data:generate` command will generate a dataset of 1,000,000 visits. The real benchmark will use 100,000,000 visits. Next, implement your solution in `app/Parser.php`:

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

Furthermore, you can validate whether your output file is formatted correctly by running the `data:validate` command:

```sh
php tempest data:validate
```

## Output formatting rules

You'll be parsing millions of CSV lines into a JSON file, with the following rules in mind:

- Each entry in the generated JSON file should be a key-value pair with the page's URL path as the key and an array with the number of visits per day as the value.
- Visits should be sorted by date in ascending order.
- The output should be encoded as a pretty JSON string.

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

## FAQ

#### What can I win?

TODO

#### What kind of server is used for the benchmark?

TODO

#### How to ensure fair results?

Each submission will be manually verified before its benchmark is run on the benchmark server. We'll also only ever run one single submission at a time to prevent any bias in the results. Additionally, we'll use a consistent, dedicated server to run benchmarks on to ensure that the results are comparable.

#### Why not one billion?

This challenge was inspired by the [1 billion row challenge in Java](https://github.com/gunnarmorling/1brc). The reason we're using only 100 million rows is because this version has a lot more complexity compared to the Java version (date parsing, JSON encoding, array sorting). To prevent the benchmark server from being overloaded, we're limiting the number of rows to 100 million.

#### What about the JIT?

While testing this challenge, the JIT didn't seem to offer any significant performance boost. Furthermore, on occasion it caused segfaults. This led to the decision for the JIT to be disabled for this challenge.