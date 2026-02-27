# 100-Million-Row Challenge — Project Guide

## Overview

A PHP performance competition to parse 100 million CSV rows of page visits into a JSON file as fast as possible.

- **Deadline:** March 15, 2026, 11:59 PM CET
- **Benchmark server:** Mac Mini M1, 12 GB RAM
- **Current fastest:** ~3.0 seconds (xHeaven)
- **PHP version required:** 8.5+

## Claude Code Instructions

- **Do NOT modify** the `WORKERS`, `BUF_SIZE`, or `PROBE_SIZE` constants in `app/Parser.php`. These values are calibrated for the Mac Mini M1 benchmark server and must remain as-is.
- **Do NOT run** `php tempest data:generate` or any variant. The existing `data/data.csv` is the benchmark dataset and must not be regenerated.

## Project Structure

```
app/
  Parser.php              ← THE ONLY FILE TO IMPLEMENT
  Commands/
    DataGenerateCommand.php   do not modify
    DataParseCommand.php      do not modify
    DataValidateCommand.php   do not modify
    BenchmarkRunCommand.php   do not modify
    Visit.php                 do not modify
data/
  test-data.csv             small dataset for validation
  test-data-expected.json   expected output for validation
  data.csv                  generated benchmark data (gitignored)
  data.json                 parser output (gitignored)
tempest                     console entry point
composer.json               PHP 8.5+, Tempest 3.2
```

## Challenge Requirements

### Input format
CSV file with no header, two columns per row:
```
https://stitcher.io/blog/some-post,2026-01-24T01:16:58+00:00
```
- Column 1: Full URL (scheme + host + path)
- Column 2: ISO 8601 timestamp with timezone offset

### Output format
Pretty-printed JSON (`JSON_PRETTY_PRINT`) written to `$outputPath`:
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

**Rules:**
- Key = URL path only (strip scheme and host: `https://stitcher.io` → omit)
- Forward slashes in keys are escaped: `/blog/slug` → `\/blog\/slug` (PHP's `json_encode` does this automatically)
- Value = object mapping `YYYY-MM-DD` date strings to integer visit counts
- Dates within each URL **sorted ascending**
- Visit counts aggregated (same URL + same date → increment counter)

### Constraints
- **No FFI** (explicitly prohibited)
- **JIT is disabled** on the benchmark server (caused segfaults; no meaningful gain)
- Solution must be original — copying other entries results in disqualification
- Must pass `php tempest data:validate` before submitting

## Parser Interface

Only implement this method in `app/Parser.php`:

```php
final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        // Read $inputPath (CSV), aggregate visits, write JSON to $outputPath
    }
}
```

## Development Workflow

```sh
composer install

# Generate test data (default: 1M rows, seeded for reproducibility)
php tempest data:generate
php tempest data:generate 10_000_000   # larger test
php tempest data:generate 100_000_000  # full benchmark size

# Validate correctness against test-data-expected.json
php tempest data:validate

# Run and time your solution
php tempest data:parse
```

Always run `data:validate` before submitting. It compares byte-for-byte against the expected output.

## Available PHP Extensions on Benchmark Server

```
bcmath, bz2, calendar, Core, ctype, curl, date, dba, dom, exif, fileinfo,
filter, ftp, gd, gettext, gmp, hash, iconv, igbinary, intl, json, ldap,
lexbor, libxml, mbstring, mysqli, mysqlnd, odbc, openssl, pcntl, pcre,
PDO, pdo_dblib, pdo_mysql, PDO_ODBC, pdo_pgsql, pdo_sqlite, pgsql, Phar,
posix, random, readline, Reflection, session, shmop, SimpleXML, snmp,
soap, sockets, sodium, SPL, sqlite3, standard, sysvmsg, sysvsem, sysvshm,
tidy, tokenizer, uri, xml, xmlreader, xmlwriter, xsl, Zend OPcache, zip,
zlib
```

Notable available: `pcntl` (forking/multiprocessing), `sockets`, `shmop` (shared memory), `posix`.

## Performance Notes

- Multi-core approaches via `pcntl_fork()` are allowed and used by top entries
- Shared memory (`shmop`) can help aggregate results across forked processes
- Buffered I/O and chunked reading are essential at 100M rows
- Avoid repeated function calls in hot loops (e.g. pre-compute offsets)
- String manipulation (extracting path from URL, date from timestamp) is the main bottleneck
- Benchmark tool: `hyperfine` (used by the organizer's benchmark system)

## Submission

1. Implement `app/Parser.php`
2. Run `php tempest data:validate` — must pass
3. Push to your fork and open a PR with your GitHub username as the title
4. Comment `/bench` on the PR to request a benchmark run
