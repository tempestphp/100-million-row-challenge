# 100 Million Row Challenge (PHP) - Project Context

This project is a performance challenge for PHP developers. The goal is to parse a dataset of 100 million page visit rows (CSV format: `URL,ISO8601_TIMESTAMP`) into a specific aggregated JSON format as quickly as possible.

## Project Overview

- **Main Goal**: Efficiently parse millions of CSV rows and output a JSON file.
- **Technologies**: 
    - **Language**: PHP 8.5+
    - **Framework**: [Tempest Framework](https://tempestphp.com/) (used for CLI and core structure).
    - **Benchmark Tool**: `hyperfine` (used by the benchmark command).
- **Key Architecture**:
    - `app/Parser.php`: This is the primary file where the solution logic resides. Participants implement the `parse(string $inputPath, string $outputPath)` method.
    - `app/Commands/`: Contains Tempest CLI commands for the development lifecycle.
    - `data/`: Contains test datasets and expected output for validation.
    - `tempest`: The CLI entry point.

## Building and Running

### Prerequisites
- PHP 8.5 or higher.
- Composer.

### Initial Setup
```sh
composer install
```

## Docker Usage

For a consistent and optimized environment, you can use the provided `Dockerfile`.

### 1. Build the Docker Image
Build the image with all required PHP extensions and configurations:
```bash
docker build -t php-100m-challenge .
```

### 2. Validate Solution
Run validation inside the container:
```bash
docker run --rm php-100m-challenge data:validate
```

### 3. Data Generation
Generate a dataset (e.g., 100M rows) and mount it to your host:
```bash
docker run --rm -v $(pwd)/data:/app/data php-100m-challenge data:generate 100_000_000
```

### 4. Run the Parser
Run the actual parsing process:
```bash
docker run --rm -v $(pwd)/data:/app/data -v $(pwd)/app:/app/app php-100m-challenge data:parse
```

### Data Management
- **Generate Dataset**: Generates a CSV file for local testing.
  ```sh
  php tempest data:generate [count]
  # Example for 1M rows: php tempest data:generate 1000000
  ```
- **Run Parser**: Executes the implementation in `app/Parser.php`.
  ```sh
  php tempest data:parse
  ```
- **Validate Solution**: Checks if the parser correctly handles a small test dataset.
  ```sh
  php tempest data:validate
  ```

### Benchmarking
The official benchmark uses `hyperfine` and is triggered via GitHub Actions/Commands, but can be explored in `app/Commands/BenchmarkRunCommand.php`.

## Development Conventions

- **Implementation Location**: All parsing logic should ideally be contained within or called from `App\Parser::parse`.
- **Formatting Rules**:
    - JSON output must be pretty-printed (`JSON_PRETTY_PRINT`).
    - URL paths should be keys, and values should be objects mapping dates (`YYYY-MM-DD`) to visit counts.
    - Dates must be sorted in ascending order.
    - Paths should be sorted alphabetically.
- **Constraints**:
    - Use only PHP (no external tools or FFI).
    - No internet access during execution.
    - The solution must work within the project directory.

## Key Files
- `app/Parser.php`: The main entry point for the challenge solution.
- `app/Commands/DataGenerateCommand.php`: Logic for generating datasets.
- `app/Commands/DataValidateCommand.php`: Logic for verifying correctness.
- `leaderboard.csv`: Stores the results of processed submissions.
