# Execution Instructions (EXEC.md)

This file describes how to build the Docker container and run the 100 Million Row Challenge solution in an optimized environment.

## 1. Build the Docker Image

First, build the image that contains all necessary PHP extensions (`pcntl`, `shmop`, `igbinary`, `intl`) and the optimized configuration:

```bash
docker build -t php-100m-challenge .
```

## 2. Solution Correctness Validation

To check if the parser correctly processes data and generates JSON according to the requirements (on a small test set), run:

```bash
docker run --rm php-100m-challenge data:validate
```

If you see the message `Validation passed!`, it means the solution is working correctly.

## 3. Data Generation (Optional)

By default, the image includes a test set. If you want to generate your own data set (e.g., 100 million rows) and save it to the host:

```bash
docker run --rm -v $(pwd)/data:/app/data php-100m-challenge data:generate 100_000_000
```

## 4. Run the Parser (Benchmark)

To run the actual parsing process of the `data/data.csv` file and get the execution time:

```bash
docker run --rm -v $(pwd)/data:/app/data php-100m-challenge data:parse
```

## 5. Technical Notes

- **Shared Memory**: The solution uses the `shmop` mechanism for fast data synchronization between processes. In some Docker environments (e.g., older versions of macOS or specific Linux configurations), it may be necessary to increase the shared memory limit (`--shm-size`).
- **Multithreading**: The script automatically detects and utilizes 8 CPU cores (as configured in `Parser.php`).
- **JIT**: According to the challenge guidelines, JIT is disabled in the PHP configuration inside the container.
