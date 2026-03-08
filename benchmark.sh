#!/bin/bash
set -e

ROWS="${1:-100000000}"

echo "=== Building Docker image ==="
docker compose build

if [ ! -f data/data.csv ] || [ "$2" = "--regenerate" ]; then
    echo "=== Generating $ROWS rows ==="
    docker compose run --rm benchmark php tempest data:generate "$ROWS" --force
fi

echo "=== Running benchmark (2 vCPUs, 1.5GB RAM, JIT disabled) ==="
docker compose run --rm benchmark php tempest data:parse

echo "=== Validating output ==="
docker compose run --rm benchmark php tempest data:validate
