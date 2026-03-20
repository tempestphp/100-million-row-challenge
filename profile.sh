#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME="100m-row-challenge"

docker run --rm \
  -e SPX_ENABLED=1 \
  -e SPX_FP_ENABLED=1 \
  -e SPX_BUILTINS=1 \
  -v "$(pwd):/app" \
  "$IMAGE_NAME" php tempest data:parse "$@"
