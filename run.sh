#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME="100m-row-challenge"

case "${1:-parse}" in
    generate)
        shift
        docker run --rm -v "$(pwd):/app" "$IMAGE_NAME" php tempest data:generate --force "$@"
        ;;
    validate)
        shift 2>/dev/null || true
        docker run --rm -v "$(pwd):/app" "$IMAGE_NAME" php tempest data:generate --force "$@"
        docker run --rm -v "$(pwd):/app" "$IMAGE_NAME" php tempest data:validate "$@"
        ;;
    parse)
        shift 2>/dev/null || true
        docker run --rm -v "$(pwd):/app" "$IMAGE_NAME" php tempest data:parse "$@"
        ;;
    *)
        echo "Usage: ./run.sh [generate|parse|validate] [options]"
        exit 1
        ;;
esac
