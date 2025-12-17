#!/bin/bash
set -e

# Get the absolute path to the repo root
REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)

echo "Building Docker image for SOP bindings..."
docker build -t sop-bindings-builder -f "$REPO_ROOT/bindings/Dockerfile.build" "$REPO_ROOT/bindings"

echo "Running build inside Docker..."
docker run --rm \
    -v "$REPO_ROOT:/app" \
    -w "/app/bindings/main" \
    -e SOP_VERSION=${SOP_VERSION:-latest} \
    sop-bindings-builder \
    ./build.sh

echo "Build complete. Artifacts are in the bindings folders."
