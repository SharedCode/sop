#!/bin/bash
set -e

# Get the absolute path to the repo root
REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)

# Extract version from pyproject.toml
VERSION=$(grep -m 1 'version =' "$REPO_ROOT/bindings/python/pyproject.toml" | sed 's/version = "//;s/"//' | tr -d '[:space:]')
echo "Detected version from pyproject.toml: $VERSION"

echo "Building Docker image for SOP bindings..."
# Use repo root as context to access go.mod/go.sum
docker build -t sop-bindings-builder -f "$REPO_ROOT/bindings/Dockerfile.build" "$REPO_ROOT"

# If on macOS, we will skip macOS builds in Docker and run them locally later
if [ "$(uname)" = "Darwin" ]; then
    SKIP_MACOS_ENV="-e SKIP_MACOS=1"
else
    SKIP_MACOS_ENV=""
fi

echo "Running build inside Docker..."
# Run the build in a temporary container (no volume mount for speed)
# We use a specific name to easily copy files out afterwards
docker rm -f sop-build-temp 2>/dev/null || true
docker run --name sop-build-temp \
    -e SOP_VERSION=$VERSION \
    $SKIP_MACOS_ENV \
    sop-bindings-builder \
    /bin/bash -c "cd bindings/main && ./build.sh"

echo "Copying artifacts back to host..."
# Copy generated artifacts from the container to the host
docker cp sop-build-temp:/app/bindings/python/sop/. "$REPO_ROOT/bindings/python/sop/"
docker cp sop-build-temp:/app/bindings/csharp/Sop/. "$REPO_ROOT/bindings/csharp/Sop/"
docker cp sop-build-temp:/app/bindings/java/src/main/resources/. "$REPO_ROOT/bindings/java/src/main/resources/"
docker cp sop-build-temp:/app/bindings/rust/lib/. "$REPO_ROOT/bindings/rust/lib/"
mkdir -p "$REPO_ROOT/release"
docker cp sop-build-temp:/app/release/. "$REPO_ROOT/release/"

# Cleanup
docker rm sop-build-temp

# If on macOS, build macOS artifacts locally now (so they overwrite any stale ones from Docker)
if [ "$(uname)" == "Darwin" ]; then
    echo "Detected macOS. Building macOS artifacts locally..."
    export SOP_VERSION=$VERSION
    "$REPO_ROOT/bindings/build_local_macos.sh"
fi

echo "Build complete. Artifacts are in the bindings folders."
