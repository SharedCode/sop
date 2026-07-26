#!/bin/bash
set -e  # Exit on error

echo "========================================"
echo "Building Go Bridge..."
echo "========================================"
cd bindings/main

# Detect OS/arch using the same naming scheme as bindings/rust/build.rs
ARCH=$(uname -m)
case "$ARCH" in
    x86_64) GOARCH_NAME="amd64" ;;
    arm64|aarch64) GOARCH_NAME="arm64" ;;
    *) echo "Unsupported arch: $ARCH"; exit 1 ;;
esac

case "$OSTYPE" in
    darwin*) OS_NAME="darwin" ;;
    linux*) OS_NAME="linux" ;;
    *) echo "Unsupported OS: $OSTYPE"; exit 1 ;;
esac

# Force GOARCH/GOOS explicitly: on Apple Silicon with an x86_64 (Rosetta) Go
# toolchain, `go build` silently defaults to GOARCH=amd64 even though uname -m
# (and rustc's default target) is arm64, producing a mislabeled/wrong-arch archive.
export CGO_ENABLED=1
case "$GOARCH_NAME" in
    arm64) export GOARCH=arm64 ;;
    amd64) export GOARCH=amd64 ;;
esac
export GOOS="$OS_NAME"
unset CC

# build.rs links this as a static archive from ../rust/lib, not a runtime shared library
mkdir -p ../rust/lib
go build -buildmode=c-archive -o "../rust/lib/libjsondb_${GOARCH_NAME}${OS_NAME}.a" .

# Return to root
cd ../..

echo ""
echo "========================================"
echo "Running Rust Unit Tests..."
echo "========================================"
cd bindings/rust

cargo test

echo ""
echo "========================================"
echo "Running Rust Examples..."
echo "========================================"

EXAMPLES=(
    "btree_basic"
    "btree_batched"
    "btree_complex_key"
    "btree_metadata"
    "btree_paging"
    "concurrent_demo"
    "logging_demo"
    "model_store_demo"
    "remove_btree_demo"
    "text_search_demo"
    "vector_demo"
    "vector_search_ai"
    "cassandra_demo"
    "concurrent_demo_clustered"
)

for example in "${EXAMPLES[@]}"; do
    echo "----------------------------------------------------------------"
    echo "Running example: $example"
    echo "----------------------------------------------------------------"
    cargo run --example "$example"
    echo ""
done

echo "========================================"
echo "Rust Suite Completed Successfully!"
echo "========================================"
