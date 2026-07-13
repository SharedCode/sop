#!/bin/bash
set -e  # Exit on error

echo "========================================"
echo "Building Go Bridge..."
echo "========================================"
cd bindings/main

# Detect OS for library extension
if [[ "$OSTYPE" == "darwin"* ]]; then
    LIB_EXT="dylib"
else
    LIB_EXT="so"
fi

# Build the shared library
go build -buildmode=c-shared -o libjsondb.$LIB_EXT .

# Return to root
cd ../..

echo ""
echo "========================================"
echo "Running Rust Unit Tests..."
echo "========================================"
cd bindings/rust

# Ensure library path is set for runtime so the tests can find libjsondb
export LIBRARY_PATH=../main:$LIBRARY_PATH
export LD_LIBRARY_PATH=../main:$LD_LIBRARY_PATH
export DYLD_LIBRARY_PATH=../main:$DYLD_LIBRARY_PATH

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
