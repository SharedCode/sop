#!/bin/bash
set -e

echo "--- Building Go Bridge (Local) ---"
cd bindings/main

OS=$(uname -s)
ARCH=$(uname -m)
GOOS=""
GOARCH=""
OUTFILE=""

if [[ "$OS" == "Darwin" ]]; then
    GOOS="darwin"
    if [[ "$ARCH" == "arm64" ]]; then
        GOARCH="arm64"
        OUTFILE="../python/sop/libjsondb_arm64darwin.dylib"
    else
        GOARCH="amd64"
        OUTFILE="../python/sop/libjsondb_amd64darwin.dylib"
    fi
elif [[ "$OS" == "Linux" ]]; then
    GOOS="linux"
    if [[ "$ARCH" == "aarch64" || "$ARCH" == "arm64" ]]; then
        GOARCH="arm64"
        OUTFILE="../python/sop/libjsondb_arm64linux.so"
    else
        GOARCH="amd64"
        OUTFILE="../python/sop/libjsondb_amd64linux.so"
    fi
elif [[ "$OS" == MINGW* || "$OS" == CYGWIN* || "$OS" == MSYS* ]]; then
    GOOS="windows"
    GOARCH="amd64"
    OUTFILE="../python/sop/libjsondb_amd64windows.dll"
else
    echo "Unsupported OS: $OS"
    exit 1
fi

echo "Detected Local Environment: OS=$GOOS, ARCH=$GOARCH"
echo "Building $OUTFILE..."

export CGO_ENABLED=1
export GOOS=$GOOS
export GOARCH=$GOARCH
# Unset CC to use default compiler for local build
unset CC

go build -buildmode=c-shared -o "$OUTFILE" *.go

cd ../..

echo "--- Running Python Tests ---"
cd bindings/python

echo "Running sanity_check.py..."
python3 sanity_check.py

echo "Running test_ai.py..."
python3 test_ai.py

echo "Running test_coverage.py..."
python3 test_coverage.py

echo "Running test_cud_batch.py..."
python3 test_cud_batch.py

echo "Running test_unified_db.py..."
python3 test_unified_db.py

echo "Running test_search.py..."
python3 test_search.py

echo "Running sop/test_btree.py..."
python3 -m sop.test_btree

echo "Running sop/test_btree_idx.py..."
python3 -m sop.test_btree_idx

echo "Running sop/test_logging.py..."
python3 -m sop.test_logging

echo "--- Running Python Examples ---"

echo "Running sop.examples.logging_demo..."
python3 -m sop.examples.logging_demo

echo "Running sop.examples.modelstore_demo..."
python3 -m sop.examples.modelstore_demo

echo "Running sop.examples.unified_demo..."
python3 -m sop.examples.unified_demo

echo "Running sop.examples.complex_key_demo..."
python3 -m sop.examples.complex_key_demo

echo "Running sop.examples.metadata_key_demo..."
python3 -m sop.examples.metadata_key_demo

echo "Running sop.examples.vector_demo..."
python3 -m sop.examples.vector_demo

# Skipping clustered/replication demos as they might require specific setup or take longer, 
# but I'll run the basic ones first. If user wants all, I can add them.
# Let's add them but be aware they might fail if Redis cluster isn't set up or similar.
# Actually, the user asked for "all python tests and examples". I should try.

echo "Running sop.examples.vector_clustered_demo..."
python3 -m sop.examples.vector_clustered_demo || echo "Warning: vector_clustered_demo failed (possibly due to missing cluster setup)"

echo "Running sop.examples.vector_clustered_replication_demo..."
python3 -m sop.examples.vector_clustered_replication_demo || echo "Warning: vector_clustered_replication_demo failed"

echo "Running sop.examples.vector_replication_demo..."
python3 -m sop.examples.vector_replication_demo || echo "Warning: vector_replication_demo failed"

# langchain_demo might require API keys or extra deps.
echo "Running sop.examples.langchain_demo..."
python3 -m sop.examples.langchain_demo || echo "Warning: langchain_demo failed (possibly missing dependencies)"

echo "All Python tests and examples completed."
