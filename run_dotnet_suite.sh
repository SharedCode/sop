#!/bin/bash
set -e

# Define paths
TEST_PROJECT_DIR="bindings/csharp/Sop.Tests"
# Note: This path depends on the TargetFramework in Sop.Tests.csproj. 
# If that changes, this script needs updating.
BIN_DIR="$TEST_PROJECT_DIR/bin/Debug/netcoreapp3.1"

echo "--- Building C# Tests ---"
# Build first to ensure the bin directory exists
dotnet build "$TEST_PROJECT_DIR"

echo "--- Building Go Bridge ---"
cd bindings/main

ARCH=$(uname -m)
OS=$(uname -s)

# Ensure output dir exists
mkdir -p "../../$BIN_DIR"

if [ "$OS" == "Darwin" ]; then
    echo "Building for Darwin ($ARCH)..."
    # C# DllImport("libjsondb") looks for libjsondb.dylib on macOS
    go build -buildmode=c-shared -o "../../$BIN_DIR/libjsondb.dylib" *.go
elif [ "$OS" == "Linux" ]; then
    echo "Building for Linux ($ARCH)..."
    # C# DllImport("libjsondb") looks for libjsondb.so on Linux
    go build -buildmode=c-shared -o "../../$BIN_DIR/libjsondb.so" *.go
else
    echo "Unsupported OS for this script: $OS"
    exit 1
fi

cd ../..

echo "--- Running C# Tests ---"
# Run tests without rebuilding, so our copied dylib isn't overwritten/cleaned
dotnet test "$TEST_PROJECT_DIR" --no-build --verbosity normal
