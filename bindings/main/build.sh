#!/bin/bash
set -e
export CGO_ENABLED=1
VERSION=${SOP_VERSION:-latest}
RELEASE_DIR="../../release"
mkdir -p $RELEASE_DIR
mkdir -p ../rust/lib

# Ensure the wrapper is executable if it exists
if [ -f "zig-cc-wrapper.sh" ]; then
    chmod +x zig-cc-wrapper.sh
fi

# Fix for zig cc on macOS when cross-compiling
if [ "$(uname)" == "Darwin" ]; then
    export SDKROOT=$(xcrun --show-sdk-path)
    echo "SDKROOT is set to: $SDKROOT"
fi

if [ -z "$SKIP_MACOS" ]; then
echo "Building AMD64 darwin"

export GOOS=darwin
export GOARCH=amd64
if [ "$(uname)" == "Linux" ]; then
    export CC="zig cc -target x86_64-macos"
    export CGO_CFLAGS="-fno-stack-protector"
    # Don't use SDKROOT paths on Linux as they don't exist/conflict with Zig's internal sysroot
    export CGO_LDFLAGS="-Wl,-undefined,dynamic_lookup"
else
    unset CC
fi
# Added -s to strip symbol table (fixes _runtime.covctrs error)
go build -ldflags "-w" -buildmode=c-shared -o ../python/sop/libjsondb_amd64darwin.dylib *.go
go build -ldflags "-w" -buildmode=c-archive -o ../rust/lib/libjsondb_amd64darwin.a *.go
cp ../python/sop/libjsondb_amd64darwin.dylib ../csharp/Sop/
cp ../python/sop/libjsondb_amd64darwin.h ../csharp/Sop/
# For testing in Examples.
cp ../python/sop/libjsondb_amd64darwin.dylib ../csharp/Sop.CLI/libjsondb.dylib
cp ../python/sop/libjsondb_amd64darwin.h ../csharp/Sop.CLI/libjsondb.h
# Java Packaging (JNA)
mkdir -p ../java/src/main/resources/darwin-x86-64
cp ../python/sop/libjsondb_amd64darwin.dylib ../java/src/main/resources/darwin-x86-64/libjsondb.dylib

# Build Browser
echo "Building sop-httpserver for darwin/amd64..."
CGO_ENABLED=0 go build -ldflags "-X main.Version=$VERSION" -o $RELEASE_DIR/sop-httpserver-darwin-amd64 ../../tools/httpserver
fi

if [ -z "$ONLY_MACOS" ]; then
rm -rf ../csharp/Sop.CLI/bin
rm -rf ../csharp/Sop.CLI/obj
rm -rf ../csharp/Sop.CLI/data

echo "Building AMD64 windows"

export GOOS=windows
export GOARCH=amd64
export CC=x86_64-w64-mingw32-gcc
# Added -s to strip symbol table (consistent with other targets)
go build -ldflags "-w" -buildmode=c-shared -o ../python/sop/libjsondb_amd64windows.dll *.go
go build -ldflags "-w" -buildmode=c-archive -o ../rust/lib/libjsondb_amd64windows.a *.go
cp ../python/sop/libjsondb_amd64windows.dll ../csharp/Sop/
cp ../python/sop/libjsondb_amd64windows.h ../csharp/Sop/
# Java Packaging (JNA)
mkdir -p ../java/src/main/resources/win32-x86-64
cp ../python/sop/libjsondb_amd64windows.dll ../java/src/main/resources/win32-x86-64/libjsondb.dll

# Build Browser
echo "Building sop-httpserver for windows/amd64..."
CGO_ENABLED=0 go build -ldflags "-X main.Version=$VERSION" -o $RELEASE_DIR/sop-httpserver-windows-amd64.exe ../../tools/httpserver
fi

if [ -z "$SKIP_MACOS" ]; then
echo "Building ARM64 darwin"

export GOOS=darwin
export GOARCH=arm64
if [ "$(uname)" == "Linux" ]; then
    export CC="zig cc -target aarch64-macos"
    export CGO_CFLAGS="-fno-stack-protector"
    # Don't use SDKROOT paths on Linux as they don't exist/conflict with Zig's internal sysroot
    export CGO_LDFLAGS="-Wl,-undefined,dynamic_lookup"
else
    unset CC
fi
# Added -s to strip symbol table (fixes _runtime.covctrs error)
go build -ldflags "-w" -buildmode=c-archive -o ../rust/lib/libjsondb_arm64darwin.a *.go
go build -ldflags "-w" -buildmode=c-shared -o ../python/sop/libjsondb_arm64darwin.dylib *.go
cp ../python/sop/libjsondb_arm64darwin.dylib ../csharp/Sop/
cp ../python/sop/libjsondb_arm64darwin.h ../csharp/Sop/
# Java Packaging (JNA)
mkdir -p ../java/src/main/resources/darwin-aarch64
cp ../python/sop/libjsondb_arm64darwin.dylib ../java/src/main/resources/darwin-aarch64/libjsondb.dylib

# Build Browser
echo "Building sop-httpserver for darwin/arm64..."
CGO_ENABLED=0 go build -ldflags "-X main.Version=$VERSION" -o $RELEASE_DIR/sop-httpserver-darwin-arm64 ../../tools/httpserver
fi

if [ -z "$ONLY_MACOS" ]; then
echo "Building AMD64 linux"

export GOOS=linux
export GOARCH=amd64
export CC="zig cc -target x86_64-linux-gnu"
# Added -s to strip symbol table
go build -ldflags "-w" -buildmode=c-archive -o ../rust/lib/libjsondb_amd64linux.a *.go
go build -ldflags "-w" -buildmode=c-shared -o ../python/sop/libjsondb_amd64linux.so *.go
cp ../python/sop/libjsondb_amd64linux.so ../csharp/Sop/
cp ../python/sop/libjsondb_amd64linux.h ../csharp/Sop/
# Java Packaging (JNA)
mkdir -p ../java/src/main/resources/linux-x86-64
cp ../python/sop/libjsondb_amd64linux.so ../java/src/main/resources/linux-x86-64/libjsondb.so

# Build Browser
echo "Building sop-httpserver for linux/amd64..."
CGO_ENABLED=0 go build -ldflags "-X main.Version=$VERSION" -o $RELEASE_DIR/sop-httpserver-linux-amd64 ../../tools/httpserver

echo "Building ARM64 linux"

export GOOS=linux
export GOARCH=arm64
export CC="zig cc -target aarch64-linux-gnu"
# Added -s to strip symbol table
go build -ldflags "-w" -buildmode=c-shared -o ../python/sop/libjsondb_arm64linux.so *.go
go build -ldflags "-w" -buildmode=c-archive -o ../rust/lib/libjsondb_arm64linux.a *.go
# Java Packaging (JNA)
mkdir -p ../java/src/main/resources/linux-aarch64
cp ../python/sop/libjsondb_arm64linux.so ../java/src/main/resources/linux-aarch64/libjsondb.so
cp ../python/sop/libjsondb_arm64linux.so ../csharp/Sop/
cp ../python/sop/libjsondb_arm64linux.h ../csharp/Sop/

# Build Browser
echo "Building sop-httpserver for linux/arm64..."
CGO_ENABLED=0 go build -ldflags "-X main.Version=$VERSION" -o $RELEASE_DIR/sop-httpserver-linux-amd64 ../../tools/httpserver
fi

echo "Build complete."
