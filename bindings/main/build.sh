#!/bin/bash
set -e
export CGO_ENABLED=1
VERSION=${SOP_VERSION:-latest}
RELEASE_DIR="../../release"
mkdir -p $RELEASE_DIR
mkdir -p ../rust/lib

# Fix for zig cc on macOS when cross-compiling
if [ "$(uname)" == "Darwin" ]; then
    export SDKROOT=$(xcrun --show-sdk-path)
    echo "SDKROOT is set to: $SDKROOT"
fi

if [ -z "$SKIP_MACOS" ]; then
echo "Building AMD64 darwin"
# Clean potential env vars that force coverage
unset GOFLAGS

export GOOS=darwin
export GOARCH=amd64
if [ "$(uname)" == "Linux" ]; then
    # Create a wrapper for Zig to handle Go's linker flags that Zig/LLD doesn't recognize
    cat <<EOF > zig-cc-wrapper.sh
#!/bin/bash
args=()
for arg in "\$@"; do
    if [ "\$arg" == "-x" ]; then
        # Check if -x is passed (Strip local symbols). 
        # Zig/LLD (via Clang driver) might reject -x or -Wl,-x on some targets.
        # We simply ignore it to allow the link to proceed.
        # We rely on -w (passed to Go) and -Wl,-S (passed by Go for -w?) to handle stripping.
        :
    else
        args+=("\$arg")
    fi
done
exec zig cc "\${args[@]}"
EOF
    chmod +x zig-cc-wrapper.sh
    export CC="$(pwd)/zig-cc-wrapper.sh -target x86_64-macos"

    export CGO_CFLAGS="-fno-stack-protector"
    
    # Hack: Compiling real dummy dylibs to satisfy Zig/LLD linker checks.
    # Empty files fail TBD parsing; we need actual Mach-O binaries.
    mkdir -p libs
    echo "void dummy() {}" > libs/dummy.c
    
    # Compile dependencies using the cross-compiler
    $CC -shared -Wl,-install_name,@rpath/libresolv.dylib -o libs/libresolv.dylib libs/dummy.c
    
    mkdir -p libs/CoreFoundation.framework
    $CC -shared -Wl,-install_name,@rpath/CoreFoundation.framework/CoreFoundation -o libs/CoreFoundation.framework/CoreFoundation libs/dummy.c
    
    mkdir -p libs/Security.framework
    $CC -shared -Wl,-install_name,@rpath/Security.framework/Security -o libs/Security.framework/Security libs/dummy.c

    export CGO_LDFLAGS="-L$(pwd)/libs -F$(pwd)/libs"
else
    unset CC
fi
# Restore -s (strip) to prevent _runtime.covctrs errors, handled by wrapper
go build -tags "netgo,osusergo,ignore_test_helpers" -ldflags "-s -w -extldflags -Wl,-undefined,dynamic_lookup" -buildmode=c-shared -o ../python/sop/libjsondb_amd64darwin.dylib .
go build -tags "ignore_test_helpers" -ldflags "-s -w" -buildmode=c-archive -o ../rust/lib/libjsondb_amd64darwin.a .
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
go build -tags "ignore_test_helpers" -buildmode=c-shared -o ../python/sop/libjsondb_amd64windows.dll .
go build -tags "ignore_test_helpers" -buildmode=c-archive -o ../rust/lib/libjsondb_amd64windows.a .
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
    export CC="$(pwd)/zig-cc-wrapper.sh -target aarch64-macos"
    export CGO_CFLAGS="-fno-stack-protector"
    
    # Use the same dummy lib hack
    mkdir -p libs
    echo "void dummy() {}" > libs/dummy.c
    
    $CC -shared -Wl,-install_name,@rpath/libresolv.dylib -o libs/libresolv.dylib libs/dummy.c
    
    mkdir -p libs/CoreFoundation.framework
    $CC -shared -Wl,-install_name,@rpath/CoreFoundation.framework/CoreFoundation -o libs/CoreFoundation.framework/CoreFoundation libs/dummy.c
    
    mkdir -p libs/Security.framework
    $CC -shared -Wl,-install_name,@rpath/Security.framework/Security -o libs/Security.framework/Security libs/dummy.c

    export CGO_LDFLAGS="-L$(pwd)/libs -F$(pwd)/libs"
else
    unset CC
fi
go build -tags "ignore_test_helpers" -ldflags "-s -w" -buildmode=c-archive -o ../rust/lib/libjsondb_arm64darwin.a .
go build -tags "netgo,osusergo,ignore_test_helpers" -ldflags "-s -w -extldflags -Wl,-undefined,dynamic_lookup" -buildmode=c-shared -o ../python/sop/libjsondb_arm64darwin.dylib .
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
go build -tags "ignore_test_helpers" -buildmode=c-archive -o ../rust/lib/libjsondb_amd64linux.a .
go build -tags "ignore_test_helpers" -buildmode=c-shared -o ../python/sop/libjsondb_amd64linux.so .
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
go build -tags "ignore_test_helpers" -buildmode=c-shared -o ../python/sop/libjsondb_arm64linux.so .
go build -tags "ignore_test_helpers" -buildmode=c-archive -o ../rust/lib/libjsondb_arm64linux.a .
# Java Packaging (JNA)
mkdir -p ../java/src/main/resources/linux-aarch64
cp ../python/sop/libjsondb_arm64linux.so ../java/src/main/resources/linux-aarch64/libjsondb.so
cp ../python/sop/libjsondb_arm64linux.so ../csharp/Sop/
cp ../python/sop/libjsondb_arm64linux.h ../csharp/Sop/

# Build Browser
echo "Building sop-httpserver for linux/arm64..."
CGO_ENABLED=0 go build -ldflags "-X main.Version=$VERSION" -o $RELEASE_DIR/sop-httpserver-linux-arm64 ../../tools/httpserver
fi

echo "Build complete."
