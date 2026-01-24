#!/bin/bash
set -e

# ==========================================
# SOP Release Builder
# ==========================================
# Prerequisites for Full Cross-Platform Build:
#
# 1. macOS (Host):
#    brew install mingw-w64 zig
#
# 2. Linux (Host):
#    sudo apt-get install gcc-mingw-w64 zip
#    sudo snap install zig --classic --beta
#
# If these are missing, the script will attempt to build only 
# the artifacts supported by your current OS/Arch.
#
# ------------------------------------------
# How to Unpack, Install & Run (from .tar.gz):
# ------------------------------------------
# 1. Unpack the bundle:
#    tar -xzf release/sop-bundle-darwin-amd64-*.tar.gz
#
# 2. Enter the directory:
#    cd sop-bundle-darwin-amd64-*
#
# 3. Run the server directly:
#    ./sop-httpserver
#

# Read version from version file
if [ -f VERSION ]; then
  SOP_VERSION=$(cat VERSION | tr -d '[:space:]')
fi

VERSION=${SOP_VERSION:-"1.0.0-beta"}
OUTPUT_DIR="release"
#
# ------------------------------------------
# How to Unpack, Install & Run (from .tar.gz):
# ------------------------------------------
# 1. Unpack the bundle:
#    tar -xzf release/sop-bundle-darwin-amd64-*.tar.gz
#
# 2. Enter the directory:
#    cd sop-bundle-darwin-amd64-*
#
# 3. Run the server directly:
#    ./sop-httpserver
#    # Note for macOS users: If you see "Unknown Developer" warning,
#    # run: xattr -d com.apple.quarantine sop-httpserver
#    # OR Right-click > Open
#
# 4. (Optional) Install (to ~/.sop):
#    ./install.sh
#    # Note for macOS users: If denied, run: xattr -d com.apple.quarantine install.sh
#    # specific Usage instructions printed on success, likely adding to PATH
#
# 5. Uninstall:
#    ./uninstall.sh
# ==========================================

# Configuration
VERSION=${SOP_VERSION:-"1.0.0-beta"}
OUTPUT_DIR="release"
SOP_ROOT="$(pwd)"

echo "Starting SOP Release Build v$VERSION"
echo "Cleaning up..."
rm -rf $OUTPUT_DIR
mkdir -p $OUTPUT_DIR

# Step 1: Build Core Binaries & Shared Libraries
echo "Building Core Bindings and Binaries..."

# Check if Docker is available to use the robust build environment
if command -v docker &> /dev/null; then
    echo "Docker detected. Using robust, reproducible build environment (bindings/build_in_docker.sh)..."
    # Ensure execute permissions (handle CWD being either root or specific)
    # Assume script is run from SOP_ROOT if finding this file
    if [ -f "bindings/build_in_docker.sh" ]; then
        chmod +x bindings/build_in_docker.sh
        if ./bindings/build_in_docker.sh; then
            echo "Docker build successful."
        else
            echo "Docker build failed."
            exit 1
        fi
    else
        echo "Error: bindings/build_in_docker.sh not found in current directory $(pwd)"
        exit 1
    fi
else
    echo "Docker not found. Falling back to local toolchain (Host OS)..."
    echo "WARNING: Binaries built locally may rely on your system's specific 'glibc' version and might not run on older Linux systems."
    
    cd bindings/main
    # Ensure script is executable
    chmod +x build.sh

    # We run the build.sh. If it fails due to missing cross-compilers, we fail if in strict mode (CI),
    # otherwise we warn and proceed (for local dev convenience).
    if ./build.sh; then
        echo "Bindings build successful."
    else
        echo "Bindings build failed."
        # Fail hard in CI environments to prevent broken releases
        if [ "$CI" == "true" ] || [ "$GITHUB_ACTIONS" == "true" ]; then
            echo "Error: Aborting release build due to compilation errors."
            exit 1
        fi
        echo "Proceeding with available artifacts (Local Dev Mode)..."
    fi
    cd "$SOP_ROOT"
fi

# Function to Create Platform Bundle
create_bundle() {
    OS=$1
    ARCH=$2
    LABEL=$3 # suffix used by build.sh for binaries, e.g. darwin-amd64
    
    # Define Bundle Name
    BUNDLE_NAME="sop-bundle-${OS}-${ARCH}-${VERSION}"
    BUNDLE_DIR="$OUTPUT_DIR/$BUNDLE_NAME"
    
    # Check if we have the main binary for this target
    # build.sh produces binaries in release/ with specific names
    BIN_SRC="$OUTPUT_DIR/sop-httpserver-${LABEL}"
    [ "$OS" == "windows" ] && BIN_SRC="${BIN_SRC}.exe"
    
    if [ ! -f "$BIN_SRC" ]; then
        echo "Skipping bundle $BUNDLE_NAME: Binary $BIN_SRC not found."
        return
    fi

    echo "Creating Bundle: $BUNDLE_NAME"
    mkdir -p "$BUNDLE_DIR"
    
    # 1. Main Server Binary
    cp "$BIN_SRC" "$BUNDLE_DIR/sop-httpserver"
    [ "$OS" == "windows" ] && mv "$BUNDLE_DIR/sop-httpserver" "$BUNDLE_DIR/sop-httpserver.exe"
    chmod +x "$BUNDLE_DIR/"*
    
    # 2. Shared Libraries (Generic)
    echo "  - Adding Shared Libraries..."
    mkdir -p "$BUNDLE_DIR/libs"
    LIB_NAME=""
    if [ "$OS" == "darwin" ]; then
        LIB_NAME="libjsondb_${ARCH}darwin.dylib"
        DEST_LIB="libjsondb.dylib"
    elif [ "$OS" == "linux" ]; then
        LIB_NAME="libjsondb_${ARCH}linux.so"
        DEST_LIB="libjsondb.so"
    elif [ "$OS" == "windows" ]; then
        LIB_NAME="libjsondb_${ARCH}windows.dll"
        DEST_LIB="libjsondb.dll"
    fi
    
    if [ -n "$LIB_NAME" ] && [ -f "bindings/python/sop/$LIB_NAME" ]; then
        cp "bindings/python/sop/$LIB_NAME" "$BUNDLE_DIR/libs/$DEST_LIB"
    else
        echo "Warning: Shared library $LIB_NAME not found."
    fi

    # 3. Python Bindings
    echo "  - Adding Python Bindings..."
    mkdir -p "$BUNDLE_DIR/python"
    cp bindings/python/README.md "$BUNDLE_DIR/python/" 2>/dev/null || true
    cp bindings/python/setup.py "$BUNDLE_DIR/python/" 2>/dev/null || true
    cp bindings/python/pyproject.toml "$BUNDLE_DIR/python/" 2>/dev/null || true
    cp -r bindings/python/sop "$BUNDLE_DIR/python/" 2>/dev/null || true
    
    # 3. C# Bindings
    echo "  - Adding C# Bindings..."
    mkdir -p "$BUNDLE_DIR/dotnet"
    cp bindings/csharp/README.md "$BUNDLE_DIR/dotnet/" 2>/dev/null || true
    cp -r bindings/csharp/Sop "$BUNDLE_DIR/dotnet/" 2>/dev/null || true
    
    # Include Examples (Sop.CLI)
    echo "  - Adding C# Examples (Sop.CLI)..."
    cp -r bindings/csharp/Sop.CLI "$BUNDLE_DIR/dotnet/" 2>/dev/null || true
    # Clean build artifacts to keep bundle small
    rm -rf "$BUNDLE_DIR/dotnet/Sop.CLI/bin" "$BUNDLE_DIR/dotnet/Sop.CLI/obj" "$BUNDLE_DIR/dotnet/Sop.CLI/sop_data_*"
    
    # 4. Java Bindings
    echo "  - Adding Java Bindings..."
    mkdir -p "$BUNDLE_DIR/java"
    cp bindings/java/README.md "$BUNDLE_DIR/java/" 2>/dev/null || true
    cp bindings/java/pom.xml "$BUNDLE_DIR/java/" 2>/dev/null || true
    cp -r bindings/java/src "$BUNDLE_DIR/java/" 2>/dev/null || true

    # 4b. Rust Bindings
    echo "  - Adding Rust Bindings..."
    mkdir -p "$BUNDLE_DIR/rust"
    cp bindings/rust/README.md "$BUNDLE_DIR/rust/" 2>/dev/null || true
    cp bindings/rust/Cargo.toml "$BUNDLE_DIR/rust/" 2>/dev/null || true
    cp bindings/rust/build.rs "$BUNDLE_DIR/rust/" 2>/dev/null || true
    cp -r bindings/rust/src "$BUNDLE_DIR/rust/" 2>/dev/null || true
    cp -r bindings/rust/lib "$BUNDLE_DIR/rust/" 2>/dev/null || true
    cp -r bindings/rust/examples "$BUNDLE_DIR/rust/" 2>/dev/null || true

    # 5. READMEs & Scripts
    cp README.md "$BUNDLE_DIR/"
    cp scripts/install.sh "$BUNDLE_DIR/" 2>/dev/null || true
    cp scripts/uninstall.sh "$BUNDLE_DIR/" 2>/dev/null || true
    chmod +x "$BUNDLE_DIR/install.sh" "$BUNDLE_DIR/uninstall.sh" 2>/dev/null || true
    
    # 6. Archive
    cd "$OUTPUT_DIR"
    if [ "$OS" == "windows" ]; then
        zip -r -q "${BUNDLE_NAME}.zip" "$BUNDLE_NAME"
    else
        tar -czf "${BUNDLE_NAME}.tar.gz" "$BUNDLE_NAME"
    fi
    cd "$SOP_ROOT"
    
    # Cleanup
    rm -rf "$BUNDLE_DIR"
}

# Try to package everything
create_bundle darwin arm64 "darwin-arm64"
create_bundle darwin amd64 "darwin-amd64"
create_bundle linux amd64 "linux-amd64"
create_bundle linux arm64 "linux-arm64"
create_bundle windows amd64 "windows-amd64"

echo "Release Packaging Complete."
ls -lh $OUTPUT_DIR/*.{tar.gz,zip} 2>/dev/null || echo "No bundles created."



