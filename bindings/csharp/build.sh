#!/bin/bash
set -e

# Get the directory of the script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
REPO_ROOT="$SCRIPT_DIR/../.."

echo "Building C# NuGet packages..."
echo "Repo Root: $REPO_ROOT"

# 0. Read Version
VERSION_FILE="$SCRIPT_DIR/VERSION"
if [ -f "$VERSION_FILE" ]; then
    VERSION=$(cat "$VERSION_FILE" | tr -d '[:space:]')
    echo "Using Version: $VERSION"
else
    echo "Error: VERSION file not found at $VERSION_FILE"
    exit 1
fi

# Clean dist directory
echo "Cleaning dist directory..."
rm -rf "$SCRIPT_DIR/dist"
mkdir -p "$SCRIPT_DIR/dist"

# Update Version in Files
# Helper for cross-platform sed (macOS requires empty string for -i)
sed_i() {
    if [[ "$OSTYPE" == "darwin"* ]]; then
        sed -i '' "$@"
    else
        sed -i "$@"
    fi
}

echo "Updating version in project files..."
sed_i "s|<Version>.*</Version>|<Version>$VERSION</Version>|g" "$SCRIPT_DIR/Sop/Sop.csproj"
sed_i "s|<Version>.*</Version>|<Version>$VERSION</Version>|g" "$SCRIPT_DIR/Sop.CLI/Sop.CLI.csproj"

# 1. Check for Native Libraries (Assumed to be pre-built)
echo "Checking for Native Libraries..."
MISSING_LIBS=0
for lib in "libjsondb_amd64darwin.dylib" "libjsondb_arm64darwin.dylib" "libjsondb_amd64linux.so" "libjsondb_arm64linux.so" "libjsondb_amd64windows.dll" "libjsondb_arm64windows.dll"; do
    if [ ! -f "$SCRIPT_DIR/Sop/$lib" ]; then
        echo "Error: Missing $lib in $SCRIPT_DIR/Sop/"
        MISSING_LIBS=1
    fi
done

if [ $MISSING_LIBS -eq 1 ]; then
    echo "Please build the native libraries first (see RELEASE_NUGET.md)."
    # Optional: Uncomment to attempt build if you have the toolchain
    # GOOS=darwin GOARCH=amd64 CGO_ENABLED=1 go build -buildmode=c-shared -o "$SCRIPT_DIR/Sop/libjsondb_amd64darwin.dylib" "$REPO_ROOT/bindings/main/..."
    exit 1
fi

echo "All native libraries found."

# 2. Pack Sop.Data (The Library)
echo "Packing Sop.Data..."
dotnet pack "$SCRIPT_DIR/Sop/Sop.csproj" -c Release -o "$SCRIPT_DIR/dist"

# 3. Pack Sop.CLI (The Tool)
echo "Packing Sop.CLI..."
dotnet pack "$SCRIPT_DIR/Sop.CLI/Sop.CLI.csproj" -c Release -o "$SCRIPT_DIR/dist"

echo "Build complete. Packages are in $SCRIPT_DIR/dist"
ls -l "$SCRIPT_DIR/dist"
