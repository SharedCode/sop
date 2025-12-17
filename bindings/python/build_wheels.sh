#!/bin/bash
set -e

# Clean dist
rm -rf dist/ build/ *.egg-info

# 1. Build Source Distribution (sdist) - Includes ALL binaries
echo "Building Source Distribution (sdist)..."
python3 -m build --sdist

# 2. Build Platform-Specific Wheels
platforms=(
    "macosx_10_9_x86_64"
    "macosx_11_0_arm64"
    "manylinux_2_17_x86_64"
    "manylinux_2_17_aarch64"
    "win_amd64"
)

for plat in "${platforms[@]}"; do
    echo "Building wheel for $plat..."
    export SOP_BUILD_PLATFORM=$plat
    python3 setup.py bdist_wheel --plat-name $plat
    
    # Clean build dir to ensure no cross-contamination
    rm -rf build/
done

echo "Build complete. Contents of dist/:"
ls -lh dist/
