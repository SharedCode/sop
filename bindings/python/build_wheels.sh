#!/bin/bash
set -e

# Clean dist
rm -rf dist/ build/ *.egg-info

# Sync version from pyproject.toml to sop/__init__.py
echo "Syncing version..."
python3 -c "
import re
try:
    with open('pyproject.toml', 'r') as f:
        content = f.read()
    match = re.search(r'version\s*=\s*\"([^\"]+)\"', content)
    if match:
        version = match.group(1)
        init_file = 'sop/__init__.py'
        with open(init_file, 'r') as f_in:
            init_content = f_in.read()
        new_content = re.sub(r'__version__\s*=\s*\"[^\"]+\"', f'__version__=\"{version}\"', init_content)
        with open(init_file, 'w') as f_out:
            f_out.write(new_content)
        print(f'Synced version {version} to {init_file}')
    else:
        print('Could not find version in pyproject.toml')
        exit(1)
except Exception as e:
    print(f'Error syncing version: {e}')
    exit(1)
"

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
