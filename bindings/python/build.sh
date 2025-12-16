#!/bin/bash
set -e

# Clean up previous build artifacts
echo "Cleaning up previous builds..."
rm -rf dist/
rm -rf build/
rm -rf sop4py.egg-info/

# Build the shared libraries (Go Core)
echo "Building Go shared libraries..."
# We use a subshell or pushd/popd to ensure we return to the python dir
(cd ../main && ./build.sh)

# Sync version from pyproject.toml to sop/__init__.py
echo "Syncing version..."
PYTHON_EXEC=${PYTHON:-python3}
$PYTHON_EXEC -c "
import re
import sys

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
            sys.exit(1)
except Exception as e:
    print(f'Error syncing version: {e}')
    sys.exit(1)
"

# Build the package
echo "Building new release..."
$PYTHON_EXEC -m build

echo "Build complete. Ready to upload."