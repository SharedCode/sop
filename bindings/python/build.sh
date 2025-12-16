#!/bin/bash
set -e

# Clean up previous build artifacts
echo "Cleaning up previous builds..."
rm -rf dist/
rm -rf build/
rm -rf sop4py.egg-info/

# Sync version from pyproject.toml to sop/__init__.py and export for build
echo "Syncing version..."
PYTHON_EXEC=${PYTHON:-python3}
# Extract version to a temp file to read it into bash variable
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
            
            # Write version to a temp file for the shell script to read
            with open('.version_tmp', 'w') as f_v:
                f_v.write(version)
        else:
            print('Could not find version in pyproject.toml')
            sys.exit(1)
except Exception as e:
    print(f'Error syncing version: {e}')
    sys.exit(1)
"

if [ -f .version_tmp ]; then
    export SOP_VERSION=$(cat .version_tmp)
    rm .version_tmp
    echo "Exported SOP_VERSION=$SOP_VERSION"
else
    echo "Failed to extract version"
    exit 1
fi

# Build the shared libraries (Go Core) and Tools
echo "Building Go shared libraries and Tools..."
# We use a subshell or pushd/popd to ensure we return to the python dir
(cd ../main && ./build.sh)

# Build the package
echo "Building new release..."
$PYTHON_EXEC -m build

echo "Build complete. Ready to upload."