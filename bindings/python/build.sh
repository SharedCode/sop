#!/bin/bash
set -e

# Clean up previous build artifacts
echo "Cleaning up previous builds..."
rm -rf dist/
rm -rf build/
rm -rf sop4py.egg-info/

# Build the package
echo "Building new release..."
# Use the python executable from the environment if set, otherwise default to python3
PYTHON_EXEC=${PYTHON:-python3}
$PYTHON_EXEC -m build

echo "Build complete. Ready to upload."