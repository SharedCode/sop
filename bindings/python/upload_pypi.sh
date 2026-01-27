#!/bin/bash
set -e

# Get the directory of the script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$SCRIPT_DIR"

echo "Preparing to upload to PyPI..."

# Create a temporary virtual environment for the upload process
if [ ! -d ".upload_venv" ]; then
    echo "Creating upload virtual environment..."
    python3 -m venv .upload_venv
fi

echo "Activating upload environment..."
source .upload_venv/bin/activate

# Ensure twine is installed
echo "Installing twine..."
pip install --upgrade twine

# Check if dist directory exists and has files
if [ ! -d "dist" ] || [ -z "$(ls -A dist)" ]; then
    echo "Error: 'dist' directory is empty or missing. Please run build_wheels.sh first."
    exit 1
fi

# Upload
echo "Uploading artifacts to PyPI..."
twine upload dist/*

echo "Upload complete!"
