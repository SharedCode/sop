#!/bin/bash
set -e

# Build Release Binaries for SOP Data Manager

VERSION="1.0.0"
OUTPUT_DIR="release"

mkdir -p $OUTPUT_DIR

echo "Building for macOS (ARM64)..."
GOOS=darwin GOARCH=arm64 go build -o $OUTPUT_DIR/sop-manager-macos-arm64 ./tools/httpserver

echo "Building for macOS (AMD64)..."
GOOS=darwin GOARCH=amd64 go build -o $OUTPUT_DIR/sop-manager-macos-amd64 ./tools/httpserver

echo "Building for Linux (AMD64)..."
GOOS=linux GOARCH=amd64 go build -o $OUTPUT_DIR/sop-manager-linux-amd64 ./tools/httpserver

echo "Building for Windows (AMD64)..."
GOOS=windows GOARCH=amd64 go build -o $OUTPUT_DIR/sop-manager-windows-amd64.exe ./tools/httpserver

echo "Done! Binaries are in $OUTPUT_DIR"
ls -l $OUTPUT_DIR
