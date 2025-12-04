#!/bin/bash
set -e

echo "Running Core Tests..."
go test ./...

echo "Running AI Tests..."
cd ai
go test ./...
cd ..

echo "All tests passed!"
