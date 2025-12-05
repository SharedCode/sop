#!/bin/bash
set -e

echo "Running Core Tests..."
go test ./...

echo "Running AI Tests..."
(cd ai && go test ./...)

echo "Running Cassandra Adapter Tests..."
(cd adapters/cassandra && go test ./...)

echo "Running Redis Adapter Tests..."
(cd adapters/redis && go test ./...)

echo "Running JSONDB Tests..."
(cd jsondb && go test ./...)

echo "Running Bindings Tests..."
(cd bindings/main && go test ./...)

echo "Running REST API Tests..."
(cd restapi && go test ./...)

echo "Running INFS Tests..."
(cd infs && go test ./...)

echo "Running INCFS Tests..."
(cd incfs && go test ./...)

echo "Running Search Tests..."
(cd search && go test ./...)

echo "All tests passed!"
