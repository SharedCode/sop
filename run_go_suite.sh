#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

echo "========================================"
echo "   Running SOP Go Examples Suite"
echo "========================================"

echo ""
echo "[1/5] Running Multi-Redis URL Example..."
echo "----------------------------------------"
go run examples/multi_redis_url/main.go

echo ""
echo "[2/5] Running Interop JSONDB Example..."
echo "----------------------------------------"
go run examples/interop_jsondb/main.go

echo ""
echo "[3/5] Running Interop Secondary Indexes Example..."
echo "----------------------------------------"
go run examples/interop_secondary_indexes/main.go

echo ""
echo "[4/5] Running Swarm Standalone Example..."
echo "----------------------------------------"
go run examples/swarm_standalone/main.go

echo ""
echo "[5/5] Running Swarm Clustered Example..."
echo "----------------------------------------"
go run examples/swarm_clustered/main.go

echo ""
echo "========================================"
echo "   All examples completed successfully!"
echo "========================================"
