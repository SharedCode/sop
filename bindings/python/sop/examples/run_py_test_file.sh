#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
export PYTHONPATH="$SCRIPT_DIR/../.."
python3 "$SCRIPT_DIR/concurrent_demo.py"
