#!/bin/bash
# Activate virtual environment
source .venv/bin/activate

# Run the tests
# -s: Show stdout (print statements)
# -v: Verbose output
pytest -s -v tools/httpserver/tests/ui/test_edit_store.py
