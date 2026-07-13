#!/bin/bash
set -e

# Explain setup
echo "=========================================================="
echo "SOP AI - Google Gemini API Demo Setup"
echo "=========================================================="
echo ""
echo "Requirements:"
echo "1. Export GEMINI_API_KEY environment variable"
echo "   export GEMINI_API_KEY='your-real-key-here'"
echo ""
echo "Press ENTER to continue when your API key is exported (or Ctrl+C to exit)..."
read

echo "1. Cleaning old vector stores (changing embedding dimension to Gemini's 768d)..."
rm -rf ai/data/nurse_local/db
rm -rf ai/data/doctor_core/db

echo "2. Running ETL Workflow (Downloading Dataset, Re-ingesting via Gemini)..."
cd ai && go run cmd/etl/main.go -workflow etl_workflow.json && cd ..

echo "3. Starting the Medical Query via Gemini Pipeline..."
go run ai/cmd/demo_doctor/main.go

