#!/bin/bash
set -e

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Change to the ai directory
cd "$SCRIPT_DIR"

# Configuration
# DATA_LIMIT is now controlled in etl_workflow.json

echo "Building tools..."
go build -o sop-etl ./cmd/etl
go build -o sop-ai ./cmd/agent

echo "---------------------------------------------------"
echo "0. Cleaning up old data"
echo "---------------------------------------------------"
rm -rf data/nurse_local data/doctor_core data/data
rm -rf data/doctor_core
rm -rf data/vector_sys_config
rm -rf data/storelist.txt
rm -rf data/reghashmod.txt
rm -rf data/translogs

echo "---------------------------------------------------"
echo "1. Running ETL Workflow"
echo "---------------------------------------------------"
./sop-etl -workflow etl_workflow.json

echo ""
echo "---------------------------------------------------"
echo "4. Sanity Test"
echo "---------------------------------------------------"
# We test with colloquial terms ("tummy hurt", "hot") to verify that 
# the nurse_local agent correctly translates them to medical terms 
# ("abdomen pain", "fever") before the doctor agent performs the lookup.
echo "I have a tummy hurt and feel hot" | ./sop-ai -config data/doctor_pipeline.json

echo ""
echo "---------------------------------------------------"
echo "5. Common Cold Test"
echo "---------------------------------------------------"
echo "I have a bad cough and a runny nose" | ./sop-ai -config data/doctor_pipeline.json
