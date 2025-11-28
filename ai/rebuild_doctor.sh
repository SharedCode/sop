#!/bin/bash
set -e

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Change to the ai directory
cd "$SCRIPT_DIR"

echo "Building tools..."
go build -o sop-prepare ./cmd/prepare
go build -o sop-etl ./cmd/etl
go build -o sop-ai ./cmd/agent

echo "---------------------------------------------------"
echo "0. Cleaning up old data"
echo "---------------------------------------------------"
rm -rf data/nurse_local
rm -rf data/doctor_core

echo "---------------------------------------------------"
echo "1. Preparing Data"
echo "---------------------------------------------------"
# Downloads the dataset and converts it to JSON
./sop-prepare -url "https://raw.githubusercontent.com/itachi9604/healthcare-chatbot/master/Data/dataset.csv" -out "data/doctor_data.json"

echo ""
echo "---------------------------------------------------"
echo "2. Rebuilding Nurse Agent (Dependency)"
echo "---------------------------------------------------"
# The doctor agent depends on nurse_local for symptom translation.
# nurse_local.json has its own data embedded, so we don't need external data file.
./sop-etl -config data/nurse_local.json

echo ""
echo "---------------------------------------------------"
echo "3. Rebuilding Doctor Agent (Target)"
echo "---------------------------------------------------"
# We use the pipeline config, but target the 'doctor_core' agent for data ingestion.
# We pass the prepared data file.
./sop-etl -config data/doctor_pipeline.json -agent doctor_core -data data/doctor_data.json

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
