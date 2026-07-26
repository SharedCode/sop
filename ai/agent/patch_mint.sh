#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
sed -i '' 's/"mint_to_space"/"mint_to_kb"/g' "$SCRIPT_DIR/copilottools.space.go"
sed -i '' 's/toolMintToSpace/toolMintToKB/g' "$SCRIPT_DIR/copilottools.space.go"
sed -i '' 's/toolMintToSpace/toolMintToKB/g' "$SCRIPT_DIR/copilottools.write.go"
sed -i '' 's/toolMintToSpace/toolMintToKB/g' "$SCRIPT_DIR/copilot_enrichment_integ_test.go"
