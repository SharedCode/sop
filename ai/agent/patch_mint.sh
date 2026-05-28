#!/bin/bash
sed -i '' 's/"mint_to_space"/"mint_to_kb"/g' /Users/grecinto/sop/ai/agent/copilottools.space.go
sed -i '' 's/toolMintToSpace/toolMintToKB/g' /Users/grecinto/sop/ai/agent/copilottools.space.go
sed -i '' 's/toolMintToSpace/toolMintToKB/g' /Users/grecinto/sop/ai/agent/copilottools.write.go
sed -i '' 's/toolMintToSpace/toolMintToKB/g' /Users/grecinto/sop/ai/agent/copilot_enrichment_integ_test.go
sed -i '' 's/mint_to_space/mint_to_kb/g' /Users/grecinto/sop/ai/agent/test.log
