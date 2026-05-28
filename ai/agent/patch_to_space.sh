#!/bin/bash
# 1. Reverse mint
sed -i '' 's/mint_to_kb/mint_to_space/g' *.go
sed -i '' 's/mint_to_kb/mint_to_space/g' prompts/tools_spaces.md
sed -i '' 's/toolMintToKB/toolMintToSpace/g' *.go

# 2. update config
sed -i '' 's/update_kb_config/update_space_config/g' *.go
sed -i '' 's/update_kb_config/update_space_config/g' prompts/tools_spaces.md

# 3. read config
sed -i '' 's/read_kb_config/read_space_config/g' *.go
sed -i '' 's/read_kb_config/read_space_config/g' prompts/tools_spaces.md

# 4. vectorize categories & items
sed -i '' 's/vectorize_kb_categories/vectorize_space_categories/g' *.go
sed -i '' 's/vectorize_kb_categories/vectorize_space_categories/g' prompts/tools_spaces.md

sed -i '' 's/vectorize_kb_items/vectorize_space_items/g' *.go
sed -i '' 's/vectorize_kb_items/vectorize_space_items/g' prompts/tools_spaces.md

# 5. vectorize kb generally
sed -i '' 's/vectorize_kb/vectorize_space/g' *.go
sed -i '' 's/vectorize_kb/vectorize_space/g' psed -i '' 's/vectorize_kb/vecenrised -i '' 's/vectorize_kb/vectoensed -inowledsed -i '' 's/h_spased -i '' 's/vectorize_kb/vectorize_spacebase/enrich_space/g' prompts/tools_spaces.md
sed -i '' 's/toolEnrichKnowledgeBase/toolEnrichSpace/g' *.go

sed -i '' 's/mint_to_kb/mint_to_space/g' *.go
sed -i '' 's/mint_to_kb/mint_to_space/g' prompts/tools_spaces.md
sed -i '' 's/toolMintToKB/toolMintToSpace/g' *.go
sed -i '' 's/update_kb_config/update_space_config/g' *.go
sed -i '' 's/update_kb_config/update_space_config/g' prompts/tools_spaces.md
sed -i '' 's/read_kb_config/read_space_config/g' *.go
sed -i '' 's/read_kb_config/read_space_config/g' prompts/tools_spaces.md
sed -i '' 's/vectorize_kb_categories/vectorize_space_categories/g' *.go
sed -i '' 's/vectorize_kb_categories/vectorize_space_categories/g' prompts/tools_spaces.md
sed -i '' 's/vectorize_kb_items/vectorize_space_items/g' *.go
